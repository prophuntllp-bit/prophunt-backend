const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const https = require("https");
const WebSocket = require("ws");
const axios = require("axios");
const FormData = require("form-data");
const Redis = require("ioredis");
const { Counter, Histogram, Registry, collectDefaultMetrics } = require("prom-client");
const { LanguageManager } = require("./language-manager");
const { v2: cloudinary } = require("cloudinary");

if (process.env.CLOUDINARY_CLOUD_NAME) {
  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET,
  });
}

const config = {
  port: parseInt(process.env.PORT || "8000", 10),
  services: {
    vad: process.env.VAD_URL || "http://vad:8001",
    stt: process.env.STT_URL || "http://stt:8002",
    tts: process.env.TTS_URL || "http://tts:8003",
    llm: process.env.LLM_URL || "http://llm:11434",
    crmAdapter: process.env.CRM_ADAPTER_URL || "http://crm-adapter:8010",
    knowledge: process.env.KNOWLEDGE_SERVICE_URL || "http://knowledge-service:8011",
    platformApi: process.env.PLATFORM_API_URL || "http://platform-api:8013",
  },
  redisUrl: process.env.REDIS_URL || "redis://redis:6379",
  internalToken: process.env.ORCHESTRATOR_INTERNAL_TOKEN || "local-dev-internal-token",
  recordingsDir: process.env.RECORDINGS_DIR || "/data/recordings",
  maxConcurrentCalls: parseInt(process.env.MAX_CONCURRENT || "50", 10),
  callTimeoutMs: parseInt(process.env.CALL_TIMEOUT_MS || `${5 * 60 * 1000}`, 10),
  sttTimeoutMs: parseInt(process.env.STT_REQUEST_TIMEOUT_MS || "45000", 10),
  enablex: {
    appId: process.env.ENABLEX_APP_ID || "",
    appKey: process.env.ENABLEX_APP_KEY || "",
    fromNumber: process.env.ENABLEX_FROM_NUMBER || "",
    baseUrl: (process.env.ENABLEX_VOICE_BASE_URL || "https://api.enablex.io/voice/v1").replace(/\/$/, ""),
  },
  telephonyProvider: (process.env.TELEPHONY_PROVIDER || "enablex").toLowerCase(),
};

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const registry = new Registry();
collectDefaultMetrics({ register: registry });

const callsTotal = new Counter({
  name: "calls_total",
  help: "Total number of calls handled",
  labelNames: ["status"],
  registers: [registry],
});

const callDuration = new Histogram({
  name: "call_duration_seconds",
  help: "End to end call duration",
  buckets: [5, 15, 30, 60, 120, 300],
  registers: [registry],
});

const serviceLatency = new Histogram({
  name: "service_latency_ms",
  help: "Latency by dependency",
  labelNames: ["service"],
  buckets: [25, 50, 100, 250, 500, 1000, 3000, 5000],
  registers: [registry],
});

const redis = new Redis(config.redisUrl, { lazyConnect: false, maxRetriesPerRequest: 2 });
const sessions = new Map();
const languageManager = new LanguageManager();
let acceptingTraffic = true;
const enablexAuthHeader = config.enablex.appId && config.enablex.appKey
  ? `Basic ${Buffer.from(`${config.enablex.appId}:${config.enablex.appKey}`).toString("base64")}`
  : "";

fs.mkdirSync(config.recordingsDir, { recursive: true });

app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true }));
app.use("/recordings", express.static(config.recordingsDir));

function getPublicBaseUrl(req) {
  const host = process.env.PUBLIC_HOST || req.get("host") || "localhost:8000";
  const protocol = req.get("x-forwarded-proto") || (host.includes("localhost") ? "http" : "https");
  return `${protocol}://${host}`.replace(/\/$/, "");
}

function getPublicWsBaseUrl(req) {
  return getPublicBaseUrl(req).replace(/^http/i, "ws");
}

function getConfiguredPublicBaseUrl() {
  const host = process.env.PUBLIC_HOST || `localhost:${config.port}`;
  const protocol = host.includes("localhost") ? "http" : "https";
  return `${protocol}://${host}`.replace(/\/$/, "");
}

function getConfiguredPublicWsBaseUrl() {
  return getConfiguredPublicBaseUrl().replace(/^http/i, "ws");
}

function resolveTelephonyProvider(requestedProvider) {
  const provider = String(requestedProvider || config.telephonyProvider || "enablex").trim().toLowerCase();
  return provider === "enablex" ? "enablex" : "simulated";
}

function hasEnablexConfig() {
  return Boolean(enablexAuthHeader && config.enablex.fromNumber);
}

function buildEnablexOpeningLine(leadName = "there") {
  return `Hello, this is Priya from Prophunt. I am calling regarding your interest in our project. Is this a good time to talk for thirty seconds, ${leadName}?`;
}

function normalizeEnablexPhoneNumber(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (digits.length === 10) return `91${digits}`;
  return digits;
}

async function placeEnablexOutboundCall({ lead, session, openingLine }) {
  if (!hasEnablexConfig()) {
    throw new Error("EnableX credentials or caller number are missing");
  }

  const publicBaseUrl = getConfiguredPublicBaseUrl();
  const payload = {
    name: "Prophunt AI Voice Agent",
    owner_ref: session.callSid,
    auto_record: false,
    from: normalizeEnablexPhoneNumber(config.enablex.fromNumber),
    to: normalizeEnablexPhoneNumber(lead.phone),
    event_url: `${publicBaseUrl}/call/enablex/events`,
  };

  let response;
  try {
    response = await timed("enablex", () =>
      axios.post(`${config.enablex.baseUrl}/call`, payload, {
        headers: {
          Authorization: enablexAuthHeader,
          "Content-Type": "application/json",
        },
        timeout: 45000,
      })
    );
  } catch (error) {
    console.error("[enablex] outbound call failed", {
      message: error.message,
      status: error.response?.status,
      data: error.response?.data,
      from: payload.from,
      to: payload.to,
    });
    throw error;
  }

  const data = response.data || {};
  console.log("[enablex] outbound call response", {
    status: response.status,
    voice_id: data.voice_id,
    state: data.state,
    msg: data.msg,
  });
  return {
    provider_call_id: data.voice_id || data.call_id || data.callId || data.id || data.sid || session.callSid,
    provider_status: data.state || data.status || "initiated",
    raw: data,
  };
}

async function callEnablexApi(method, pathName, payload = null, options = {}) {
  if (!enablexAuthHeader) {
    throw new Error("EnableX credentials are missing");
  }
  const response = await timed("enablex", () =>
    axios({
      method,
      url: `${config.enablex.baseUrl}${pathName}`,
      data: payload,
      headers: {
        Authorization: enablexAuthHeader,
        "Content-Type": "application/json",
      },
      timeout: options.timeout || 45000,
    })
  );
  const data = response.data;
  if (
    data &&
    (data.statusCode >= 400 ||
      data.result >= 400 ||
      /not found|not allowed|failed|error/i.test(String(data.msg || data.playstate || data.state || "")))
  ) {
    const error = new Error(data.msg || data.playstate || data.state || "EnableX API rejected the request");
    error.response = { status: data.statusCode || data.result || response.status, data };
    throw error;
  }
  return data;
}

async function callEnablexDeleteRaw(pathName) {
  if (!enablexAuthHeader) {
    throw new Error("EnableX credentials are missing");
  }
  const endpoint = new URL(`${config.enablex.baseUrl}${pathName}`);
  return timed("enablex", () =>
    new Promise((resolve, reject) => {
      const req = https.request(
        {
          protocol: endpoint.protocol,
          hostname: endpoint.hostname,
          port: endpoint.port || 443,
          path: `${endpoint.pathname}${endpoint.search}`,
          method: "DELETE",
          headers: {
            Authorization: enablexAuthHeader,
            "Content-Type": "application/json",
          },
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => {
            body += chunk;
          });
          res.on("end", () => {
            if (res.statusCode >= 200 && res.statusCode < 300) {
              resolve(body);
              return;
            }
            const error = new Error(`EnableX delete failed with status ${res.statusCode}`);
            error.response = { status: res.statusCode, data: body };
            reject(error);
          });
        }
      );
      req.on("error", reject);
      req.end("");
    })
  );
}

async function startEnablexStream(voiceId) {
  const wssHost = `${getConfiguredPublicWsBaseUrl()}/audio/enablex/${encodeURIComponent(voiceId)}`;
  console.log("[enablex-media] starting stream", { voice_id: voiceId, wss_host: wssHost });
  return callEnablexApi(
    "put",
    `/call/${encodeURIComponent(voiceId)}/stream`,
    { wss_host: wssHost },
    { timeout: 10000 }
  );
}

async function stopEnablexStream(voiceId) {
  return callEnablexDeleteRaw(`/call/${encodeURIComponent(voiceId)}/stream`);
}

async function hangupEnablexCall(voiceId) {
  return callEnablexDeleteRaw(`/call/${encodeURIComponent(voiceId)}`);
}

const ENABLEX_STREAM_READY_STATUSES = new Set([
  "answered",
  "answer",
  "connected",
  "in-progress",
  "in_progress",
  "live",
  "ongoing",
  "active",
  "bridged",
]);

function extractEnablexCallSid(payload = {}) {
  return payload.voice_id || payload.call_id || payload.callId || payload.id || payload.sid || payload.call_sid;
}

function normalizeEnablexStatus(payload = {}) {
  const rawStatus = payload.status || payload.state || payload.event || payload.call_status || payload.callStatus || "";
  return String(rawStatus).toLowerCase();
}

function shouldStartEnablexStream(callStatus) {
  return ENABLEX_STREAM_READY_STATUSES.has(String(callStatus || "").toLowerCase());
}

function scheduleEnablexStreamStart(session, reason = "scheduled", options = {}) {
  const force = options.force === true;
  if (!session?.callSid || session.closed || session.telephony?.streamStarted || (!force && session.telephony?.streamStartScheduled)) {
    return;
  }
  session.telephony = {
    ...(session.telephony || {}),
    provider: "enablex",
    streamStartScheduled: true,
    streamStartInFlight: false,
    streamStartReason: reason,
  };

  const voiceId = session.callSid;
  const delays = [
    0,
    1000,
    2000,
    3000,
    4000,
    5000,
    6000,
    7000,
    8000,
    9000,
    10000,
    11000,
    12000,
    13000,
    14000,
    16000,
    18000,
    21000,
    25000,
  ];
  delays.forEach((delayMs, index) => {
    setTimeout(async () => {
      const current = sessions.get(voiceId);
      if (!current || current.closed || current.telephony?.streamStarted || current.telephony?.streamStartInFlight) return;
      try {
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartInFlight: true,
        };
        console.log("[enablex-media] stream start attempt", { voice_id: voiceId, attempt: index + 1, reason });
        const streamResponse = await startEnablexStream(voiceId);
        console.log("[enablex-media] stream start accepted", { voice_id: voiceId, attempt: index + 1, response: streamResponse });
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartResponse: streamResponse,
          streamStartInFlight: false,
          streamStartScheduled: false,
          streamStarted: true,
        };
        await persistSession(current);
      } catch (streamError) {
        const errorPayload = streamError.response?.data || streamError.message;
        console.error("[enablex-media] stream start failed", {
          voice_id: voiceId,
          attempt: index + 1,
          reason,
          error: errorPayload,
        });
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartError: errorPayload,
          streamStartInFlight: false,
          streamStartScheduled: index < delays.length - 1,
        };
        await persistSession(current).catch(() => {});
      }
    }, delayMs);
  });
}

function nowIso() {
  return new Date().toISOString();
}

function buildSystemPrompt(lead, knowledgeContext, language) {
  return `You are a real estate sales consultant calling on behalf of ${lead.developer || "our firm"}.

PROJECT KNOWLEDGE:
${knowledgeContext || "No specific project data loaded."}

LEAD INFO:
- Name: ${lead.name}
- Phone: ${lead.phone}
- Project Interest: ${lead.project || "Unknown"}
- Budget: ${lead.budget || "not discussed yet"}
- Language: ${language || lead.language_preference || "auto-detect"}

CONVERSATION STRATEGY:
1. Open warmly and use the lead name.
2. Ask one qualification question at a time.
3. Keep every reply under 3 sentences.
4. Mirror the lead's language naturally.
5. Never invent prices or possession dates.
6. Close toward a site visit or callback with specific options.
7. If asked whether you are AI, say you are calling from the developer's team.

Return an invisible JSON payload when the conversation is ready to close:
OUTCOME:{"status":"interested","site_visit":false,"callback_date":null,"qualification":{"bhk":"","budget_range":"","purpose":"","timeline":""},"notes":""}`;
}

async function timed(service, fn) {
  const start = Date.now();
  try {
    return await fn();
  } finally {
    serviceLatency.labels(service).observe(Date.now() - start);
  }
}

async function callCrmAdapter(method, url, payload) {
  const response = await timed("crm_adapter", () =>
    axios({
      method,
      url: `${config.services.crmAdapter}${url}`,
      data: payload,
      timeout: 10000,
    })
  );
  return response.data;
}

async function fetchLeadByPhone(phone) {
  return callCrmAdapter("get", `/api/leads/by-phone/${encodeURIComponent(phone)}`);
}

async function fetchDialableLeads(campaignId, limit, filters = {}) {
  const data = await callCrmAdapter("post", "/api/leads/fetch-dialable", {
    campaign_id: campaignId,
    limit,
    filters,
  });
  return data.leads || [];
}

async function pushToCRM(leadId, outcome) {
  return callCrmAdapter("patch", `/api/leads/${leadId}/update`, { outcome });
}

async function persistCallLog(session, outcome, durationSec, finalStatus) {
  const tenantId =
    session.campaign?.tenant_id ||
    session.campaign?.tenantId ||
    session.lead?.tenant_id ||
    session.lead?.tenantId ||
    process.env.DEFAULT_TENANT_ID ||
    "";
  if (!tenantId) {
    console.warn("[call-log] skipped platform persistence because tenant_id was not available", {
      call_sid: session.callSid,
      lead_id: session.lead?.id,
    });
    return null;
  }
  const payload = {
    tenant_id: tenantId,
    campaign_id: session.campaign?.id || session.campaign?.campaign_id || null,
    lead_id: session.lead?.id || null,
    phone: session.lead?.phone || "unknown",
    status: finalStatus,
    call_metadata: {
      provider: session.telephony?.provider || "simulated",
      call_id: session.telephony?.voiceId || session.telephony?.callSid || session.callSid,
      duration_sec: durationSec,
      started_at: session.startedAt,
      ended_at: session.endedAt,
      outcome,
      transcript_summary: outcome.transcript_summary,
      full_transcript: outcome.full_transcript,
      recording_url: outcome.recording_url,
      recordings: session.recordings || {},
      lead_name: session.lead?.name || null,
    },
  };
  try {
    const response = await axios.post(`${config.services.platformApi}/internal/calls`, payload, {
      headers: {
        "Content-Type": "application/json",
        "X-Internal-Token": config.internalToken,
      },
      timeout: 10000,
    });
    return response.data;
  } catch (error) {
    console.warn("[call-log] platform persistence failed", {
      call_sid: session.callSid,
      error: error.response?.data || error.message,
    });
    return null;
  }
}

async function getKnowledgeContext(projectId, transcript) {
  if (!projectId || !transcript) {
    return "";
  }
  try {
    const response = await timed("knowledge_service", () =>
      axios.get(`${config.services.knowledge}/projects/${projectId}/query`, {
        params: { q: transcript },
        timeout: 10000,
      })
    );
    const matches = response.data.matches || [];
    return matches.map((match) => `[${match.section}] ${match.text}`).join("\n");
  } catch {
    return "";
  }
}

async function detectSpeech(audioChunk) {
  try {
    const response = await timed("vad", () =>
      axios.post(`${config.services.vad}/detect`, audioChunk, {
        headers: { "Content-Type": "application/octet-stream" },
        timeout: 500,
      })
    );
    return response.data.is_speech;
  } catch {
    return false;
  }
}

async function detectLanguage(audioBuffer) {
  const form = new FormData();
  form.append("audio", ensureWavBuffer(audioBuffer), { filename: "sample.wav", contentType: "audio/wav" });
  const response = await timed("stt", () =>
    axios.post(`${config.services.stt}/detect-language`, form, {
      headers: form.getHeaders(),
      timeout: Math.min(config.sttTimeoutMs, 15000),
    })
  );
  return response.data;
}

async function transcribeAudio(audioBuffer, language = "auto") {
  const form = new FormData();
  form.append("audio", ensureWavBuffer(audioBuffer), { filename: "audio.wav", contentType: "audio/wav" });
  form.append("language", language);
  const response = await timed("stt", () =>
    axios.post(`${config.services.stt}/transcribe`, form, {
      headers: form.getHeaders(),
      timeout: config.sttTimeoutMs,
    })
  );
  return response.data;
}

function buildRuleBasedReply(session, userText = "") {
  const text = String(userText || "").toLowerCase();
  const project = session.lead?.project || session.campaign?.project_name || "the project";
  const lang = languageManager.getBaseLanguage(session.callSid);
  const isHindi = lang === "hi";

  const wantsConfiguration = /(?:\b|[^a-z0-9])(?:1|one|ek|2|two|do|3|three|teen|4|four|char)\s*(?:b|v|d)?\s*h\s*k\b|bhk|vhk|dhk|dbhk|vbhk|configuration|config|flat size|carpet|sq ?ft/.test(text);
  const wantsTwoBhk = /(?:2|two|to|too|do|d)\s*(?:b|v|d)?\s*h\s*k|dbhk|2bhk|two bhk|do bhk/.test(text);
  const wantsThreeBhk = /(?:3|three|tree|free|teen)\s*(?:b|v|d)?\s*h\s*k|3vhk|3bhk|three bhk|teen bhk/.test(text);
  // Positive: English + Hindi (haan, ji, bilkul, theek, sahi, zaroor)
  const positiveIntent = /yes|yeah|yep|sure|proceed|tell me|go ahead|interested|ok|okay|alright|all right|hello|hi|speaking|here|haan|ji\b|bilkul|theek|sahi|zaroor|batao|bataiye/.test(text);
  // Explicit farewell: English + Hindi (bye, alvida, rakhta, band, chhodo)
  const explicitFarewell = /\b(bye|goodbye|good bye|not interested|no thank|stop calling|remove|alvida|band karo|chhodo|mujhe nahi chahiye)\b/.test(text);
  // Negative: English + Hindi (nahi, nahi chahiye, baad mein, busy)
  const negativeIntent = /bye|not interested|stop|later|no\b|not now|busy|nahi\b|nahin\b|na\b|mat\b|baad mein|abhi nahi/.test(text);
  const guidedState = session.guidedState || null;

  // ── Helpers ───────────────────────────────────────────────────────────────
  const T = (en, hi) => isHindi ? hi : en;

  if (/price|cost|rate|budget|how much|pricing|daam|kimat|rate|kitna|kitne|paisa/.test(text)) {
    session.guidedState = "awaiting_configuration";
    return T(
      `For ${project}, do you want the two BHK price or the three BHK price?`,
      `${project} mein do BHK ka rate chahiye ya teen BHK ka?`
    );
  }
  if (wantsTwoBhk || wantsThreeBhk || wantsConfiguration) {
    const cfg = isHindi
      ? (wantsThreeBhk ? "teen BHK" : wantsTwoBhk ? "do BHK" : "aapki pasand ki")
      : (wantsThreeBhk ? "three BHK" : wantsTwoBhk ? "two BHK" : "preferred configuration");
    session.guidedState = "awaiting_callback_confirmation";
    return T(
      `Got it, ${wantsThreeBhk ? "three BHK" : wantsTwoBhk ? "two BHK" : "preferred configuration"}. I can arrange a sales callback today with the live quote. Should I do that?`,
      `Theek hai, ${cfg}. Main aaj hi sales team ka callback arrange kar sakti hoon live quote ke saath. Karoon?`
    );
  }
  if (guidedState === "awaiting_configuration" && !negativeIntent) {
    return T(
      `Please tell me, do you want the two BHK price or the three BHK price?`,
      `Batayein, do BHK ka rate chahiye ya teen BHK ka?`
    );
  }
  if (guidedState === "awaiting_callback_confirmation") {
    if (positiveIntent) {
      session.guidedState = "callback_confirmed";
      return T(
        `Done. I will arrange the callback today. Thank you for your time. Goodbye.`,
        `Bilkul. Main aaj callback arrange kar dungi. Aapka bahut shukriya. Namaste.`
      );
    }
    if (negativeIntent) {
      session.guidedState = "callback_declined";
      return T(
        `Understood. I will not schedule a callback right now. Thank you for your time. Goodbye.`,
        `Theek hai, abhi callback nahi karta. Aapka shukriya. Namaste.`
      );
    }
    return T(
      `Should I schedule the callback for today?`,
      `Kya main aaj callback schedule karoon?`
    );
  }
  if (guidedState === "awaiting_close_confirmation") {
    if (explicitFarewell) {
      session.guidedState = "closed";
      return T(
        `No problem. Have a great day. Goodbye.`,
        `Koi baat nahi. Aapka din shubh ho. Namaste.`
      );
    }
    session.guidedState = "awaiting_configuration";
    return T(
      `Of course! Are you interested in a two BHK or a three BHK at ${project}?`,
      `Zaroor! ${project} mein do BHK mein interest hai ya teen BHK mein?`
    );
  }
  if (positiveIntent) {
    session.guidedState = "open_discovery";
    return T(
      `I can help with price, location, or site visit details for ${project}. What would you like to know first?`,
      `Main ${project} ke baare mein rate, location ya site visit ki jaankari de sakti hoon. Pehle kya jaanna chahenge?`
    );
  }
  if (/location|where|near|connectivity|area|kahan|jagah|location/.test(text)) {
    session.guidedState = "location_shared";
    return T(
      `${project} is in Pune with strong city connectivity. Would you like the pricing next?`,
      `${project} Pune mein hai, city connectivity bahut acchi hai. Ab rate bata doon?`
    );
  }
  if (/visit|site|schedule|appointment|callback|dekhna|visit|milna/.test(text)) {
    session.guidedState = "awaiting_visit_day";
    return T(
      `Sure. I can note a site visit request. Which works better, today or tomorrow?`,
      `Zaroor. Main site visit request note kar sakti hoon. Aaj aayenge ya kal?`
    );
  }
  if (negativeIntent) {
    if (!guidedState || guidedState === "open_discovery" || guidedState === "location_shared") {
      session.guidedState = "awaiting_close_confirmation";
      return T(
        `I understand. Just before I let you go — would you like to know the pricing for ${project}? It only takes a moment.`,
        `Samajh gayi. Jaane se pehle ek kaam — ${project} ka rate ek baar sun lein, sirf ek minute lagega?`
      );
    }
    session.guidedState = "closed";
    return T(
      `No problem. Thank you for your time. Goodbye.`,
      `Koi baat nahi. Aapka shukriya. Namaste.`
    );
  }
  // If already in open_discovery and lead's reply is unclear, move conversation forward
  if (guidedState === "open_discovery") {
    session.guidedState = "awaiting_configuration";
    return T(
      `Are you interested in a two BHK or three BHK at ${project}? I can share the current pricing.`,
      `${project} mein do BHK ka interest hai ya teen BHK ka? Main rate bata sakti hoon.`
    );
  }
  // Generic fallback — only reached if guidedState is null and nothing matched
  session.guidedState = "open_discovery";
  return T(
    `I can help with price, location, or site visit details for ${project}. What would you like to know?`,
    `Main ${project} ke baare mein rate, location ya site visit ki jaankari de sakti hoon. Kya jaanna chahenge?`
  );
}

function isTerminalGuidedState(session) {
  return ["callback_confirmed", "callback_declined", "closed"].includes(session?.guidedState || "");
}

function shouldUseGuidedReply(session, userText = "") {
  const text = String(userText || "").toLowerCase();
  const guidedState = session?.guidedState || null;
  if (guidedState) {
    return true;
  }
  return /price|cost|rate|budget|how much|pricing|(?:\b|[^a-z0-9])(?:1|one|2|two|3|three|4|four)\s*(?:b|v|d)?\s*h\s*k\b|bhk|vhk|dhk|dbhk|vbhk|configuration|config|flat size|carpet|sq ?ft|location|where|near|connectivity|area|visit|site|schedule|appointment|callback|bye|goodbye|not interested|stop|later/.test(text);
}

async function getLLMResponse(session, userText) {
  const language = languageManager.getLanguage(session.callSid);
  const knowledgeContext = await getKnowledgeContext(session.campaign?.project_id || session.lead.project_id, userText);
  session.history.push({ role: "user", content: userText });
  session.history = session.history.slice(-12);
  if (shouldUseGuidedReply(session, userText)) {
    const reply = buildRuleBasedReply(session, userText);
    session.history.push({ role: "assistant", content: reply });
    return reply;
  }
  try {
    const response = await timed("llm", () =>
      axios.post(
        `${config.services.llm}/v1/chat/completions`,
        {
          model: process.env.LLM_MODEL || "llama3:latest",
          messages: [{ role: "system", content: buildSystemPrompt(session.lead, knowledgeContext, language) }, ...session.history],
          temperature: 0.35,
          max_tokens: 110,
          stream: false,
        },
        { timeout: parseInt(process.env.LLM_REQUEST_TIMEOUT_MS || "45000", 10) }
      )
    );
    const reply = response.data.choices?.[0]?.message?.content || languageManager.fallback(session.callSid);
    session.history.push({ role: "assistant", content: reply });
    const match = reply.match(/OUTCOME:({.*})/s);
    if (match) {
      try {
        session.outcome = JSON.parse(match[1]);
      } catch {}
    }
    return reply.replace(/OUTCOME:({.*})/s, "").trim();
  } catch (error) {
    console.warn("[llm] response failed, using guided fallback", {
      callSid: session.callSid,
      message: error.message,
      status: error.response?.status,
    });
    const reply = buildRuleBasedReply(session, userText);
    session.history.push({ role: "assistant", content: reply });
    return reply;
  }
}

async function getOpeningMessage(session) {
  const explicitOpening =
    session.campaign?.opening_line ||
    session.campaign?.openingLine ||
    session.campaign?.opening_text ||
    session.campaign?.opening_message ||
    session.lead?.opening_text ||
    process.env.DEFAULT_OPENING_TEXT ||
    "";

  if (explicitOpening && explicitOpening.trim()) {
    const opening = explicitOpening.trim();
    session.history.push({ role: "assistant", content: opening });
    session.history = session.history.slice(-12);
    return opening;
  }

  return getLLMResponse(session, "[CALL_STARTED]");
}

function emotionFromContext(text = "", state = {}) {
  const lowered = text.toLowerCase();
  if (state.stage === "opening") return "warm";
  if (/(benefit|amenity|feature|offer|launch)/.test(lowered)) return "excited";
  if (/(price|budget|expensive|concern|issue|problem)/.test(lowered)) return "empathetic";
  if (/(visit|schedule|book|callback|meeting)/.test(lowered)) return "professional";
  return "neutral";
}

const SARVAM_VOICE_MAP = {
  hi: { female: "ritu", male: "rahul" },
  mr: { female: "roopa", male: "anand" },
  ta: { female: "kavya", male: "kavya" },
  pa: { female: "simran", male: "simran" },
  te: { female: "vijay", male: "vijay" },
  bn: { female: "shreya", male: "shreya" },
  en: { female: "priya", male: "shubh" },
};

async function synthesizeSpeech(session, text) {
  const gender = session.campaign?.voice_gender || session.lead.voice_gender || "female";
  const resolvedVoiceId = session.campaign?.voice_id || languageManager.resolveVoice(session.callSid, gender);
  const language = languageManager.getLanguage(session.callSid);
  const lang = languageManager.getBaseLanguage(session.callSid);
  // Map language-manager voice IDs (e.g. hi_female_01) to correct Sarvam speaker per language
  const voiceId = /^([a-z]{2})_(male|female)_\d{2}$/i.test(resolvedVoiceId)
    ? (SARVAM_VOICE_MAP[lang]?.[gender] || SARVAM_VOICE_MAP["en"][gender] || "priya")
    : resolvedVoiceId;
  const emotion = emotionFromContext(text, { stage: session.stage });
  try {
    const response = await timed("tts", () =>
      axios.post(
        `${config.services.tts}/synthesize`,
        {
          text,
          voice_id: voiceId,
          language,
          gender,
          emotion,
          context: { stage: session.stage, lead_status: session.outcome?.status || session.lead.status || "new" },
        },
        { responseType: "arraybuffer", timeout: parseInt(process.env.TTS_REQUEST_TIMEOUT_MS || "25000", 10) }
      )
    );
    return Buffer.from(response.data);
  } catch (error) {
    console.warn("[tts] synthesis failed", {
      callSid: session.callSid,
      voiceId,
      language,
      message: error.message,
      status: error.response?.status,
    });
    return null;
  }
}

async function persistSession(session) {
  const serializable = { ...session, startedAt: session.startedAt, updatedAt: nowIso() };
  delete serializable.timer;
  await redis.set(`session:${session.callSid}`, JSON.stringify(serializable), "EX", Math.ceil(config.callTimeoutMs / 1000));
}

function safeRecordingId(callSid) {
  return String(callSid || crypto.randomUUID()).replace(/[^a-zA-Z0-9_.-]/g, "_");
}

function writeWavFile(filePath, pcm16Buffer, sampleRate = 16000) {
  fs.writeFileSync(filePath, createWavBuffer(pcm16Buffer, sampleRate));
}

function createWavBuffer(pcm16Buffer, sampleRate = 16000) {
  const header = Buffer.alloc(44);
  const byteRate = sampleRate * 2;
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm16Buffer.length, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm16Buffer.length, 40);
  return Buffer.concat([header, pcm16Buffer]);
}

function ensureWavBuffer(audioBuffer, sampleRate = 16000) {
  if (!audioBuffer?.length) return audioBuffer;
  return audioBuffer.subarray(0, 4).toString("ascii") === "RIFF" ? audioBuffer : createWavBuffer(audioBuffer, sampleRate);
}

function recordingUrl(callSid, fileName) {
  return `${getConfiguredPublicBaseUrl()}/recordings/${safeRecordingId(callSid)}/${fileName}`;
}

function getRecordingState(session) {
  if (!session.recording) {
    const recordingId = safeRecordingId(session.callSid);
    const dir = path.join(config.recordingsDir, recordingId);
    fs.mkdirSync(dir, { recursive: true });
    session.recording = {
      id: recordingId,
      dir,
      callerPcmPath: path.join(dir, "caller.pcm"),
      agentPcmPath: path.join(dir, "agent.pcm"),
      mixedPcmPath: path.join(dir, "mixed.pcm"),
      timelinePath: path.join(dir, "timeline.json"),
      timeline: [],
      startedAt: nowIso(),
    };
  }
  return session.recording;
}

async function appendRecordingAudio(session, speaker, pcm16Buffer, label = "audio") {
  if (!session || !pcm16Buffer?.length) return null;
  const recording = getRecordingState(session);
  const targetPath = speaker === "agent" ? recording.agentPcmPath : recording.callerPcmPath;
  await fs.promises.appendFile(targetPath, pcm16Buffer);
  await fs.promises.appendFile(recording.mixedPcmPath, pcm16Buffer);
  recording.timeline.push({
    speaker,
    label,
    timestamp: nowIso(),
    bytes: pcm16Buffer.length,
    duration_ms: Math.round((pcm16Buffer.length / 2 / 16000) * 1000),
  });
  return recording;
}

async function recordCallerAudio(session, pcm16Buffer, label = "caller-media") {
  return appendRecordingAudio(session, "caller", pcm16Buffer, label);
}

async function recordAgentAudio(session, wavBuffer, label = "agent-media") {
  if (!wavBuffer?.length) return null;
  const { pcm, sampleRate } = parseWavInfo(wavBuffer);
  return appendRecordingAudio(session, "agent", resamplePcm16(pcm, sampleRate, 16000), label);
}

async function uploadRecordingToCloudinary(filePath, callSid) {
  if (!process.env.CLOUDINARY_CLOUD_NAME || !fs.existsSync(filePath)) return null;
  try {
    const result = await cloudinary.uploader.upload(filePath, {
      resource_type: "video",
      folder: "call-recordings",
      public_id: `${callSid}/mixed`,
      overwrite: true,
    });
    return result.secure_url;
  } catch (err) {
    console.error("[cloudinary] upload failed:", err.message);
    return null;
  }
}

async function finalizeRecording(session) {
  if (!session?.recording || session.recording.finalized) {
    return session?.recordings || null;
  }
  const recording = session.recording;
  const files = {
    caller: path.join(recording.dir, "caller.wav"),
    agent: path.join(recording.dir, "agent.wav"),
    mixed: path.join(recording.dir, "mixed.wav"),
  };
  const callerPcm = fs.existsSync(recording.callerPcmPath) ? fs.readFileSync(recording.callerPcmPath) : Buffer.alloc(0);
  const agentPcm = fs.existsSync(recording.agentPcmPath) ? fs.readFileSync(recording.agentPcmPath) : Buffer.alloc(0);
  const mixedPcm = fs.existsSync(recording.mixedPcmPath) ? fs.readFileSync(recording.mixedPcmPath) : Buffer.alloc(0);
  if (callerPcm.length) writeWavFile(files.caller, callerPcm);
  if (agentPcm.length) writeWavFile(files.agent, agentPcm);
  if (mixedPcm.length) writeWavFile(files.mixed, mixedPcm);
  await fs.promises.writeFile(recording.timelinePath, JSON.stringify(recording.timeline, null, 2));
  recording.finalized = true;
  session.recordingPath = mixedPcm.length ? recordingUrl(session.callSid, "mixed.wav") : null;
  session.recordings = {
    caller_path: callerPcm.length ? files.caller : null,
    agent_path: agentPcm.length ? files.agent : null,
    mixed_path: mixedPcm.length ? files.mixed : null,
    timeline_path: recording.timelinePath,
    caller_url: callerPcm.length ? recordingUrl(session.callSid, "caller.wav") : null,
    agent_url: agentPcm.length ? recordingUrl(session.callSid, "agent.wav") : null,
    mixed_url: mixedPcm.length ? recordingUrl(session.callSid, "mixed.wav") : null,
    timeline: recording.timeline,
  };
  return session.recordings;
}

async function endCall(session, finalStatus = "completed") {
  if (session.closed) return;
  session.closed = true;
  session.status = finalStatus;
  session.endedAt = nowIso();
  await finalizeRecording(session);
  if (session.recordings?.mixed_path) {
    const cloudUrl = await uploadRecordingToCloudinary(session.recordings.mixed_path, session.callSid);
    if (cloudUrl) {
      session.recordingPath = cloudUrl;
      session.recordings.mixed_url = cloudUrl;
    }
  }
  const durationSec = Math.max(1, Math.round((Date.now() - session.startedTs) / 1000));
  callsTotal.labels(finalStatus).inc();
  callDuration.observe(durationSec);
  const outcome = {
    ...(session.outcome || {
    status: finalStatus,
    call_duration_sec: durationSec,
    transcript_summary: session.history.map((item) => `${item.role}: ${item.content}`).join(" | ").slice(0, 1000),
    site_visit_scheduled: false,
    callback_date: null,
    lead_temperature: "warm",
    qualification: { bhk: "", budget_range: "", purpose: "", timeline: "" },
    full_transcript: JSON.stringify(session.history),
    }),
    status: session.outcome?.status || finalStatus,
    call_duration_sec: durationSec,
    full_transcript: session.outcome?.full_transcript || JSON.stringify(session.history),
    recording_url: session.recordingPath || session.outcome?.recording_url || null,
  };
  try {
    await pushToCRM(session.lead.id, { ...outcome, call_duration_sec: durationSec });
  } catch {}
  await persistCallLog(session, { ...outcome, call_duration_sec: durationSec }, durationSec, finalStatus);
  await persistSession(session);
  sessions.delete(session.callSid);
  languageManager.clear(session.callSid);
}

function createSession(lead, campaign = {}, callSid = crypto.randomUUID()) {
  const preferredLanguage = lead.language_preference || campaign.language || "auto";
  languageManager.initialize(callSid, preferredLanguage);
  const session = {
    callSid,
    lead,
    campaign,
    history: [],
    status: "initiated",
    stage: "opening",
    startedAt: nowIso(),
    startedTs: Date.now(),
    closed: false,
    outcome: null,
    recordingPath: null,
    telephony: null,
    pendingGreetingAudio: null,
  };
  session.timer = setTimeout(() => endCall(session, "timeout"), config.callTimeoutMs);
  sessions.set(callSid, session);
  return session;
}

function remapSessionCallSid(session, nextCallSid) {
  if (!session || !nextCallSid || session.callSid === nextCallSid) return;
  const previousCallSid = session.callSid;
  const preferredLanguage = languageManager.getLanguage(previousCallSid);
  sessions.delete(previousCallSid);
  session.callSid = nextCallSid;
  sessions.set(nextCallSid, session);
  languageManager.initialize(nextCallSid, preferredLanguage);
  languageManager.clear(previousCallSid);
}

function muLawDecodeSample(muLawByte) {
  const MULAW_BIAS = 0x84;
  muLawByte = ~muLawByte & 0xff;
  const sign = muLawByte & 0x80;
  const exponent = (muLawByte >> 4) & 0x07;
  const mantissa = muLawByte & 0x0f;
  let sample = ((mantissa << 3) + MULAW_BIAS) << exponent;
  sample -= MULAW_BIAS;
  return sign ? -sample : sample;
}

function decodeMuLawToPcm16(muLawBuffer) {
  const pcm = Buffer.alloc(muLawBuffer.length * 2);
  for (let i = 0; i < muLawBuffer.length; i += 1) {
    pcm.writeInt16LE(muLawDecodeSample(muLawBuffer[i]), i * 2);
  }
  return pcm;
}

function upsamplePcm16To16k(pcm8kBuffer) {
  const sampleCount = Math.floor(pcm8kBuffer.length / 2);
  const pcm16k = Buffer.alloc(sampleCount * 4);
  for (let i = 0; i < sampleCount; i += 1) {
    const sample = pcm8kBuffer.readInt16LE(i * 2);
    pcm16k.writeInt16LE(sample, i * 4);
    pcm16k.writeInt16LE(sample, i * 4 + 2);
  }
  return pcm16k;
}

function parseWavToPcm16(wavBuffer) {
  return parseWavInfo(wavBuffer).pcm;
}

function parseWavInfo(wavBuffer) {
  const dataIndex = wavBuffer.indexOf(Buffer.from("data"));
  if (dataIndex === -1 || wavBuffer.length < 44) {
    return { pcm: wavBuffer, sampleRate: 16000 };
  }
  const dataLength = wavBuffer.readUInt32LE(dataIndex + 4);
  const sampleRate = wavBuffer.readUInt32LE(24) || 16000;
  return {
    pcm: wavBuffer.subarray(dataIndex + 8, dataIndex + 8 + dataLength),
    sampleRate,
  };
}

function resamplePcm16(pcmBuffer, fromRate, toRate) {
  if (!pcmBuffer?.length || fromRate === toRate) return pcmBuffer;
  const inputSamples = Math.floor(pcmBuffer.length / 2);
  const outputSamples = Math.max(1, Math.floor((inputSamples * toRate) / fromRate));
  const out = Buffer.alloc(outputSamples * 2);
  for (let i = 0; i < outputSamples; i += 1) {
    const sourceIndex = (i * fromRate) / toRate;
    const low = Math.floor(sourceIndex);
    const high = Math.min(low + 1, inputSamples - 1);
    const ratio = sourceIndex - low;
    const a = pcmBuffer.readInt16LE(low * 2);
    const b = pcmBuffer.readInt16LE(high * 2);
    out.writeInt16LE(Math.round(a + (b - a) * ratio), i * 2);
  }
  return out;
}

function downsamplePcm16To8k(pcm16kBuffer) {
  const inputSamples = Math.floor(pcm16kBuffer.length / 2);
  const outputSamples = Math.floor(inputSamples / 2);
  const out = Buffer.alloc(outputSamples * 2);
  for (let i = 0; i < outputSamples; i += 1) {
    const sample = pcm16kBuffer.readInt16LE(i * 4);
    out.writeInt16LE(sample, i * 2);
  }
  return out;
}

function muLawEncodeSample(sample) {
  const MULAW_MAX = 32635;
  const MULAW_BIAS = 0x84;
  let sign = 0;
  if (sample < 0) {
    sign = 0x80;
    sample = -sample;
  }
  sample = Math.min(sample, MULAW_MAX);
  sample += MULAW_BIAS;
  let exponent = 7;
  for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; exponent -= 1, expMask >>= 1) {}
  const mantissa = (sample >> Math.max(exponent + 3, 0)) & 0x0f;
  return (~(sign | (exponent << 4) | mantissa)) & 0xff;
}

function encodePcm16ToMuLaw(pcm8kBuffer) {
  const sampleCount = Math.floor(pcm8kBuffer.length / 2);
  const out = Buffer.alloc(sampleCount);
  for (let i = 0; i < sampleCount; i += 1) {
    out[i] = muLawEncodeSample(pcm8kBuffer.readInt16LE(i * 2));
  }
  return out;
}

function toEnablexMuLawChunks(ttsWavBuffer) {
  const { pcm, sampleRate } = parseWavInfo(ttsWavBuffer);
  const pcm8k = sampleRate === 16000 ? downsamplePcm16To8k(pcm) : resamplePcm16(pcm, sampleRate, 8000);
  const muLaw = encodePcm16ToMuLaw(pcm8k);
  const chunks = [];
  for (let offset = 0; offset < muLaw.length; offset += 160) {
    chunks.push(muLaw.subarray(offset, offset + 160));
  }
  return chunks;
}

function decodeEnablexInboundMedia(event) {
  const payload = Buffer.from(event.media.payload, "base64");
  const format = event.media.format || {};
  const encoding = String(format.encoding || "ulaw").toLowerCase();
  const sampleRate = Number(format.sample_rate || format.sampleRate || 8000);
  if (/linear|pcm|l16|s16/.test(encoding)) {
    return sampleRate === 16000 ? payload : resamplePcm16(payload, sampleRate, 16000);
  }
  const pcm = decodeMuLawToPcm16(payload);
  return sampleRate === 16000 ? pcm : resamplePcm16(pcm, sampleRate, 16000);
}

function sendEnablexMedia(ws, session, audioBuffer, label = "audio") {
  const streamId = session.telephony?.streamId;
  const voiceId = session.telephony?.voiceId || session.callSid;
  if (!audioBuffer || ws.readyState !== WebSocket.OPEN || session.telephony?.provider !== "enablex" || !streamId || !voiceId) {
    return false;
  }
  const chunks = toEnablexMuLawChunks(audioBuffer);
  const playbackMs = chunks.length * 20;
  const generation = (session.telephony.outGeneration || 0) + 1;
  session.telephony.outGeneration = generation;
  session.telephony.agentSpeakingUntil = Date.now() + playbackMs + 700;
  if (session.inboundAudio && !session.inboundAudio.processing) {
    session.inboundAudio.chunks = [];
    session.inboundAudio.speechFrames = 0;
    session.inboundAudio.silenceFrames = 0;
  }
  console.log(`[enablex-media] sending ${label} for ${voiceId} (${audioBuffer.length} bytes, ${chunks.length} chunks)`);
  session.telephony.lastPlaybackMs = playbackMs;
  chunks.forEach((chunk, index) => {
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN || session.telephony.outGeneration !== generation) return;
      const seq = session.telephony.outSeq || 0;
      ws.send(
        JSON.stringify({
          event: "media",
          voice_id: voiceId,
          stream_id: streamId,
          media: {
            seq,
            timestamp: Date.now(),
            format: {
              encoding: "ulaw",
              sample_rate: 8000,
              channels: 1,
            },
            payload: chunk.toString("base64"),
          },
        })
      );
      session.telephony.outSeq = seq + 1;
    }, index * 20);
  });
  return true;
}

function scheduleAgentSideHangup(ws, session, reason = "completed-reply") {
  if (!session || session.closed || session.telephony?.hangupScheduled) {
    return;
  }
  const delayMs = Math.max(1500, (session.telephony?.lastPlaybackMs || 0) + 1200);
  const callSidSnapshot = session.callSid;
  session.telephony = {
    ...(session.telephony || {}),
    hangupScheduled: true,
    hangupReason: reason,
  };
  setTimeout(async () => {
    // Look up by snapshot callSid — session may have already been deleted from map if caller hung up first
    const current = sessions.get(callSidSnapshot) || (session.closed ? null : session);
    if (!current || current.closed) {
      return;
    }
    const voiceId = current.telephony?.voiceId || current.callSid;
    console.log("[enablex-media] agent-side hangup firing", { callSid: callSidSnapshot, voiceId, reason, delayMs });

    // Step 1: Cancel any remaining agent audio
    if (ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, current);
    }

    // Step 2: Close WebSocket from our side — primary signal to EnableX to end media/call
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
      ws.close(1000, "agent-ended");
    }

    // Step 3: Brief pause then call REST hangup API as belt-and-suspenders
    await new Promise((r) => setTimeout(r, 600));
    try {
      await hangupEnablexCall(voiceId);
      console.log("[enablex-media] hangup API succeeded", { callSid: callSidSnapshot, voiceId });
    } catch (error) {
      console.warn("[enablex-media] hangup API call failed", {
        callSid: callSidSnapshot,
        voiceId,
        status: error.response?.status,
        body: error.response?.data || error.message,
      });
    }

    // Step 4: Clean up our session (ws.on("close") may also call endCall, but endCall is idempotent)
    await endCall(current, "agent_completed");
  }, delayMs);
}

function clearEnablexMedia(ws, session) {
  if (ws.readyState !== WebSocket.OPEN || !session?.telephony?.streamId) return;
  session.telephony.outGeneration = (session.telephony.outGeneration || 0) + 1;
  ws.send(
    JSON.stringify({
      event: "clear_media",
      voice_id: session.telephony.voiceId || session.callSid,
      stream_id: session.telephony.streamId,
    })
  );
}

async function processCallerUtterance(ws, session, callSid, reason = "utterance") {
  const inbound = session.inboundAudio;
  if (!inbound || inbound.processing || !inbound.chunks.length || session.telephony?.hangupScheduled) return;
  inbound.processing = true;
  const utteranceAudio = Buffer.concat(inbound.chunks);
  inbound.chunks = [];
  inbound.speechFrames = 0;
  inbound.silenceFrames = 0;
  inbound.lastFlushAt = Date.now();

  // Skip utterances that are too short to contain meaningful speech (< 0.5s of audio)
  const MIN_UTTERANCE_BYTES = 8000;
  if (utteranceAudio.length < MIN_UTTERANCE_BYTES) {
    console.log(`[enablex-media] skipping short utterance callSid=${callSid} reason=${reason} bytes=${utteranceAudio.length}`);
    inbound.processing = false;
    return;
  }

  try {
    console.log(`[enablex-media] processing caller utterance callSid=${callSid} reason=${reason} bytes=${utteranceAudio.length}`);
    const transcription = await transcribeAudio(utteranceAudio, "auto");
    console.log(`[stt] caller utterance callSid=${callSid} text="${transcription.text || ""}" language=${transcription.language || ""}`);
    if (!transcription.text) return;
    // Skip transcriptions that are too short — likely noise or fragment (< 3 meaningful words)
    const wordCount = transcription.text.trim().split(/\s+/).filter(w => w.length > 1).length;
    if (wordCount < 3) {
      console.log(`[enablex-media] skipping fragment callSid=${callSid} words=${wordCount} text="${transcription.text}"`);
      return;
    }
    languageManager.recordUtterance(callSid, transcription.language, transcription.text);
    session.stage = "qualification";
    let reply = await getLLMResponse(session, transcription.text);
    console.log(`[agent] reply callSid=${callSid} text="${reply}"`);
    let speech = await synthesizeSpeech(session, reply);
    if (!speech) {
      reply =
        "I can help with that. Pricing depends on configuration, floor, and availability. Are you looking for a two BHK or a three BHK?";
      console.warn("[enablex-media] retrying reply with compact fallback", { callSid, reply });
      speech = await synthesizeSpeech(session, reply);
    }
    if (speech && ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, session);
      await recordAgentAudio(session, speech, "agent-reply");
      sendEnablexMedia(ws, session, speech, "reply");
      if (isTerminalGuidedState(session)) {
        scheduleAgentSideHangup(ws, session, session.guidedState);
      }
    } else {
      console.warn("[enablex-media] reply speech unavailable", {
        callSid,
        websocketOpen: ws.readyState === WebSocket.OPEN,
        reply,
      });
    }
    await persistSession(session);
  } catch (error) {
    console.warn("[enablex-media] caller utterance handling failed", { callSid, message: error.message });
    const fallback = languageManager.fallback(callSid);
    const speech = await synthesizeSpeech(session, fallback);
    if (speech && ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, session);
      await recordAgentAudio(session, speech, "agent-fallback");
      sendEnablexMedia(ws, session, speech, "fallback");
    }
  } finally {
    const currentInbound = session.inboundAudio;
    if (currentInbound) {
      currentInbound.processing = false;
      currentInbound.lastFlushAt = Date.now();
      const queuedBytes = currentInbound.chunks.reduce((s, c) => s + c.length, 0);
      if (queuedBytes > MIN_UTTERANCE_BYTES && ws.readyState === WebSocket.OPEN && !session.closed) {
        setImmediate(() => {
          processCallerUtterance(ws, session, callSid, "queued-after-processing").catch((error) =>
            console.warn("[enablex-media] queued utterance failed", { callSid, message: error.message })
          );
        });
      } else if (currentInbound.chunks.length) {
        // Discard tiny queued fragments — they're noise from the agent's playback period
        currentInbound.chunks = [];
        currentInbound.speechFrames = 0;
        currentInbound.silenceFrames = 0;
      }
    }
  }
}

async function handleCallerAudioFrame(ws, session, callSid, audioBuffer) {
  if (!session.inboundAudio) {
    session.inboundAudio = { chunks: [], speechFrames: 0, silenceFrames: 0, processing: false, lastFlushAt: Date.now() };
  }
  await recordCallerAudio(session, audioBuffer, "caller-media");

  const inbound = session.inboundAudio;
  const hasSpeech = await detectSpeech(audioBuffer);

  // Barge-in: if caller speaks while agent is playing audio, cancel agent and start listening immediately
  if (hasSpeech && session.telephony?.agentSpeakingUntil && Date.now() < session.telephony.agentSpeakingUntil) {
    clearEnablexMedia(ws, session);
    session.telephony.agentSpeakingUntil = 0;
    console.log(`[enablex-media] barge-in detected callSid=${callSid}`);
  }

  if (session.telephony?.agentSpeakingUntil && Date.now() < session.telephony.agentSpeakingUntil) {
    return;
  }

  const isCollecting = inbound.chunks.length > 0;

  if (hasSpeech || isCollecting) {
    inbound.chunks.push(audioBuffer);
  }

  if (inbound.processing) return;

  if (hasSpeech) {
    inbound.speechFrames += 1;
    inbound.silenceFrames = 0;
    return;
  }

  if (!isCollecting) return;
  inbound.silenceFrames += 1;
  const bufferedMs = inbound.chunks.length * 20;
  // Require at least 12 speech frames (240ms) before considering an utterance valid
  const enoughSpeech = inbound.speechFrames >= 12 || bufferedMs >= 2000;
  // Wait for 25 silence frames (500ms) before cutting — prevents chopping mid-sentence
  const endedBySilence = inbound.silenceFrames >= 25;
  // Allow up to 10 seconds of buffering for long utterances
  const tooLong = bufferedMs >= 10000;

  if ((enoughSpeech && endedBySilence) || tooLong) {
    await processCallerUtterance(ws, session, callSid, endedBySilence ? "silence" : "max-buffer");
  }
}

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", registry.contentType);
  res.end(await registry.metrics());
});

app.get("/health", async (_req, res) => {
  try {
    await redis.ping();
    res.json({ status: acceptingTraffic ? "ok" : "draining", active_sessions: sessions.size });
  } catch (error) {
    res.status(500).json({ status: "error", message: error.message });
  }
});

// Session status — polled by dashboard Test Call panel
app.get("/sessions", (_req, res) => {
  const list = Array.from(sessions.values()).map((s) => ({
    call_sid: s.callSid,
    status: s.status || "active",
    state: s.guidedState || null,
    closed: s.closed,
    phone: s.lead?.phone,
    lead_name: s.lead?.name,
    language: languageManager.getLanguage(s.callSid),
    started_at: s.startedAt,
  }));
  res.json({ sessions: list, count: list.length });
});

app.get("/sessions/:callSid", (req, res) => {
  const session = sessions.get(req.params.callSid);
  if (!session) {
    return res.status(404).json({ call_sid: req.params.callSid, status: "completed", state: "not_found" });
  }
  res.json({
    call_sid: session.callSid,
    status: session.status || "active",
    state: session.guidedState || null,
    closed: session.closed,
    phone: session.lead?.phone,
    lead_name: session.lead?.name,
    language: languageManager.getLanguage(session.callSid),
    started_at: session.startedAt,
  });
});

app.post("/call/dial", async (req, res) => {
  if (!acceptingTraffic) {
    return res.status(503).json({ error: "Service draining" });
  }
  const lead = req.body.lead || (req.body.phone ? { id: crypto.randomUUID(), name: "Unknown Lead", phone: req.body.phone } : null);
  if (!lead || !lead.phone) {
    return res.status(400).json({ error: "lead.phone is required" });
  }
  const session = createSession(lead, req.body.campaign || {});
  await persistSession(session);
  const greeting = await getOpeningMessage(session);
  session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
  const provider = resolveTelephonyProvider(req.body.provider);

  if (provider === "enablex") {
    try {
      const openingLine = (
        req.body.opening_line ||
        req.body.campaign?.opening_line ||
        req.body.campaign?.openingLine ||
        greeting ||
        buildEnablexOpeningLine(lead.name || "there")
      ).trim();
      const enablexCall = await placeEnablexOutboundCall({ lead, session, openingLine });
      remapSessionCallSid(session, enablexCall.provider_call_id);
      session.telephony = {
        provider: "enablex",
        from: config.enablex.fromNumber,
        to: lead.phone,
        callSid: enablexCall.provider_call_id,
      };
      session.status = enablexCall.provider_status;
      scheduleEnablexStreamStart(session, "post-dial");
      await persistSession(session);
      return res.json({
        call_sid: enablexCall.provider_call_id,
        lead_id: lead.id,
        phone: lead.phone,
        status: enablexCall.provider_status,
        greeting: openingLine,
        provider: "enablex",
        provider_response: enablexCall.raw,
      });
    } catch (error) {
      return res.status(502).json({
        error: "Failed to place outbound EnableX call",
        details: error.response?.data || error.message,
        call_sid: session.callSid,
        lead_id: lead.id,
        greeting,
      });
    }
  }

  res.json({
    call_sid: session.callSid,
    lead_id: lead.id,
    phone: lead.phone,
    status: "queued",
    greeting,
    provider: "simulated",
  });
});

app.post("/call/bulk-dial", async (req, res) => {
  const campaignId = req.body.campaign_id || crypto.randomUUID();
  const leads = req.body.leads || (await fetchDialableLeads(campaignId, req.body.limit || 10, req.body.filters || {}));
  const results = [];
  for (const lead of leads.slice(0, config.maxConcurrentCalls)) {
    const session = createSession(lead, req.body.campaign || {});
    await persistSession(session);
    results.push({ call_sid: session.callSid, lead_id: lead.id, phone: lead.phone, status: "queued" });
  }
  res.json({ campaign_id: campaignId, queued: results.length, results });
});

app.post("/call/inbound", async (req, res) => {
  const { phone } = req.body;
  if (!phone) {
    return res.status(400).json({ error: "phone is required" });
  }
  try {
    const lead = await fetchLeadByPhone(phone);
    const session = createSession(lead, {});
    await persistSession(session);
    res.json({ call_sid: session.callSid, lead });
  } catch {
    res.status(404).json({ error: "Lead not found" });
  }
});

app.all("/call/enablex/events", async (req, res) => {
  const payload = req.body && Object.keys(req.body).length ? req.body : req.query;
  const callSid = extractEnablexCallSid(payload);
  const callStatus = normalizeEnablexStatus(payload);
  console.log("[enablex-event] received", {
    voice_id: callSid,
    status: callStatus,
    keys: Object.keys(payload || {}),
    payload,
  });
  const session = callSid ? sessions.get(callSid) : null;

  if (session) {
    session.status = callStatus || session.status;
    session.telephony = {
      ...(session.telephony || {}),
      provider: "enablex",
      lastEvent: payload,
    };
    if (shouldStartEnablexStream(callStatus)) {
      scheduleEnablexStreamStart(session, `event-${callStatus}`, { force: callStatus === "connected" });
    }
    if (["completed", "disconnected", "failed", "busy", "no-answer", "cancelled", "canceled"].includes(callStatus)) {
      clearTimeout(session.timer);
      await endCall(session, callStatus);
    } else {
      await persistSession(session);
    }
  }

  res.json({ status: "ok" });
});

wss.on("connection", (ws, req) => {
  console.log(`[enablex-media] websocket connected url=${req.url || "/"}`);
  const wsUrl = new URL(req.url, "http://localhost");
  const pathParts = wsUrl.pathname.split("/").filter(Boolean);
  const requestedCallSid = wsUrl.searchParams.get("callSid") || pathParts[pathParts.length - 1] || crypto.randomUUID();
  let activeCallSid = requestedCallSid;
  let session = sessions.get(requestedCallSid) || null;
  const heartbeat = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, 10000);

  ws.on("message", async (message, isBinary) => {
    let audioBuffer = null;
    if (isBinary) {
      if (!session) return;
      console.log(`[enablex-media] binary frame received bytes=${Buffer.byteLength(message)}`);
      audioBuffer = Buffer.from(message);
    } else {
      try {
        const event = JSON.parse(message.toString());
        if (event.event !== "media") {
          console.log(`[enablex-media] event received type=${event.event || "unknown"}`);
        }
        if (event.event === "connected") {
          console.log("[enablex-media] connected");
          return;
        }
        if (event.event === "start_media") {
          const voiceId = event.start?.voice_id || event.voice_id || activeCallSid;
          const streamId = event.stream_id || event.start?.stream_id || null;
          console.log(`[enablex-media] start_media received voiceId=${voiceId} streamId=${streamId || ""}`);
          if (!session && voiceId) {
            session = sessions.get(voiceId) || null;
            activeCallSid = voiceId;
          }
          if (!session) return;
          session.telephony = {
            ...(session.telephony || {}),
            provider: "enablex",
            streamId,
            voiceId,
            callSid: voiceId,
            outSeq: session.telephony?.outSeq || 0,
          };
          session.status = "stream_started";
          console.log(`[enablex-media] stream started for ${voiceId}`);
          if (!session.pendingGreetingAudio) {
            const greeting = await getOpeningMessage(session);
            session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
          }
          if (session.pendingGreetingAudio) {
            const pending = session.pendingGreetingAudio;
            setTimeout(() => {
              if (sendEnablexMedia(ws, session, pending, "opening-greeting")) {
                recordAgentAudio(session, pending, "opening-greeting").catch((error) =>
                  console.warn("[recording] opening capture failed", error.message)
                );
                session.pendingGreetingAudio = null;
                session.openingPlayedAt = nowIso();
              }
            }, 700);
          }
          await persistSession(session);
          return;
        }
        if (event.event === "stop_media") {
          console.log(`[enablex-media] stop_media received callSid=${activeCallSid}`);
          if (!session) return;
          clearTimeout(session.timer);
          await endCall(session, "completed");
          return;
        }
        if (event.event !== "media" || !event.media?.payload) return;
        if (!session) return;
        const voiceId = event.voice_id || session.telephony?.voiceId || activeCallSid;
        const streamId = event.stream_id || session.telephony?.streamId || null;
        activeCallSid = voiceId || activeCallSid;
        session.telephony = {
          ...(session.telephony || {}),
          provider: "enablex",
          voiceId: activeCallSid,
          streamId,
          callSid: activeCallSid,
          lastInboundSeq: event.media.seq ?? session.telephony?.lastInboundSeq,
        };
        if (!session.telephony.inboundFormatLogged) {
          console.log("[enablex-media] inbound format", {
            callSid: activeCallSid,
            format: event.media.format || null,
          });
          session.telephony.inboundFormatLogged = true;
        }
        if (!session.pendingGreetingAudio && !session.openingPlayedAt) {
          const greeting = await getOpeningMessage(session);
          session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
        }
        if (session.pendingGreetingAudio) {
          const pending = session.pendingGreetingAudio;
          if (sendEnablexMedia(ws, session, pending, "opening-greeting-on-first-media")) {
            await recordAgentAudio(session, pending, "opening-greeting");
            session.pendingGreetingAudio = null;
            session.openingPlayedAt = nowIso();
          }
        }
        audioBuffer = decodeEnablexInboundMedia(event);
      } catch (error) {
        console.log("[enablex-media] failed to parse text frame", error.message);
        return;
      }
    }
    if (!audioBuffer) return;
    await handleCallerAudioFrame(ws, session, activeCallSid, audioBuffer);
  });

  ws.on("close", async () => {
    console.log(`[enablex-media] websocket closed callSid=${activeCallSid}`);
    clearInterval(heartbeat);
    if (session) {
      clearTimeout(session.timer);
      try {
        await stopEnablexStream(activeCallSid);
      } catch {}
      await endCall(session, "completed");
    }
  });
});

async function gracefulShutdown() {
  acceptingTraffic = false;
  for (const session of sessions.values()) {
    clearTimeout(session.timer);
    await endCall(session, "drained");
  }
  await redis.quit();
  server.close(() => process.exit(0));
}

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

server.listen(config.port, () => {
  console.log(`orchestrator listening on ${config.port}`);
});
