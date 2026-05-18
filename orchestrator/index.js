/**
 * AI Voice Orchestrator — Real-time calling loop
 * Connects: SIP/Twilio → VAD → STT → LLM → TTS → caller
 * Handles barge-in, interruption, concurrent calls
 */

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const FormData = require('form-data');
const { EventEmitter } = require('events');
const twilio = require('twilio');
const { createAgniSession } = require('./agni-bridge');

// ─── Config ───────────────────────────────────────────────────────────────────
const config = {
  port: 8000,
  services: {
    vad: process.env.VAD_URL || 'http://vad:8001',
    stt: process.env.STT_URL || 'http://stt:8002',
    tts: process.env.TTS_URL || 'http://tts:8003',
    llm: process.env.LLM_URL || 'http://llm:11434',
  },
  twilio: {
    accountSid: process.env.TWILIO_ACCOUNT_SID,
    authToken: process.env.TWILIO_AUTH_TOKEN,
    fromNumber: process.env.TWILIO_FROM_NUMBER,
  },
  maxConcurrentCalls: parseInt(process.env.MAX_CONCURRENT) || 50,
  silenceThresholdMs: 800,   // wait 800ms of silence before responding
  bargeInThresholdMs: 300,   // detect barge-in after 300ms of speech
};

// ─── Call Session Manager ─────────────────────────────────────────────────────
class CallSession extends EventEmitter {
  constructor(callSid, leadData) {
    super();
    this.callSid = callSid;
    this.lead = leadData;
    this.state = 'idle';  // idle | listening | processing | speaking
    this.history = [];    // conversation history
    this.audioBuffer = [];
    this.isSpeaking = false;
    this.bargeInDetected = false;
    this.startTime = Date.now();
    this.outcome = null;   // interested | callback | site_visit | not_interested
    console.log(`[${callSid}] Session started for lead: ${leadData.name}`);
  }

  addToHistory(role, content) {
    this.history.push({ role, content });
    // Keep last 10 turns to manage token cost
    if (this.history.length > 20) {
      this.history = this.history.slice(-20);
    }
  }

  setState(newState) {
    const prev = this.state;
    this.state = newState;
    console.log(`[${this.callSid}] State: ${prev} → ${newState}`);
    this.emit('stateChange', { prev, current: newState });
  }

  getDurationSec() {
    return Math.round((Date.now() - this.startTime) / 1000);
  }
}

// ─── Active Sessions Store ────────────────────────────────────────────────────
const sessions = new Map();

// ─── App Setup ────────────────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── Script Builder (per project) ────────────────────────────────────────────
function buildSystemPrompt(lead) {
  const projectScripts = {
    default: `You are an AI calling assistant for a Pune real estate firm.
Lead name: ${lead.name}
Project: ${lead.project || 'Premium Residential'}
Budget: ${lead.budget || 'not specified'}
Location interest: ${lead.location || 'Pune'}
Lead source: ${lead.source || 'online inquiry'}`,
  };

  const base = projectScripts[lead.project] || projectScripts.default;

  return `${base}

INSTRUCTIONS:
- Speak in Hindi if the lead speaks Hindi. Mix Hindi/English naturally (Hinglish is fine).
- Be friendly, warm, and conversational. Not robotic.
- Your goal: qualify the lead and book a site visit.
- Ask these questions naturally (not all at once):
  1. Confirm they are looking for a property
  2. Ask preferred BHK (2BHK/3BHK)
  3. Ask budget range
  4. Ask if for self-use or investment
  5. Try to book a site visit: "Aap Saturday ya Sunday kab free hain?"
- Keep each response under 3 sentences.
- If they are not interested, thank them politely and end.
- At the end of conversation, output a JSON block like this (invisible to caller):
  OUTCOME:{"status":"interested","site_visit":true,"callback_date":null,"notes":"wants 3BHK, budget 80L"}

IMPORTANT: Never mention you are an AI unless directly asked. If asked, say "Main ek virtual assistant hoon."`;
}

// ─── STT Service Call ─────────────────────────────────────────────────────────
async function transcribeAudio(audioBuffer, language = 'hi') {
  try {
    const form = new FormData();
    form.append('audio', audioBuffer, {
      filename: 'audio.wav',
      contentType: 'audio/wav',
    });
    form.append('language', language);

    const response = await axios.post(`${config.services.stt}/transcribe`, form, {
      headers: form.getHeaders(),
      timeout: 3000,
    });

    return response.data.text || '';
  } catch (err) {
    console.error('STT error:', err.message);
    return '';
  }
}

// ─── LLM Service Call (streaming) ────────────────────────────────────────────
async function getLLMResponse(session, userText) {
  session.addToHistory('user', userText);

  try {
    const response = await axios.post(
      `${config.services.llm}/v1/chat/completions`,
      {
        model: process.env.LLM_MODEL || 'llama3:latest',
        messages: [
          { role: 'system', content: buildSystemPrompt(session.lead) },
          ...session.history,
        ],
        stream: false,
        temperature: 0.7,
        max_tokens: 150,  // keep responses short for natural conversation
      },
      { timeout: 5000 }
    );

    const reply = response.data.choices[0].message.content;
    session.addToHistory('assistant', reply);

    // Extract outcome if present
    const outcomeMatch = reply.match(/OUTCOME:({.*?})/s);
    if (outcomeMatch) {
      try {
        session.outcome = JSON.parse(outcomeMatch[1]);
        console.log(`[${session.callSid}] Outcome detected:`, session.outcome);
      } catch (_) {}
    }

    // Strip the outcome JSON from spoken text
    return reply.replace(/OUTCOME:{.*?}/s, '').trim();
  } catch (err) {
    console.error('LLM error:', err.message);
    return 'Ek second, main samajh raha hoon...';
  }
}

// ─── TTS Service Call ─────────────────────────────────────────────────────────
async function synthesizeSpeech(text, voiceId = 'default') {
  try {
    const response = await axios.post(
      `${config.services.tts}/synthesize`,
      { text, voice_id: voiceId, language: 'hi' },
      { responseType: 'arraybuffer', timeout: 4000 }
    );
    return Buffer.from(response.data);
  } catch (err) {
    console.error('TTS error:', err.message);
    return null;
  }
}

// ─── VAD (Voice Activity Detection) ──────────────────────────────────────────
async function detectSpeech(audioChunk) {
  try {
    const response = await axios.post(
      `${config.services.vad}/detect`,
      audioChunk,
      {
        headers: { 'Content-Type': 'application/octet-stream' },
        timeout: 200,  // must be very fast
      }
    );
    return response.data.is_speech;
  } catch {
    return false;  // if VAD fails, assume no speech
  }
}

// ─── Main Real-Time Call Handler ──────────────────────────────────────────────
async function handleCallLoop(session, ws) {
  let silenceTimer = null;
  let audioAccumulator = Buffer.alloc(0);
  let speechActive = false;
  let bargeInTimer = null;

  session.setState('listening');

  // Send initial greeting
  const greeting = await getLLMResponse(session, '[CALL_STARTED]');
  const greetingAudio = await synthesizeSpeech(greeting, session.lead.voice_id);

  if (greetingAudio && ws.readyState === WebSocket.OPEN) {
    session.setState('speaking');
    session.isSpeaking = true;
    ws.send(JSON.stringify({ type: 'audio', data: greetingAudio.toString('base64') }));
    ws.send(JSON.stringify({ type: 'speaking_start' }));
  }

  // Handle incoming audio chunks from Twilio/SIP
  ws.on('message', async (rawMessage) => {
    let msg;
    try {
      msg = JSON.parse(rawMessage);
    } catch {
      return;
    }

    if (msg.type === 'audio_chunk') {
      const chunk = Buffer.from(msg.data, 'base64');
      const isSpeech = await detectSpeech(chunk);

      // ── Barge-in Detection ──────────────────────────────────────────────
      if (isSpeech && session.isSpeaking) {
        if (!bargeInTimer) {
          bargeInTimer = setTimeout(() => {
            // Caller is interrupting — stop TTS immediately
            session.bargeInDetected = true;
            session.isSpeaking = false;
            ws.send(JSON.stringify({ type: 'stop_audio' }));
            session.setState('listening');
            console.log(`[${session.callSid}] Barge-in detected — AI interrupted`);
            bargeInTimer = null;
          }, config.bargeInThresholdMs);
        }
        return;
      }

      if (bargeInTimer && !isSpeech) {
        clearTimeout(bargeInTimer);
        bargeInTimer = null;
      }

      // ── Audio Accumulation ──────────────────────────────────────────────
      if (isSpeech && !session.isSpeaking) {
        speechActive = true;
        audioAccumulator = Buffer.concat([audioAccumulator, chunk]);

        // Reset silence timer on every speech chunk
        if (silenceTimer) clearTimeout(silenceTimer);
        silenceTimer = setTimeout(async () => {
          // Silence detected — process what we have
          if (audioAccumulator.length > 0 && session.state === 'listening') {
            const audioToProcess = audioAccumulator;
            audioAccumulator = Buffer.alloc(0);
            speechActive = false;
            session.setState('processing');

            await processUserSpeech(session, ws, audioToProcess);
          }
        }, config.silenceThresholdMs);
      }
    }

    // Handle TTS playback complete
    if (msg.type === 'audio_ended') {
      session.isSpeaking = false;
      session.setState('listening');
    }
  });

  ws.on('close', () => {
    if (silenceTimer) clearTimeout(silenceTimer);
    if (bargeInTimer) clearTimeout(bargeInTimer);
    finalizeSession(session);
  });
}

// ─── Process User Speech ──────────────────────────────────────────────────────
async function processUserSpeech(session, ws, audioBuffer) {
  const t0 = Date.now();

  // Step 1: STT
  const transcript = await transcribeAudio(audioBuffer);
  console.log(`[${session.callSid}] STT (${Date.now() - t0}ms): "${transcript}"`);

  if (!transcript || transcript.trim().length < 2) {
    session.setState('listening');
    return;
  }

  // Step 2: LLM
  const t1 = Date.now();
  const aiReply = await getLLMResponse(session, transcript);
  console.log(`[${session.callSid}] LLM (${Date.now() - t1}ms): "${aiReply}"`);

  // Step 3: TTS
  const t2 = Date.now();
  const audioResponse = await synthesizeSpeech(aiReply, session.lead.voice_id);
  console.log(`[${session.callSid}] TTS (${Date.now() - t2}ms): ${audioResponse?.length} bytes`);

  const totalLatency = Date.now() - t0;
  console.log(`[${session.callSid}] Total latency: ${totalLatency}ms`);

  // Step 4: Send audio back
  if (audioResponse && ws.readyState === WebSocket.OPEN) {
    session.setState('speaking');
    session.isSpeaking = true;
    ws.send(JSON.stringify({
      type: 'audio',
      data: audioResponse.toString('base64'),
      transcript,
      reply: aiReply,
      latency_ms: totalLatency,
    }));
  } else {
    session.setState('listening');
  }

  // Check if call should end
  if (session.outcome || transcript.toLowerCase().includes('bye') ||
      transcript.toLowerCase().includes('alvida')) {
    setTimeout(() => endCall(session, ws), 3000);
  }
}

// ─── End Call ─────────────────────────────────────────────────────────────────
async function endCall(session, ws) {
  console.log(`[${session.callSid}] Ending call. Duration: ${session.getDurationSec()}s`);
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'end_call' }));
  }
  await finalizeSession(session);
}

async function finalizeSession(session) {
  sessions.delete(session.callSid);

  // Push outcome to CRM
  if (session.outcome) {
    await pushToCRM(session);
  }

  console.log(`[${session.callSid}] Session closed. Active calls: ${sessions.size}`);
}

// ─── CRM Webhook ──────────────────────────────────────────────────────────────
async function pushToCRM(session) {
  try {
    await axios.patch(
      `${process.env.CRM_URL}/api/leads/${session.lead.id}`,
      {
        status: session.outcome?.status || 'called',
        call_duration_sec: session.getDurationSec(),
        call_notes: session.history
          .filter(h => h.role === 'user')
          .map(h => h.content)
          .join(' | '),
        site_visit_scheduled: session.outcome?.site_visit || false,
        next_followup: session.outcome?.callback_date,
        ai_outcome: session.outcome,
        called_at: new Date().toISOString(),
      },
      {
        headers: { Authorization: `Bearer ${process.env.CRM_API_KEY}` },
        timeout: 3000,
      }
    );
    console.log(`[${session.callSid}] CRM updated: ${session.outcome?.status}`);
  } catch (err) {
    console.error('CRM push failed:', err.message);
  }
}

// ─── HTTP Routes ──────────────────────────────────────────────────────────────

// Twilio/Plivo webhook — called when a dial-out call connects
app.post('/call/connected', async (req, res) => {
  const { CallSid, To } = req.body;

  // Look up lead from CRM by phone number
  let lead = { id: 'unknown', name: 'Lead', phone: To, project: 'default' };
  try {
    const crmRes = await axios.get(
      `${process.env.CRM_URL}/api/leads/by-phone/${encodeURIComponent(To)}`,
      { headers: { Authorization: `Bearer ${process.env.CRM_API_KEY}` } }
    );
    lead = crmRes.data;
  } catch (_) {}

  if (sessions.size >= config.maxConcurrentCalls) {
    console.warn('Max concurrent calls reached — rejecting');
    return res.status(503).json({ error: 'capacity_full' });
  }

  const session = new CallSession(CallSid, lead);
  sessions.set(CallSid, session);

  // Return TwiML/XML to connect audio to our WebSocket
  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://${req.headers.host}/audio/${CallSid}" />
  </Connect>
</Response>`);
});

// Initiate outbound call
app.post('/call/dial', async (req, res) => {
  const { lead, phone: rawPhone, project, voice_id, opening_line, dynamic_variables, kb_id } = req.body;
  const phone = lead?.phone || rawPhone;

  if (sessions.size >= config.maxConcurrentCalls) {
    return res.status(503).json({ error: 'capacity_full', active: sessions.size });
  }

  // ── Agni path: use Ravan.ai if credentials are configured ────────────────
  const agniKey    = process.env.AGNI_API_KEY;
  const agniAgent  = process.env.AGNI_AGENT_ID;

  if (agniKey && agniAgent) {
    try {
      const callSid = `agni_${Date.now()}_${Math.random().toString(36).slice(2,8)}`;

      // Build dynamic variables: merge caller info + KB context
      const dynVars = {
        lead_name:    lead?.name    || 'Lead',
        project_name: lead?.project || project || '',
        language:     lead?.language || 'Hindi',
        opening_line: opening_line  || '',
        ...(dynamic_variables || {}),
      };

      console.log(`[Agni] Creating session for ${phone}, KB attached: ${!!dynVars.knowledge_base}`);

      const session = await createAgniSession({
        apiKey:           agniKey,
        agentId:          agniAgent,
        callSid,
        dynamicVariables: dynVars,
      });

      // Store session info so the frontend can poll it
      sessions.set(callSid, {
        callSid,
        lead: lead || { phone, name: 'Lead' },
        state: 'initiated',
        agniSession: session,
        startTime: Date.now(),
        getDurationSec() { return Math.round((Date.now() - this.startTime) / 1000); },
      });

      console.log(`[Agni] Session created: ${session.session_id}, LiveKit URL: ${session.url}`);
      return res.json({
        success:    true,
        call_sid:   callSid,
        session_id: session.session_id,
        livekit_url: session.url,
        access_token: session.access_token,
        provider:   'agni',
        kb_attached: !!dynVars.knowledge_base,
      });
    } catch (err) {
      console.error('[Agni] Session creation failed:', err.message);
      return res.status(500).json({ error: `Agni error: ${err.message}`, provider: 'agni' });
    }
  }

  // ── Twilio fallback: use Twilio if Agni not configured ───────────────────
  if (!config.twilio.accountSid || !config.twilio.authToken) {
    return res.status(503).json({
      error: 'No calling provider configured. Set AGNI_API_KEY + AGNI_AGENT_ID (or Twilio creds) in Railway env vars.',
      hint: 'Go to Railway dashboard → orchestrator service → Variables',
    });
  }

  try {
    const client = twilio(config.twilio.accountSid, config.twilio.authToken);
    const call = await client.calls.create({
      to: phone,
      from: config.twilio.fromNumber,
      url: `https://${req.headers.host}/call/connected`,
      statusCallback: `https://${req.headers.host}/call/status`,
      statusCallbackMethod: 'POST',
    });

    console.log(`Dialing ${phone} → CallSid: ${call.sid}`);
    res.json({ success: true, call_sid: call.sid, provider: 'twilio' });
  } catch (err) {
    console.error('Dial error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Bulk dial from CRM queue
app.post('/call/bulk-dial', async (req, res) => {
  const { project, limit = 10, calls_per_second = 2 } = req.body;

  try {
    // Fetch dialable leads from CRM
    const leadsRes = await axios.get(
      `${process.env.CRM_URL}/api/leads/dialable?project=${project}&limit=${limit}`,
      { headers: { Authorization: `Bearer ${process.env.CRM_API_KEY}` } }
    );
    const leads = leadsRes.data;

    let dialed = 0;
    for (const lead of leads) {
      if (sessions.size >= config.maxConcurrentCalls) break;

      // Rate limiting — don't spam
      await new Promise(r => setTimeout(r, 1000 / calls_per_second));

      try {
        const client = twilio(config.twilio.accountSid, config.twilio.authToken);
        await client.calls.create({
          to: lead.phone,
          from: config.twilio.fromNumber,
          url: `https://${req.headers.host}/call/connected`,
        });
        dialed++;
      } catch (err) {
        console.error(`Failed to dial ${lead.phone}:`, err.message);
      }
    }

    res.json({ dialed, queued: leads.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Call status webhook
app.post('/call/status', async (req, res) => {
  const { CallSid, CallStatus, CallDuration } = req.body;
  console.log(`Call ${CallSid}: ${CallStatus} (${CallDuration}s)`);

  if (['completed', 'failed', 'busy', 'no-answer'].includes(CallStatus)) {
    const session = sessions.get(CallSid);
    if (session) await finalizeSession(session);
  }

  res.sendStatus(200);
});

// Session status — polled by dashboard after call is placed
app.get('/sessions/:sid', (req, res) => {
  const s = sessions.get(req.params.sid);
  if (!s) return res.status(404).json({ error: 'session_not_found' });
  res.json({
    call_sid:     s.callSid,
    state:        s.state || 'active',
    duration_sec: s.getDurationSec ? s.getDurationSec() : 0,
    lead:         s.lead,
    provider:     s.agniSession ? 'agni' : 'sarvam',
    session_id:   s.agniSession?.session_id || null,
    livekit_url:  s.agniSession?.url || null,
  });
});

// Health + metrics
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    active_calls: sessions.size,
    max_calls: config.maxConcurrentCalls,
    uptime_sec: Math.round(process.uptime()),
  });
});

app.get('/metrics', (req, res) => {
  const callList = Array.from(sessions.values()).map(s => ({
    call_sid: s.callSid,
    lead_name: s.lead.name,
    state: s.state,
    duration_sec: s.getDurationSec(),
    turns: Math.floor(s.history.length / 2),
  }));
  res.json({ active_calls: sessions.size, calls: callList });
});

// ─── WebSocket Handler (real-time audio stream) ───────────────────────────────
wss.on('connection', (ws, req) => {
  const callSid = req.url.split('/').pop();
  const session = sessions.get(callSid);

  if (!session) {
    ws.close(4004, 'Session not found');
    return;
  }

  console.log(`[${callSid}] WebSocket audio stream connected`);
  handleCallLoop(session, ws);
});

// ─── Start Server ─────────────────────────────────────────────────────────────
server.listen(config.port, () => {
  console.log(`AI Voice Orchestrator running on port ${config.port}`);
  console.log(`Max concurrent calls: ${config.maxConcurrentCalls}`);
  console.log('Services:', config.services);
});

module.exports = app;
