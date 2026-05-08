FROM node:20-alpine
WORKDIR /app
COPY orchestrator/package.json orchestrator/package-lock.json* ./
RUN npm install --omit=dev && npm install -g pm2
COPY orchestrator/. .
EXPOSE 8000
CMD ["pm2-runtime", "main.js"]
