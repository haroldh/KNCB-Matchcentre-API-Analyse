# docker/Dockerfile
FROM node:20-bookworm-slim

# Chrome/Puppeteer dependencies (Debian bookworm)
RUN apt-get update && apt-get install -y \
  ca-certificates fonts-liberation libasound2 libatk1.0-0 libatk-bridge2.0-0 \
  libc6 libcairo2 libcups2 libdbus-1-3 libdrm2 libexpat1 libgbm1 libglib2.0-0 \
  libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 \
  libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 \
  libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 \
  wget curl xdg-utils jq && \
  rm -rf /var/lib/apt/lists/*
# (Setje veelgebruikte OS-libs voor Headless Chrome in Docker.) :contentReference[oaicite:1]{index=1}

WORKDIR /usr/src/app

# Alleen manifesten eerst voor betere cache
COPY ../package*.json ./
RUN npm ci --omit=dev

# App code uit repo root kopiëren
COPY ../fetchmatches-master.mjs ./

# Helpers
COPY bin/ ./bin/
RUN chmod +x ./bin/get-oidc.sh

# Config (credential config wordt als volume gemount)
# ENV voor prettige logging en juiste tijdzone
ENV TZ=Europe/Amsterdam \
    NODE_ENV=production

CMD ["node", "fetchmatches-master.mjs"]
