# Self-host (Docker) – Executable-sourced OIDC (Workload Identity Federation)

Dit project draait je `fetchmatches-master.mjs` in een container met:

- Headless Chrome-deps voor Puppeteer
- **Executable-sourced OIDC** richting Google Cloud (geen SA-keyfile!)
- Milieuvariabelen voor je app + IdP
- Geschikt voor **Synology Container Manager** of lokaal `docker compose`

---

## 1) Vereisten

- **GCP Workload Identity Federation** (WIF):

  1. Maak een **Service Account** met Sheets-rechten (bv. `roles/spreadsheets.editor`).
  2. Maak een **Workload Identity Pool + OIDC-provider** (issuer/audience configureren).
  3. Geef de SA de rol **`roles/iam.workloadIdentityUser`** op de provider.
  4. Genereer een **credential config** (ADC) met `gcloud iam workload-identity-pools create-cred-config` (executable-variant) en sla op als `docker/config/gcp-wif-exec.json`. :contentReference[oaicite:4]{index=4}

- **IdP** (bv. Keycloak/Auth0) met client-credentials die een **OIDC ID-token** of JWT access token levert (met geldige `iss/aud/exp/iat`) voor jouw WIF-provider.

- **Google Sheet**: deel het target-Sheet met de **Service Account** (dezelfde SA als hierboven), anders krijg je 403/404.

---

## 2) Bestanden in deze map

### `Dockerfile`

Bouwt een image op `node:20-bookworm-slim` met alle OS-libs voor Headless Chrome (Puppeteer). :contentReference[oaicite:5]{index=5}

### `docker-compose.yml`

Start je service:

- Mount `config/` (met `gcp-wif-exec.json`)
- Mount `bin/` (met `get-oidc.sh`)
- Zet **GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1** en `GOOGLE_APPLICATION_CREDENTIALS=/config/gcp-wif-exec.json`. :contentReference[oaicite:6]{index=6}

### `bin/get-oidc.sh`

Executable dat **on-demand** een OIDC-token ophaalt bij je IdP (client_credentials) en **JSON** naar **stdout** schrijft in het vereiste format (met `id_token` en `expiration_time`). :contentReference[oaicite:7]{index=7}

### `config/gcp-wif-exec.json`

**Credential config** die verwijst naar jouw WIF-provider én naar `/usr/src/app/bin/get-oidc.sh`.

---

## 3) In te vullen variabelen

Zet onderstaande env-variabelen in **Synology → Project → Environment** óf maak een `.env` in de repo-root (zie `../../.env.example`):

- **Google / OIDC**

  - `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1` (staat al in compose)
  - `GOOGLE_APPLICATION_CREDENTIALS=/config/gcp-wif-exec.json` (staat al in compose)
  - `KC_TOKEN_URL`, `KC_CLIENT_ID`, `KC_CLIENT_SECRET`, `KC_AUDIENCE` (IdP gegevens voor token)

- **App**
  - `SEASON_ID`, `RV_ID`, `MATCH_REFERRER_URL`, `GRADES_REFERRER_URL`, `SEASONS_REFERRER_URL`
  - `API_URL`, `MATCH_JSON_API_ENDPOINT`, `GRADES_JSON_API_ENDPOINT`, `SEASONS_JSON_API_ENDPOINT`
  - `SPREADSHEET_ID`
  - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` (numerieke chat-id)
  - `IAS_API_KEY` (verplicht voor de fetch naar Resultsvault)
  - `CSV_OUTPUT`, `GRADE_IDS`, `VERBOSE`, `DISABLE_SHEETS` (optioneel)

---

## 4) Build & Run

### Lokaal (in project root):

```bash
docker compose -f docker/docker-compose.yml up --build
```
