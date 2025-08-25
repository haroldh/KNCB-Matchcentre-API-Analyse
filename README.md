#  KNCB MatchFetcher

_Automatisch data ophalen van KNCB MatchCentre, wegschrijven naar Google Sheets, en loggen via GitHub Actions._

---

##  Wat doet dit project?

- **Scraping**: gebruikt Puppeteer om grades- en matchdata op te halen via de Resultsvault API.
- **Opslag**:
  - Exporteert als CSV-bestanden (`MASTER.csv`, `Grade_{id}.csv`).
  - Schrijft data naar een Google Sheet (tabbladen per grade, plus `MASTER`, `CHANGES`, `LOG`, `RUNS`).
- **Logging**:
  - Houdt metadata bij zoals `run_id`, `version` (git), timestamps, `_hash`, `_active`, diffs (`created`, `updated`, `deleted`).
- **Versiebeheer**: logt automatisch Git-versie of CI-commit via `git describe` of SHA.
- **Notificaties**: stuurt foutmeldingen via Telegram bij problemen.
- **CI-gebruik**: draait lokaal of via een GitHub Actions workflow, authenticeert via OIDC (geen credentials nodig).

---

##  Prerequisites

| Vereiste                    | Beschrijving                                                                                  |
|-----------------------------|----------------------------------------------------------------------------------------------|
| Node.js 18+                 | JavaScript runtime voor dit script                                                          |
| Google Cloud                | Sheets API ingeschakeld en service account met Sheets-rechten (via OIDC, geen keyfile)       |
| Telegram bot + chat ID      | Voor foutmeldingen via Telegram (numeriek, geen alias)                                       |
| Git repository met tags     | Voor versie-logging (`git describe` of commit SHA)                                           |
| GitHub Secrets & Variables: |                                                                                              |
| ‣ `SPREADSHEET_ID`          | ID van target Google Sheet                                                                   |
| ‣ `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | Voor Telegram notificaties                                                              |
| ‣ `IAS_API_KEY`             | Vereist voor toegang tot API-endpoint                                                       |
| ‣ `GCP_WIF_PROVIDER`, `GCP_SERVICE_ACCOUNT` | Voor OIDC-authenticatie via GitHub Actions                                      |
| ||<br>**En bij voorkeur**: `SEASON_ID`, `RV_ID`, `REFERRER_URLS`, `*_API_ENDPOINT`, `GRADE_IDS`, `VERBOSE`, enz.|

---

##  Lokaal gebruik

1. `/fetchmatches-master.mjs` en dependencies in projectroot.
2. `.env` vullen met alle vereiste parameters & secrets.
3. Installeer dependencies:
   ```bash
   npm install
