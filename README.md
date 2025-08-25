KNCB MatchFetcher

Automatiseer het ophalen van wedstrijddatasets van kncb.matchcentre.nl, sla de data op in Google Sheets, en beheer alles via een cron-gedreven GitHub Actions workflow.

Overzicht

Deze Node.js-app:

Beheert sessies met Puppeteer voor het ophalen van grades en matchdata via Resultsvault-API’s.

Schrijft data weg als CSV’s én naar aparte tabs in een Google Sheets-document (via Sheets API).

Voegt automatisch log-structuren toe (RUNS, LOG, CHANGES, MASTER) inclusief metadata zoals Git-versie, tijdstempels, hash, en wijzigingslogs.

Draait lokaal en via GitHub Actions (cron of handmatig) met alle authentificatie via OIDC (geen gecommit credentials).

Stuurt notificaties via Telegram bij fouten.

Highlights / Waarom dit nuttig is

🆗 Volledig geautomatiseerd: van scraping tot Sheets-updates, inclusief diff-logging.

📊 Transparante logging: per run, per match, inclusief changelog en metadata.

🔒 Veilige CI: authenticatie via OIDC zonder API keys in repo.

🚀 Eenvoudig beheer: GitHub Actions met ad hoc of schematische uitvoering.

Robuust script: behandelt gradeloops, timeouts, retries, diff-logica, en CSV-export.

Prerequisites

Node.js 18+

Google Cloud

Sheets API ingeschakeld

Service account met juiste rechten (Editor of Sheets)

Workload Identity Federation in plaats van .json keys:
gebruik google-github-actions/auth@v2 in CI (we slaan zo geen credentials op in repo). 
ReadMe
freecodecamp.org
+3
Google Cloud Community
+3
Google Developer forums
+3
GitHub

Telegram bot + chat ID (numeriek, geen alias) voor meldingen

Git repository met tags of commits

Je script logt automatisch versie via git describe --tags --always --dirty of commit SHA. Voor GitHub Actions moet actions/checkout met fetch-depth: 0 om tags te zaak te laten werken. 
arXiv
+2
reddit.com
+2
arXiv
+5
GitHub
+5
GitHub
+5

GitHub Secrets / Variables:

Secrets: SPREADSHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, IAS_API_KEY, GCP_WIF_PROVIDER, GCP_SERVICE_ACCOUNT

Vars: SEASON_ID, RV_ID, REFERRER_URLS, *_API_ENDPOINT URLs, GRADE_IDS, VERBOSE, etc.

Gebruik & setup
Lokaal:

npm install

.env vullen (zie voorbeeld hierboven)

Run:

node fetchmatches-master.mjs


Bekijk CSV-export (MASTER.csv, Grade_{id}.csv) en Google Sheet tabs

Instaureer headers via voegHeadersToe.mjs als nodig (maakt kolomkoppen aan voor RUNS/LOG/CHANGES)

GitHub Actions:

Gebruik de meegeleverde workflow in .github/workflows/fetch.yml:

Draait handmatig of via cron (UTC schema)

Auth via OIDC, geen keyfile nodig

Checkout met volledige git-geschiedenis (tags)

Runt script, uploadt CSV als artefact

Configuratieparameters
Varaibel	Omschrijving
SEASON_ID, RV_ID	Parameters voor Resultsvault-API
*_REFERRER_URL, *_JSON_API_ENDPOINT	Bron-URL’s voor scraping/fetching
IAS_API_KEY	Vereist header voor access
SPREADSHEET_ID	Doel Google Sheet ID
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID	Voor notificaties
GRADE_IDS	Optioneel filter op grade IDs
VERBOSE	Logdetail, 0 of 1
DISABLE_SHEETS	Skip Sheets-writes bij debugging of lokaal test
SLOWDOWN_MS, PUPPETEER_TIMEOUT_MS, RETRIES	Tuning-fetch gedrag

Tip: 7 + 8 zijn defaults ingesteld, maar kunnen via .env of CI variabelen geborgd worden.

Aanbevelingen / Wat nu missen we nog?

Healthcheck endpoint: een eenvoudige ping of log van runstatus (via API of Slack/Telegram).

Rate limiting en retry: je hebt basis-retries, maar overweeg ook exponential backoff bijvoorbeeld bij 429 responses.

Alerting op schema-fouten: bij falen van cron-run (bijv. Telegram bij fatal).

Verspreiding logs: via GitHub Secrets, of sheet met summary dashboards (grafiek als tab)?

Replay mode voor development: mogelijkheid om met eerder opgeslagen JSON runs in te laden (speed debugging).

Test suite: unit-test voor hash & diff-logica; API-mocks voor development.

Documentatie badge: je README kan een build-status badge bevatten (CI staat bovenin), mogelijk later via shields.io.

Voorbeeld structuur
├── fetchmatches-master.mjs
├── voegHeadersToe.mjs
├── .github/
│    └── workflows/
│         └── fetch.yml
├── package.json
└── README.md
