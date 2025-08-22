// fetchmatches-master.mjs
// Vereisten: Node 18+, puppeteer, json2csv, googleapis, google-auth-library, axios, dotenv
// npm i puppeteer json2csv googleapis google-auth-library axios dotenv

import puppeteer from "puppeteer";
import fs from "fs";
import { parse } from "json2csv";
import { google } from "googleapis";
import { GoogleAuth, Impersonated } from "google-auth-library";
import dotenv from "dotenv";
import axios from "axios";
import { setTimeout as delay } from "node:timers/promises";

dotenv.config();

/* ========= .env =========
SPREADSHEET_ID=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...

SEASON_ID=19
RV_ID=134453

// Referrers (gebruikt om sessie/cookies te (re)seeden)
MATCH_REFERRER_URL=https://matchcentre.kncb.nl/matches/
GRADES_REFERRER_URL=https://matchcentre.kncb.nl/matches/
SEASONS_REFERRER_URL=https://matchcentre.kncb.nl/seasons/

// JSON endpoints (Resultsvault)
MATCH_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/matches/?apiid=1002&action=ors&maxrecs=1000&strmflg=1
GRADES_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/grades/?apiid=1002&seasonid=19
MATCHSTATUS_JSON_API_ENDPOINT=   # (optioneel)
SEASONS_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/seasons/?apiid=1002

// Optioneel gedrag
SLOWDOWN_MS=2000
PUPPETEER_TIMEOUT_MS=30000
RETRY_MAX=4
RETRY_BASE_DELAY_MS=900
RETRY_JITTER_MS=300
REFRESH_REFERRER_EVERY=5
USE_NODE_FETCH_ON_401=1
VERBOSE=1
GRADE_IDS=73942,73943          # optioneel filter, comma-separated

// Sheets via SA of ADC; impersonation optioneel
GOOGLE_APPLICATION_CREDENTIALS=./credentials.json
GOOGLE_IMPERSONATE_SERVICE_ACCOUNT=
========================== */

const {
  MATCH_REFERRER_URL,
  GRADES_REFERRER_URL,
  SEASONS_REFERRER_URL,

  MATCH_JSON_API_ENDPOINT,
  GRADES_JSON_API_ENDPOINT,
  SEASONS_JSON_API_ENDPOINT,
  MATCHSTATUS_JSON_API_ENDPOINT,

  SEASON_ID,
  SPREADSHEET_ID,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  GRADE_IDS,
} = process.env;

const SLOWDOWN_MS = Number(process.env.SLOWDOWN_MS ?? 1200);
const TIMEOUT_MS = Number(process.env.PUPPETEER_TIMEOUT_MS ?? 30000);
const VERBOSE = Number(process.env.VERBOSE ?? 0);
const DISABLE_SHEETS = String(process.env.DISABLE_SHEETS ?? "").toLowerCase() === "1";
const RETRY_MAX = Number(process.env.RETRY_MAX ?? 4);
const RETRY_BASE_DELAY_MS = Number(process.env.RETRY_BASE_DELAY_MS ?? 1200);
const RETRY_JITTER_MS = Number(process.env.RETRY_JITTER_MS ?? 400);
const REFRESH_REFERRER_EVERY = Number(process.env.REFRESH_REFERRER_EVERY ?? 5);
const USE_NODE_FETCH_ON_401 = String(process.env.USE_NODE_FETCH_ON_401 ?? "1") === "1";
const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets";

const jitter = (ms) => ms + Math.floor(Math.random() * RETRY_JITTER_MS);
const backoffMs = (attempt) => jitter(RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1));

// ---------- Telegram ----------
async function notifyTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text,
      disable_web_page_preview: true,
    });
  } catch (e) {
    console.error("❌ Telegram error:", e.response?.data || e.message || e);
  }
}

// ---------- Google Sheets client (ADC + optionele impersonation) ----------
async function getSheetsClient() {
  const impersonate = process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT;

  if (impersonate && impersonate.trim()) {
    // 1) bron-cred (moet cloud-platform scope hebben om IAMCredentials aan te roepen)
    const sourceAuth = new GoogleAuth({ scopes: ["https://www.googleapis.com/auth/cloud-platform"] });
    const sourceClient = await sourceAuth.getClient();

    // 2) impersonated client met expliciete targetScopes (Sheets)
    const impersonated = new Impersonated({
      sourceClient,
      targetPrincipal: impersonate,
      delegates: [],
      lifetime: 3600,
      targetScopes: [SHEETS_SCOPE],
    });
    return google.sheets({ version: "v4", auth: impersonated });
  }

  // Zonder impersonation: directe SA key (of pure ADC) met scopes
  const directAuth = new GoogleAuth({
    keyFile: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    scopes: [SHEETS_SCOPE],
  });
  return google.sheets({ version: "v4", auth: directAuth });
}

console.log(process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT ? "🔐 Using impersonation" : "🔐 Using direct SA/ADC");

// ---------- Sheets helpers ----------
async function getSheetTitles(sheets) {
  const resp = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  return (resp.data.sheets || []).map((s) => s.properties.title);
}

async function ensureSheet(sheets, title) {
  const titles = await getSheetTitles(sheets);
  if (!titles.includes(title)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title } } }] },
    });
    console.log(`➕ Sheet tab "${title}" gemaakt`);
  }
}

async function writeValues(sheets, sheetName, rows) {
  if (!rows?.length) return;
  const headers = uniqueFields(rows);
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:ZZ`,
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: [headers] },
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A2`,
    valueInputOption: "RAW",
    requestBody: { values: rows.map((r) => headers.map((h) => r[h] ?? "")) },
  });
}

// ---------- Utils ----------
function uniqueFields(rows) {
  const set = new Set();
  rows.forEach((r) => Object.keys(r).forEach((k) => set.add(k)));
  return [...set];
}

async function logCookies(page, label) {
  try {
    const cookies = await page.browserContext().cookies();
    console.log(`🍪 ${label}: ${cookies.map(c => c.name).join(',')}`);
  } catch (e) {
    console.warn(`🍪 ${label}: kon cookies niet lezen: ${e.message}`);
  }
}

// Haal entity/tenant id uit een grade-object.
// Resultsvault-grade payloads bevatten vaak een entiteit/associatie id.
// We proberen algemene varianten; loggen als we iets vinden.
function getEntityIdFromGrade(g) {
  const cands = [
    g?.entity_id, g?.entityId, g?.association_id, g?.associationId,
    g?.org_id, g?.orgId, g?.entity?.id, g?.Association?.Id
  ];
  for (const c of cands) {
    if (c != null && String(c).trim() !== "") return String(c).trim();
  }
  return ""; // onbekend -> val terug op RV_ID uit .env
}

// Bouw base RV endpoint dynamisch per entity.
// Vervangt het padsegment rv/<...>/ door de opgegeven entityId.
function buildEntityScopedBase(baseUrl, entityIdFallback, entityIdMaybe) {
  const u = new URL(baseUrl.toString());
  const path = u.pathname.split("/").filter(Boolean); // ['rv','134453','matches',...]
  const idx = path.indexOf("rv");
  if (idx !== -1 && path.length > idx + 1) {
    // vervang huidige rv/<id> door rv/<entityId>
    const newId = (entityIdMaybe && /^\d+$/.test(entityIdMaybe)) ? entityIdMaybe : entityIdFallback;
    path[idx + 1] = String(newId);
    u.pathname = "/" + path.join("/");
  }
  return u;
}


function flattenObject(obj) {
  // Geneste arrays/objects → JSON-string; voorkomt "list_value/struct_value" errors in Sheets
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = v !== null && typeof v === "object" ? JSON.stringify(v) : v;
  }
  return out;
}

function extractArray(any) {
  if (Array.isArray(any)) return any;
  if (any && typeof any === "object") {
    for (const k of ["matches", "data", "items", "rows"]) {
      if (Array.isArray(any[k])) return any[k];
    }
    const arrKey = Object.keys(any).find((k) => Array.isArray(any[k]));
    if (arrKey) return any[arrKey];
  }
  return null;
}

function pickReferrer() {
  // volgorde: expliciete referrers → fallback naar matches/
  const candidates = [
    MATCH_REFERRER_URL,
    GRADES_REFERRER_URL,
    SEASONS_REFERRER_URL,
    "https://matchcentre.kncb.nl/matches/",
  ].filter(Boolean);
  for (const c of candidates) {
    try {
      const u = new URL(c);
      if (/matchcentre\.kncb\.nl$/i.test(u.hostname)) return u.toString();
    } catch {}
  }
  return "https://matchcentre.kncb.nl/matches/";
}

// Normaliseer RV endpoint: host check + seasonId → seasonid (lowercase) + forceer SEASON_ID indien gezet
function normalizeRvEndpoint(name, value) {
  if (!value) throw new Error(`${name} ontbreekt`);
  const u = new URL(value);
  if (!/api\.resultsvault\.co\.uk$/i.test(u.hostname)) {
    throw new Error(`${name} moet naar api.resultsvault.co.uk wijzen (nu: ${u.hostname})`);
  }
  if (u.searchParams.has("seasonId") && !u.searchParams.has("seasonid")) {
    const v = u.searchParams.get("seasonId");
    u.searchParams.delete("seasonId");
    if (v) u.searchParams.set("seasonid", v);
  }
  if (SEASON_ID) u.searchParams.set("seasonid", String(SEASON_ID));
  return u;
}

// Bouw per-grade match-URL
function buildMatchUrl(baseUrl, { gradeId, seasonId }) {
  const u = new URL(baseUrl.toString());
  u.searchParams.delete("gradeid");
  u.searchParams.delete("seasonId");
  u.searchParams.delete("seasonid");
  if (seasonId) u.searchParams.set("seasonid", String(seasonId));
  else if (SEASON_ID) u.searchParams.set("seasonid", String(SEASON_ID));
  u.searchParams.set("gradeid", String(gradeId));
  if (!u.searchParams.has("action")) u.searchParams.set("action", "ors");
  if (!u.searchParams.has("maxrecs")) u.searchParams.set("maxrecs", "1000");
  if (!u.searchParams.has("strmflg")) u.searchParams.set("strmflg", "1");
  return u.toString();
}

function getGradeId(g) {
  const candidates = [g?.gradeId, g?.gradeID, g?.gradeid, g?.grade_id, g?.id, g?.Id, g?.ID, g?.grade?.id, g?.grade?.gradeId, g?.GradeId];
  for (const c of candidates) {
    if (c != null && String(c).trim() !== "") return String(c).trim();
  }
  return "";
}

function getSeasonIdFromGrade(g) {
  const candidates = [g?.seasonid, g?.seasonId, g?.season?.id];
  for (const c of candidates) {
    if (c != null && String(c).trim() !== "") return String(c).trim();
  }
  return "";
}

// Kleine helper om CSV te schrijven (met dynamische headers)
function writeCsv(filename, rows) {
  if (!rows?.length) return;
  try {
    const fields = uniqueFields(rows);
    const csv = parse(rows, { fields, defaultValue: "" });
    fs.writeFileSync(filename, csv, "utf8");
    console.log(`💾 ${filename} geschreven (${rows.length} rijen)`);
  } catch (e) {
    console.warn(`⚠️ Kon ${filename} niet schrijven:`, e.message);
  }
}

// Node-fetch fallback (zonder cookies) — soms werkt RV hiermee tóch op 401/403
async function nodeFetchJson(url) {
  try {
    const res = await fetch(url, {
      headers: {
        accept: "application/json, text/plain;q=0.8, */*;q=0.5",
        "user-agent": "Mozilla/5.0 KNCB-Matchcentre",
      },
    });
    const text = await res.text();
    if (!res.ok) return { __error: `HTTP ${res.status}`, __head: text.slice(0, 220) };
    const h = text.trim().charAt(0);
    if (h !== "{" && h !== "[") return { __error: "Non-JSON response", __head: text.slice(0, 220) };
    try {
      return JSON.parse(text);
    } catch (e) {
      return { __error: `JSON parse error: ${e.message}`, __head: text.slice(0, 220) };
    }
  } catch (e) {
    return { __error: `Fetch failed: ${e.message}` };
  }
}

// 🌐 Versterkte fetch in page-context met credentials, Origin/Referer en backoff.
//  - Op 401/403: referrer herladen (sessie reseeden) en retry
async function pageFetchJsonWithRetry(page, url, { refPageUrl, label }) {
  for (let attempt = 1; attempt <= RETRY_MAX; attempt++) {
    const result = await page.evaluate(async (u, ref) => {
      try {
        const origin = new URL(ref).origin;
        const resp = await fetch(u, {
          credentials: "include",
          // Gebruik fetch 'referrer' i.p.v. header 'Referer' (header wordt door browser genegeerd/set)
          referrer: ref,
          referrerPolicy: "origin-when-cross-origin",
          headers: {
            accept: "application/json, text/plain;q=0.8, */*;q=0.5",
            "x-requested-with": "XMLHttpRequest",
            // Sommige backends checken 'Origin' streng bij credentials:
            Origin: origin,
            "Cache-Control": "no-cache",
            Pragma: "no-cache",
          },
          mode: "cors",
        });
        const ct = resp.headers.get("content-type") || "";
        const text = await resp.text();
        if (!resp.ok) {
          return { __error: `HTTP ${resp.status}`, __status: resp.status, __head: text.slice(0, 220) };
        }
        if (!/application\/json/i.test(ct)) {
          const h = text.trim().charAt(0);
          if (h !== "{" && h !== "[") return { __error: "Non-JSON response", __head: text.slice(0, 220) };
          try {
            return JSON.parse(text);
          } catch (e) {
            return { __error: `JSON parse error: ${e.message}`, __head: text.slice(0, 220) };
          }
        }
        try {
          return JSON.parse(text);
        } catch (e) {
          return { __error: `JSON parse error: ${e.message}`, __head: text.slice(0, 220) };
        }
      } catch (e) {
        return { __error: `Fetch failed: ${e.message}` };
      }
    }, url, refPageUrl);

    if (!result?.__error) return result;

    const code = Number(result.__status || 0);
    const transient = [401, 403, 408, 420, 429, 500, 502, 503, 504].includes(code);
    console.warn(
      `⚠️ [attempt ${attempt}/${RETRY_MAX}] ${label ?? ""} ${url} -> ${result.__error}${
        result.__head ? ` | head=${result.__head}` : ""
      }`
    );

    // Fallback: Node-fetch op 401/403 (soms werkt RV buiten browser beter)
    if (USE_NODE_FETCH_ON_401 && (code === 401 || code === 403)) {
      const alt = await nodeFetchJson(url);
      if (!alt?.__error) {
        console.log("🔁 Fallback via Node-fetch geslaagd:", label ?? url);
        return alt;
      }
    }

    if (attempt < RETRY_MAX && transient) {
      // Sessieverversing: referrer herladen vóór retry
      if (refPageUrl) {
        try {
          await page.goto(refPageUrl, { waitUntil: "networkidle0" });
        } catch {}
      }
      const waitMs = backoffMs(attempt);
      await delay(waitMs);
      continue;
    }
    return result; // definitieve fout
  }
}

// ---------- MAIN ----------
(async () => {
  let browser;
  try {
    // Normaliseer endpoints
    const GRADES_URL = normalizeRvEndpoint("GRADES_JSON_API_ENDPOINT", GRADES_JSON_API_ENDPOINT);
    const MATCH_URL = normalizeRvEndpoint("MATCH_JSON_API_ENDPOINT", MATCH_JSON_API_ENDPOINT);
    const SEASONS_URL = SEASONS_JSON_API_ENDPOINT ? normalizeRvEndpoint("SEASONS_JSON_API_ENDPOINT", SEASONS_JSON_API_ENDPOINT) : null;

    const refPage = pickReferrer();

    // Sheets init
    let sheets = null;
    if (DISABLE_SHEETS) {
      console.log("ℹ️ Sheets uitgeschakeld (DISABLE_SHEETS=1). CSV’s worden wel geschreven.");
    } else if (SPREADSHEET_ID) {
      try {
        console.log(
          "🔐 Init Google Sheets via ADC",
          process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT ? "(impersonation)" : ""
        );
        sheets = await getSheetsClient();
        await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
        console.log("✅ Google Sheets auth OK");
      } catch (e) {
        console.warn("⚠️ Sheets init mislukt; ga verder zonder Sheets:", e.message);
        sheets = null;
      }
    } else {
      console.log("ℹ️ Geen SPREADSHEET_ID gezet; ga verder zonder Sheets.");
    }

    // Puppeteer start
    console.log("🚀 Start puppeteer…");
    browser = await puppeteer.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-web-security", "--disable-site-isolation-trials"],
    });
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(TIMEOUT_MS);
    // Stabiele UA/headers voor strengere origin checks
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    );
    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
    });

    page.on("console", (msg) => console.log("PAGE LOG →", msg.text()));
    page.on("requestfailed", (req) => console.log("PAGE REQ FAIL →", req.failure()?.errorText, req.url()));

    // Referrer openen
    console.log("📥 Open referrer pagina…", refPage);
    await page.goto(refPage, { waitUntil: "networkidle0" });
    await delay(SLOWDOWN_MS);

    // 1) GRADES ophalen + meteen wegschrijven naar GRADES-tab en GRADES.csv
    console.log("📥 Haal grades op…");
    const gradesJson = await pageFetchJsonWithRetry(page, GRADES_URL.toString(), { refPageUrl: refPage, label: "grades" });
    if (gradesJson?.__error) throw new Error(`Grades endpoint error: ${gradesJson.__error}`);

    const gradesArrRaw = extractArray(gradesJson) || [];
    console.log(`▶ Gevonden ${gradesArrRaw.length} raw grades`);

    // → NEW: ook GRADES naar sheet + CSV (flattened)
    const gradesRows = gradesArrRaw.map(flattenObject);
    if (gradesRows.length) {
      writeCsv("GRADES.csv", gradesRows);
      if (sheets) {
        await ensureSheet(sheets, "GRADES");
        await writeValues(sheets, "GRADES", gradesRows);
        console.log(`📈 Sheet "GRADES" geüpdatet (${gradesRows.length} rijen)`);
      }
    }

    // Optioneel filter op GRADE_IDS
    const onlyIds = (GRADE_IDS || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map((x) => String(x));

    if (VERBOSE >= 1) console.log("🔎 Filter GRADE_IDS =", onlyIds);

    const gradesArr = gradesArrRaw.filter((g) => {
      const gid = getGradeId(g);
      if (!gid) {
        if (VERBOSE >= 1) console.log("⚠️ Grade zonder herkenbare id, skip:", g);
        return false;
      }
      return !onlyIds.length || onlyIds.includes(gid);
    });

    console.log(
      `🧮 Te verwerken grades: ${gradesArr.length}${
        onlyIds.length ? ` (gefilterd uit ${gradesArrRaw.length})` : ""
      }`
    );

    const masterRows = [];
    let processed = 0;

    // 2) Per grade wedstrijden ophalen
    for (const g of gradesArr) {
      const gid = getGradeId(g);
      const seasonInGrade = getSeasonIdFromGrade(g) || SEASON_ID || "";

      console.log(`\n--- 🔁 Grade ${gid} (season ${seasonInGrade || SEASON_ID || "n/a"}) ---`);
      const sheetName = `Grade_${gid}`;
      if (sheets) await ensureSheet(sheets, sheetName);

      const entityId = getEntityIdFromGrade(g);             // ← nieuw
const scopedBase = buildEntityScopedBase(MATCH_URL,   // ← nieuw
                    process.env.RV_ID, entityId);
const matchUrl = buildMatchUrl(scopedBase, {          // ← aangepast: scopedBase i.p.v. MATCH_URL
  gradeId: gid,
  seasonId: seasonInGrade || SEASON_ID
});
console.log(`🏷️ entityId=${entityId || process.env.RV_ID} → ${matchUrl}`);


      const matchesJson = await pageFetchJsonWithRetry(page, matchUrl, {
        refPageUrl: refPage,
        label: `grade ${gid}`,
      });

if (matchesJson?.__error) {
  const msg = `⚠️ Fout bij grade ${gid} (entity=${entityId || process.env.RV_ID}): ${matchesJson.__error}${
    matchesJson.__head ? ` | head=${matchesJson.__head}` : ""
  }`;
  console.error(msg);
  await notifyTelegram(msg);
  // Bonus: bij 401 eenmalig "hard refresh" van de referrer proberen
  if (String(matchesJson.__error).includes("HTTP 401")) {
    try {
      console.log("🔄 401: hard refresh van referrer & korte pauze…");
      await page.goto(refPage, { waitUntil: "networkidle0" });
      await delay(1500);
    } catch {}
  }
  continue;
}

      const dataArr = extractArray(matchesJson);
      if (!Array.isArray(dataArr)) {
        const msg = `⚠️ Geen array gevonden voor grade ${gid}; sla over`;
        console.warn(msg);
        await notifyTelegram(msg);
        continue;
      }

      console.log(`✅ ${sheetName}: ${dataArr.length} matches`);
      if (VERBOSE >= 1 && dataArr.length) {
        console.log(`🧪 Eerste item:\n${JSON.stringify(dataArr[0], null, 2)}`);
      }

      const rows = dataArr.map((obj) => {
        const flat = flattenObject(obj);
        flat._grade = gid;
        flat._season = seasonInGrade || SEASON_ID || "";
        return flat;
      });

      // Per-grade CSV en sheet
      if (rows.length) {
        writeCsv(`${sheetName}.csv`, rows);
        if (sheets) {
          await writeValues(sheets, sheetName, rows);
          console.log(`📈 Sheet "${sheetName}" geüpdatet`);
        }
        masterRows.push(...rows);
      }

      processed++;
      // Sessieverversing (cookies/tokens) na elke N grades
      if (REFRESH_REFERRER_EVERY > 0 && processed % REFRESH_REFERRER_EVERY === 0) {
        console.log("🔄 Sessieverversing: referrer herladen…", refPage);
        try {
await logCookies(page, 'before refresh');
await page.goto(refPage, { waitUntil: 'networkidle0' });
await logCookies(page, 'after refresh');
        } catch {}
        await delay(SLOWDOWN_MS);
      
      }

ck = await page.browserContext().cookies();
console.log('🍪 after:', ck.map(c => c.name).join(','));

      await delay(SLOWDOWN_MS);
    }

    // 3) MASTER schrijven (alle grades samengevoegd)
    if (masterRows.length) {
      console.log(`\n📊 MASTER opbouwen: ${masterRows.length} rijen`);
      writeCsv("MASTER.csv", masterRows);                // ← alleen MASTER.csv, niet meer matches.csv
      if (sheets) {
        await ensureSheet(sheets, "MASTER");
        await writeValues(sheets, "MASTER", masterRows);
        console.log("📈 MASTER sheet bijgewerkt");
      }
    }

    console.log("\n✅ Klaar!");
  } catch (err) {
    console.error("🛑 FATAAL:", err.message);
    await notifyTelegram(`Fatal error: ${err.message}`);
  } finally {
    if (browser) {
      await browser.close();
      console.log("✅ Browser gesloten");
    }
  }
})();
