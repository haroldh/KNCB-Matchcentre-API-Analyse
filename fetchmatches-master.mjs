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

// JSON endpoints (Resultsvault) — LET OP: behoud de trailing slash in /matches/
MATCH_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/matches/?apiid=1002&action=ors&maxrecs=1000&strmflg=1
GRADES_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/grades/?apiid=1002&seasonid=19
MATCHSTATUS_JSON_API_ENDPOINT=
SEASONS_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/seasons/?apiid=1002

// Clubscript-vereiste
IAS_API_KEY=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   # ← zet hier je echte waarde in

// Optioneel gedrag
SLOWDOWN_MS=200      # clubscript gebruikt ~200ms
PUPPETEER_TIMEOUT_MS=30000
RETRY_MAX=2          # bij 401 na warm-up nog 1 retry is genoeg
RETRY_BASE_DELAY_MS=600
RETRY_JITTER_MS=250
REFRESH_REFERRER_EVERY=8
USE_NODE_FETCH_ON_401=0  # niet nodig als warm-up + IAS goed staan
VERBOSE=1
GRADE_IDS=           # optioneel filter, comma-separated

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

  SEASON_ID,
  SPREADSHEET_ID,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  GRADE_IDS,
  IAS_API_KEY,
} = process.env;

const SLOWDOWN_MS = Number(process.env.SLOWDOWN_MS ?? 200);
const TIMEOUT_MS = Number(process.env.PUPPETEER_TIMEOUT_MS ?? 30000);
const VERBOSE = Number(process.env.VERBOSE ?? 0);
const DISABLE_SHEETS = String(process.env.DISABLE_SHEETS ?? "").toLowerCase() === "1";
const RETRY_MAX = Number(process.env.RETRY_MAX ?? 2);
const RETRY_BASE_DELAY_MS = Number(process.env.RETRY_BASE_DELAY_MS ?? 600);
const RETRY_JITTER_MS = Number(process.env.RETRY_JITTER_MS ?? 250);
const REFRESH_REFERRER_EVERY = Number(process.env.REFRESH_REFERRER_EVERY ?? 8);
const USE_NODE_FETCH_ON_401 = String(process.env.USE_NODE_FETCH_ON_401 ?? "0") === "1";
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
    const sourceAuth = new GoogleAuth({ scopes: ["https://www.googleapis.com/auth/cloud-platform"] });
    const sourceClient = await sourceAuth.getClient();
    const impersonated = new Impersonated({
      sourceClient,
      targetPrincipal: impersonate,
      delegates: [],
      lifetime: 3600,
      targetScopes: [SHEETS_SCOPE],
    });
    return google.sheets({ version: "v4", auth: impersonated });
  }

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

function defaultUa() {
  return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
}

function flattenObject(obj) {
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

// Bouw per-grade match-URL — base moet een trailing slash hebben; laat .env zo staan
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

// ---------- NIEUW: warm-up per grade (zoals clubscript via iframe doet) ----------
async function warmupSession(page, { entityId, gradeId, seasonId }) {
  const warmUrl = `https://matchcentre.kncb.nl/matches/?entity=${entityId}&grade=${gradeId}&season=${seasonId}`;
  try {
    const warm = await page.browser().newPage();
    await warm.setViewport({ width: 160, height: 100 });
    await warm.goto(warmUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
    await warm.waitForTimeout(1000);
    await warm.close();
    if (VERBOSE >= 1) console.log(`🔥 warmup ok for grade ${gradeId}`);
  } catch (e) {
    console.warn(`🔥 warmup failed for grade ${gradeId}: ${e.message}`);
  }
}

// ---------- In-page fetch met IAS header + referrer ----------
async function fetchJsonInPage(page, url, refPageUrl, label) {
  return await page.evaluate(async (u, ref, ias) => {
    try {
      const res = await fetch(u, {
        credentials: "include",
        referrer: ref,
        referrerPolicy: "strict-origin-when-cross-origin", // Chrome default; expliciet voor duidelijkheid
        headers: {
          "Accept": "application/json, text/plain;q=0.8, */*;q=0.5",
          "X-IAS-API-REQUEST": ias,               // ← essentieel; clubscript stuurt deze altijd mee
          "x-requested-with": "XMLHttpRequest",
          "cache-control": "no-cache",
          "pragma": "no-cache",
        },
        method: "GET",
        mode: "cors",
      });
      const text = await res.text();
      if (!res.ok) return { __error: `HTTP ${res.status}`, __status: res.status, __head: text.slice(0, 220) };
      try { return JSON.parse(text); }
      catch (e) { return { __error: `JSON parse error: ${e.message}`, __head: text.slice(0, 220) }; }
    } catch (e) {
      return { __error: `Fetch failed: ${e.message}` };
    }
  }, url, refPageUrl, IAS_API_KEY);
}

// ---------- MAIN ----------
(async () => {
  let browser;
  try {
    if (!IAS_API_KEY) {
      throw new Error("IAS_API_KEY ontbreekt in .env (X-IAS-API-REQUEST verplicht).");
    }

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
        console.log("🔐 Init Google Sheets via ADC", process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT ? "(impersonation)" : "");
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

    // Puppeteer
    console.log("🚀 Start puppeteer…");
    browser = await puppeteer.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-web-security", "--disable-site-isolation-trials"],
    });
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(TIMEOUT_MS);
    await page.setUserAgent(defaultUa());
    await page.setExtraHTTPHeaders({ "Accept-Language": "en-US,en;q=0.9,nl;q=0.8" });

    page.on("console", (msg) => console.log("PAGE →", msg.text()));
    page.on("requestfailed", (req) => {
      const u = req.url();
      if (/googletagmanager|google-analytics|gtag/i.test(u)) return;
      console.log("REQ FAIL →", req.failure()?.errorText, u);
    });

    // Referrer openen + korte warm-up (root)
    console.log("📥 Open referrer pagina…", refPage);
    await page.goto(refPage, { waitUntil: "networkidle0" });

    if (process.env.API_URL) {
      console.log("📥 Warm-up API call…", process.env.API_URL);
      await fetchJsonInPage(page, process.env.API_URL, refPage, "warmup");
    }

    await delay(SLOWDOWN_MS);

    // 1) GRADES
    console.log("📥 Haal grades op…");
    const gradesJson = await fetchJsonInPage(page, GRADES_URL.toString(), refPage, "grades");
    if (gradesJson?.__error) throw new Error(`Grades endpoint error: ${gradesJson.__error}`);

    const gradesArrRaw = extractArray(gradesJson) || [];
    console.log(`▶ Gevonden ${gradesArrRaw.length} raw grades`);

    // GRADES → sheet + CSV (flatten)
    const gradesRows = gradesArrRaw.map(flattenObject);
    if (gradesRows.length) {
      writeCsv("GRADES.csv", gradesRows);
      if (sheets) {
        await ensureSheet(sheets, "GRADES");
        await writeValues(sheets, "GRADES", gradesRows);
        console.log(`📈 Sheet "GRADES" geüpdatet (${gradesRows.length} rijen)`);
      }
    }

    // Optioneel filter
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
      `🧮 Te verwerken grades: ${gradesArr.length}${onlyIds.length ? ` (gefilterd uit ${gradesArrRaw.length})` : ""}`
    );

    const masterRows = [];
    let processed = 0;

    // 2) Per grade wedstrijden
    for (const g of gradesArr) {
      const gid = getGradeId(g);
      const seasonInGrade = getSeasonIdFromGrade(g) || SEASON_ID || "";

      console.log(`\n--- 🔁 Grade ${gid} (season ${seasonInGrade || SEASON_ID || "n/a"}) ---`);
      const sheetName = `Grade_${gid}`;
      if (sheets) await ensureSheet(sheets, sheetName);

      // Warm-up per grade (zoals clubscript via iframe)
      const entityId = String(process.env.RV_ID || "134453");
      await warmupSession(page, { entityId, gradeId: gid, seasonId: seasonInGrade || SEASON_ID });

      // Bouw matches-URL en fetch IN PAGE met IAS header + referrer
      const matchUrl = buildMatchUrl(MATCH_URL, { gradeId: gid, seasonId: seasonInGrade || SEASON_ID });
      console.log("🌐 Fetch matches:", matchUrl);

      let matchesJson = await fetchJsonInPage(page, matchUrl, refPage, `grade ${gid}`);
      if (matchesJson?.__error && String(matchesJson.__error).includes("HTTP 401")) {
        console.log("🔄 401: nogmaals warm-up en retry…");
        await warmupSession(page, { entityId, gradeId: gid, seasonId: seasonInGrade || SEASON_ID });
        await delay(250);
        matchesJson = await fetchJsonInPage(page, matchUrl, refPage, `grade ${gid} (retry)`);
      }

      if (matchesJson?.__error) {
        const msg = `⚠️ Fout bij grade ${gid}: ${matchesJson.__error}${matchesJson.__head ? ` | head=${matchesJson.__head}` : ""}`;
        console.error(msg);
        await notifyTelegram(msg);
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

      if (rows.length) {
        writeCsv(`${sheetName}.csv`, rows);
        if (sheets) {
          await writeValues(sheets, sheetName, rows);
          console.log(`📈 Sheet "${sheetName}" geüpdatet`);
        }
        masterRows.push(...rows);
      }

      processed++;
      if (REFRESH_REFERRER_EVERY > 0 && processed % REFRESH_REFERRER_EVERY === 0) {
        console.log("🔄 Sessieverversing: referrer herladen…", refPage);
        try { await page.goto(refPage, { waitUntil: "networkidle0" }); } catch {}
        await delay(SLOWDOWN_MS);
      }

      await delay(SLOWDOWN_MS);
    }

    // 3) MASTER
    if (masterRows.length) {
      console.log(`\n📊 MASTER opbouwen: ${masterRows.length} rijen`);
      writeCsv("MASTER.csv", masterRows);
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
