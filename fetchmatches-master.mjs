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
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

dotenv.config();

// --- sanity logging over sheets-config ---
console.log(`[Sheets] SPREADSHEET_ID present: ${Boolean(process.env.SPREADSHEET_ID)}`);
console.log(`[Sheets] DISABLE_SHEETS: ${String(process.env.DISABLE_SHEETS ?? "0")}`);

/* ========= .env =========
SPREADSHEET_ID=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...

SEASON_ID=19
RV_ID=134453

// Referrers (sessie/cookies seeden)
MATCH_REFERRER_URL=https://matchcentre.kncb.nl/matches/
GRADES_REFERRER_URL=https://matchcentre.kncb.nl/matches/
SEASONS_REFERRER_URL=https://matchcentre.kncb.nl/seasons/

// JSON endpoints (Resultsvault)
MATCH_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/matches/?apiid=1002&action=ors&maxrecs=1000&strmflg=1
GRADES_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/grades/?apiid=1002&seasonid=19
MATCHSTATUS_JSON_API_ENDPOINT=
SEASONS_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/seasons/?apiid=1002

// Clubscript-vereiste
IAS_API_KEY=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

// Optioneel gedrag
SLOWDOWN_MS=200
PUPPETEER_TIMEOUT_MS=30000
RETRY_MAX=2
RETRY_BASE_DELAY_MS=600
RETRY_JITTER_MS=250
REFRESH_REFERRER_EVERY=8
USE_NODE_FETCH_ON_401=0
VERBOSE=1
GRADE_IDS=           // optioneel filter, comma-separated

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
const backoffMs = (a) => jitter(RETRY_BASE_DELAY_MS * Math.pow(2, a - 1));

/* ----------------------------
   Script-identiteit & versie
-----------------------------*/
const SCRIPT_NAME = "fetchmatches-master.mjs";
// VERSION wordt dynamisch bepaald via Git/CI:
let VERSION = "v1"; // fallback; overschreven door resolveCodeVersion()

function pickCiSha() {
  const cands = [
    process.env.GIT_COMMIT,
    process.env.GITHUB_SHA,          // GitHub Actions
    process.env.CI_COMMIT_SHA,       // GitLab CI
    process.env.BITBUCKET_COMMIT,    // Bitbucket Pipelines
    process.env.VERCEL_GIT_COMMIT_SHA,
    process.env.NETLIFY_COMMIT_REF,
  ].filter(Boolean);
  return cands.length ? cands[0] : null;
}
function tryExec(cmd) {
  try {
    const out = execSync(cmd, { stdio: ["ignore", "pipe", "ignore"] }).toString().trim();
    return out || null;
  } catch { return null; }
}
// Git-versie bepalen (CI > describe > rev-parse)
function resolveCodeVersion(fallback = "v1") {
  const ci = pickCiSha();
  if (ci) return ci;
  const described =
    tryExec("git describe --tags --always --dirty") || // tag + commits + sha + '-dirty' indien nodig 
    tryExec("git describe --always --dirty");
  if (described) return described;
  const shortSha = tryExec("git rev-parse --short HEAD"); // huidige commit hash 
  if (shortSha) return shortSha;
  return fallback;
}

/* ----------------------------
   Diff-config
-----------------------------*/
const RUNS_SHEET = "RUNS";
const LOG_SHEET = "LOG";
const CHANGES_SHEET = "CHANGES";
const MASTER_SHEET = "MASTER";

// Hash velden
const HASH_FIELDS = [
  "match_id", "home_name", "away_name", "grade_id", "score_text", "round",
  "date1", "leader_text", "venue_name", "status_id",
];

// Alias-kandidaten (fallbacks)
const FIELD_ALIASES = {
  match_id:   ["match_id","MatchId","matchId","id","Id","ID","match.id"],
  home_name:  ["homeTeam.name","HomeTeam.name","home_name","home","homeTeamName"],
  away_name:  ["awayTeam.name","AwayTeam.name","away_name","away","awayTeamName"],
  grade_id:   ["gradeId","grade_id","grade.id","_grade"],
  score_text: ["scoreText","score_text","score.fullTime","result_text"],
  round:      ["round","Round","round_name","roundName"],
  date1:      ["date1","matchDate","MatchDate","datetime","start_utc","startUtc"],
  leader_text:["leaderText","leader_text","leaders_text","leadersText"],
  venue_name: ["venue.name","Venue.name","venue_name","ground","Ground","squadName"],
  status_id:  ["status_id","statusId","match_status_id","matchStatus","match_status"]
};

/* ----------------------------
   Telegram
-----------------------------*/
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

/* ----------------------------
   Google Sheets helpers
-----------------------------*/
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
  const directAuth = new GoogleAuth({ keyFile: process.env.GOOGLE_APPLICATION_CREDENTIALS, scopes: [SHEETS_SCOPE] });
  return google.sheets({ version: "v4", auth: directAuth });
}

// Case-insensitieve ensureSheet: retourneert feitelijke titel
async function ensureSheet(sheets, desiredTitle) {
  const resp = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const existing = (resp.data.sheets || []).map(s => s.properties.title);
  const wantedLower = desiredTitle.toLowerCase();
  const hit = existing.find(t => t.toLowerCase() === wantedLower);
  if (hit) {
    if (hit !== desiredTitle) console.log(`[Sheets] Re-using existing tab "${hit}" (requested "${desiredTitle}")`);
    else console.log(`[Sheets] Tab "${desiredTitle}" bestaat al`);
    return hit;
  }
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests: [{ addSheet: { properties: { title: desiredTitle } } }] },
  });
  console.log(`➕ Sheet tab "${desiredTitle}" gemaakt`);
  return desiredTitle;
}

// Infra-tabs aanmaken/oppakken en feitelijke namen bewaren
async function ensureInfraTabs(sheets) {
  console.log("[Sheets] ensureInfraTabs → RUNS/LOG/CHANGES");
  globalThis.__RUNS_NAME__    = await ensureSheet(sheets, RUNS_SHEET);
  globalThis.__LOG_NAME__     = await ensureSheet(sheets, LOG_SHEET);
  globalThis.__CHANGES_NAME__ = await ensureSheet(sheets, CHANGES_SHEET);
  console.log(`[Sheets] infra OK → RUNS="${globalThis.__RUNS_NAME__}", LOG="${globalThis.__LOG_NAME__}", CHANGES="${globalThis.__CHANGES_NAME__}"`);
}
async function ensureInfra(sheets) {
  try { globalThis.__RUNS_NAME__    = await ensureSheet(sheets, RUNS_SHEET); }    catch(e){ console.warn(`[Sheets] RUNS ensure failed: ${e.message}`); }
  try { globalThis.__LOG_NAME__     = await ensureSheet(sheets, LOG_SHEET); }     catch(e){ console.warn(`[Sheets] LOG ensure failed: ${e.message}`); }
  try { globalThis.__CHANGES_NAME__ = await ensureSheet(sheets, CHANGES_SHEET); } catch(e){ console.warn(`[Sheets] CHANGES ensure failed: ${e.message}`); }
}

function assertSheetsReady(sheets) {
  if (!sheets) {
    throw new Error("Sheets client is niet geïnitialiseerd (sheets == null). Controleer SPREADSHEET_ID, credentials en scopes.");
  }
}

async function appendRows(sheets, sheetName, values) {
  if (!values?.length) return;
  console.log(`[Sheets] append rows → ${sheetName} (#${values.length})`);
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName, // append aan einde van ‘table’ in deze sheet 
    valueInputOption: "RAW",
    requestBody: { values },
  });
}

async function clearAndWriteObjects(sheets, sheetName, rows) {
  if (!rows?.length) {
    console.log(`[Sheets] clearAndWriteObjects overgeslagen: 0 rows voor ${sheetName}`);
    return;
  }
  const headers = uniqueFields(rows);
  console.log(`[Sheets] clearAndWrite → ${sheetName} (headers=${headers.length}, rows=${rows.length})`);
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:ZZ`,
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!A1`, valueInputOption: "RAW",
    requestBody: { values: [headers] },
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!A2`, valueInputOption: "RAW",
    requestBody: { values: rows.map(r => headers.map(h => r[h] ?? "")) },
  });
}

async function readSheetAsObjects(sheets, sheetName) {
  try {
    const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: sheetName });
    const values = resp.data.values || [];
    if (!values.length) return [];
    const headers = values[0];
    return values.slice(1).map(row => {
      const o = {};
      headers.forEach((h, i) => { o[h] = row[i] ?? ""; });
      return o;
    });
  } catch {
    return [];
  }
}

/* ----------------------------
   Utils
-----------------------------*/
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
  const candidates = [MATCH_REFERRER_URL, GRADES_REFERRER_URL, SEASONS_REFERRER_URL, "https://matchcentre.kncb.nl/matches/"].filter(Boolean);
  for (const c of candidates) {
    try {
      const u = new URL(c);
      if (/matchcentre\.kncb\.nl$/i.test(u.hostname)) return u.toString();
    } catch {}
  }
  return "https://matchcentre.kncb.nl/matches/";
}
function normalizeRvEndpoint(name, value) {
  if (!value) throw new Error(`${name} ontbreekt`);
  const u = new URL(value);
  if (!/api\.resultsvault\.co\.uk$/i.test(u.hostname)) throw new Error(`${name} moet naar api.resultsvault.co.uk wijzen (nu: ${u.hostname})`);
  if (u.searchParams.has("seasonId") && !u.searchParams.has("seasonid")) {
    const v = u.searchParams.get("seasonId");
    u.searchParams.delete("seasonId");
    if (v) u.searchParams.set("seasonid", v);
  }
  if (SEASON_ID) u.searchParams.set("seasonid", String(SEASON_ID));
  return u;
}
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
  const cands = [g?.gradeId, g?.gradeID, g?.gradeid, g?.grade_id, g?.id, g?.Id, g?.ID, g?.grade?.id, g?.grade?.gradeId, g?.GradeId];
  for (const c of cands) if (c != null && String(c).trim() !== "") return String(c).trim();
  return "";
}
function getSeasonIdFromGrade(g) {
  const cands = [g?.seasonid, g?.seasonId, g?.season?.id];
  for (const c of cands) if (c != null && String(c).trim() !== "") return String(c).trim();
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

/* ----------------------------
   Deep getters voor hash/keys
-----------------------------*/
function getByPath(obj, path) {
  try {
    return path.split('.').reduce((acc, key) => {
      if (acc == null) return undefined;
      const m = key.match(/(.+)\[(\d+)\]$/);
      if (m) {
        const k = m[1];
        const i = Number(m[2]);
        const v = acc[k];
        return Array.isArray(v) ? v[i] : undefined;
      }
      return acc[key];
    }, obj);
  } catch { return undefined; }
}
function firstNonEmpty(obj, paths) {
  for (const p of paths) {
    const v = getByPath(obj, p);
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return "";
}
function extractHashFieldsFromObj(raw, gradeId) {
  return {
    match_id:   firstNonEmpty(raw, FIELD_ALIASES.match_id),
    home_name:  firstNonEmpty(raw, FIELD_ALIASES.home_name),
    away_name:  firstNonEmpty(raw, FIELD_ALIASES.away_name),
    grade_id:   gradeId || firstNonEmpty(raw, FIELD_ALIASES.grade_id),
    score_text: firstNonEmpty(raw, FIELD_ALIASES.score_text),
    round:      firstNonEmpty(raw, FIELD_ALIASES.round),
    date1:      firstNonEmpty(raw, FIELD_ALIASES.date1),
    leader_text:firstNonEmpty(raw, FIELD_ALIASES.leader_text),
    venue_name: firstNonEmpty(raw, FIELD_ALIASES.venue_name),
    status_id:  firstNonEmpty(raw, FIELD_ALIASES.status_id)
  };
}

/* ----------------------------
   Warm-up per grade
-----------------------------*/
async function warmupSession(page, { entityId, gradeId, seasonId }) {
  const warmUrl = `https://matchcentre.kncb.nl/matches/?entity=${entityId}&grade=${gradeId}&season=${seasonId}`;
  try {
    const warm = await page.browser().newPage();
    await warm.setViewport({ width: 160, height: 100 });
    await warm.goto(warmUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
    await delay(1000);
    await warm.close();
    if (VERBOSE >= 1) console.log(`🔥 warmup ok for grade ${gradeId}`);
  } catch (e) {
    console.warn(`🔥 warmup failed for grade ${gradeId}: ${e.message}`);
  }
}

/* ----------------------------
   In-page fetch met IAS header + referrer
-----------------------------*/
async function fetchJsonInPage(page, url, refPageUrl, label) {
  return await page.evaluate(async (u, ref, ias) => {
    try {
      const res = await fetch(u, {
        credentials: "include",
        referrer: ref,
        referrerPolicy: "strict-origin-when-cross-origin",
        headers: {
          "Accept": "application/json, text/plain;q=0.8, */*;q=0.5",
          "X-IAS-API-REQUEST": ias,
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

/* ----------------------------
   RUN/LOG/DIFF helpers
-----------------------------*/
const nowIso = () => new Date().toISOString();
const newRunId = () => nowIso();
const sha1 = (str) => crypto.createHash("sha1").update(str).digest("hex");

function getFieldValue(row, aliases) {
  for (const k of aliases) {
    if (k in row) return String(row[k] ?? "");
  }
  return "";
}
function normalizeForHash(row) {
  const norm = {};
  for (const key of HASH_FIELDS) {
    const aliases = FIELD_ALIASES[key] || [key];
    norm[key] = getFieldValue(row, aliases);
  }
  return norm;
}
function calcRowHash(row) {
  const norm = normalizeForHash(row);
  const sorted = Object.keys(norm).sort().reduce((o,k)=>{o[k]=norm[k];return o;}, {});
  return sha1(JSON.stringify(sorted));
}
function getMatchIdFromRow(row) {
  return getFieldValue(row, FIELD_ALIASES.match_id || ["match_id","id"]);
}
function getGradeIdFromRow(row) {
  return getFieldValue(row, FIELD_ALIASES.grade_id || ["grade_id","_grade"]);
}

async function logStart(sheets, runId, version, note = "") {
  await ensureInfra(sheets);
  const start = nowIso();
  await appendRows(sheets, globalThis.__RUNS_NAME__, [[runId, start, "", SCRIPT_NAME, version, "", "", "", note]]);
  await appendRows(sheets, globalThis.__LOG_NAME__,  [[runId, start, SCRIPT_NAME, "main", "start", "-", "INFO", `init season=${SEASON_ID}`, note]]);
}
async function logError(sheets, runId, func, table, message, detail = "") {
  await ensureInfra(sheets);
  await appendRows(sheets, globalThis.__LOG_NAME__, [[runId, nowIso(), SCRIPT_NAME, func, "error", table, "ERROR", message, (detail||"").slice(0,200)]]);
}
async function logSummary(sheets, runId, gradeCount, matchCount, errors) {
  await ensureInfra(sheets);
  const ts = nowIso();
  await appendRows(sheets, globalThis.__LOG_NAME__,  [[runId, ts, SCRIPT_NAME, "main", "summary", "MASTER", "INFO",
    `version=${VERSION} grades=${gradeCount} matches=${matchCount} errors=${errors}`, "" ]]);
  await appendRows(sheets, globalThis.__RUNS_NAME__, [[runId, "", ts, SCRIPT_NAME, VERSION, gradeCount, matchCount, errors, "ok"]]);
}

/* ----------------------------
   MAIN
-----------------------------*/
(async () => {
  let browser;
  let sheets = null;
  const runId = newRunId();
  let errorsCount = 0;
  let totalMatches = 0;

  try {
    // Versie bepalen en loggen
    VERSION = resolveCodeVersion("v1.1-delta-tabs");
    console.log(`[Version] running build: ${VERSION}`);

    if (!IAS_API_KEY) throw new Error("IAS_API_KEY ontbreekt in .env (X-IAS-API-REQUEST verplicht).");

    const GRADES_URL = normalizeRvEndpoint("GRADES_JSON_API_ENDPOINT", GRADES_JSON_API_ENDPOINT);
    const MATCH_URL = normalizeRvEndpoint("MATCH_JSON_API_ENDPOINT", MATCH_JSON_API_ENDPOINT);
    const SEASONS_URL = SEASONS_JSON_API_ENDPOINT ? normalizeRvEndpoint("SEASONS_JSON_API_ENDPOINT", SEASONS_JSON_API_ENDPOINT) : null;

    const refPage = pickReferrer();

    // Sheets init
    if (!DISABLE_SHEETS && SPREADSHEET_ID) {
      try {
        const impersonationNote = process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT ? "(impersonation)" : "";
        console.log("🔐 Init Google Sheets via ADC", impersonationNote);
        sheets = await getSheetsClient();
        await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
        console.log("✅ Google Sheets auth OK");
        await ensureInfraTabs(sheets);
        await logStart(sheets, runId, VERSION);
      } catch (e) {
        console.warn("⚠️ Sheets init waarschuwing:", e.message);
      }
    } else {
      console.log("ℹ️ Geen SPREADSHEET_ID of Sheets disabled; logging naar Sheets uit.");
    }

    if (!DISABLE_SHEETS) {
      if (!SPREADSHEET_ID) throw new Error("SPREADSHEET_ID ontbreekt; kan niet naar Google Sheets schrijven.");
      if (!sheets)      throw new Error("Sheets client niet beschikbaar na init; kan niet naar Google Sheets schrijven.");
      await ensureInfra(sheets);
    }

    // Puppeteer
    console.log("🚀 Start puppeteer…");
    browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox", "--disable-web-security", "--disable-site-isolation-trials"] });
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

    // Referrer + warm-up (root)
    console.log("📥 Open referrer pagina…", refPage);
    await page.goto(refPage, { waitUntil: "networkidle0" });
    if (process.env.API_URL) {
      console.log("📥 Warm-up API call…", process.env.API_URL);
      await fetchJsonInPage(page, process.env.API_URL, refPage, "warmup");
    }
    await delay(SLOWDOWN_MS);

    // 1) GRADES ophalen
    console.log("📥 Haal grades op…");
    const gradesJson = await fetchJsonInPage(page, GRADES_URL.toString(), refPage, "grades");
    if (gradesJson?.__error) throw new Error(`Grades endpoint error: ${gradesJson.__error}`);

    const gradesArrRaw = extractArray(gradesJson) || [];
    console.log(`▶ Gevonden ${gradesArrRaw.length} raw grades`);

    // GRADES → CSV + Sheet
    const gradesRows = gradesArrRaw.map(flattenObject);
    if (gradesRows.length) {
      writeCsv("GRADES.csv", gradesRows);
      if (sheets) {
        const gradesTab = await ensureSheet(sheets, "GRADES");
        await clearAndWriteObjects(sheets, gradesTab, gradesRows);
        console.log(`📈 Sheet "${gradesTab}" geüpdatet (${gradesRows.length} rijen)`);
      }
    }

    // Filter
    const onlyIds = (GRADE_IDS || "").split(",").map(s => s.trim()).filter(Boolean).map(String);
    if (VERBOSE >= 1) console.log("🔎 Filter GRADE_IDS =", onlyIds);

    const gradesArr = gradesArrRaw.filter((g) => {
      const gid = getGradeId(g);
      if (!gid) { if (VERBOSE >= 1) console.log("⚠️ Grade zonder id, skip:", g); return false; }
      return !onlyIds.length || onlyIds.includes(gid);
    });
    console.log(`🧮 Te verwerken grades: ${gradesArr.length}${onlyIds.length ? ` (gefilterd uit ${gradesArrRaw.length})` : ""}`);

    const masterRowsRaw = [];
    let processed = 0;

    // 2) Per grade wedstrijden
for (let i = 1; i < gradesArr.length; i++) {
  const g = gradesArr[i];
//  console.log(`Processing grade ${i + 1}/${gradesArr.length}`);
  // rest van je bestaande code voor grade g ...

      const gid = getGradeId(g);
      const seasonInGrade = getSeasonIdFromGrade(g) || SEASON_ID || "";

      console.log(`\n${i}/${gradesArr.length}: --- 🔁 Grade ${gid} (season ${seasonInGrade || SEASON_ID || "n/a"}) ---`);
      let sheetName = `Grade_${gid}`;
      if (sheets) sheetName = await ensureSheet(sheets, sheetName);

      // Warm-up per grade
      const entityId = String(process.env.RV_ID || "134453");
      await warmupSession(page, { entityId, gradeId: gid, seasonId: seasonInGrade || SEASON_ID });

      // Matches URL + fetch
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
        const msg = `Grade ${gid}: ${matchesJson.__error}${matchesJson.__head ? ` | head=${matchesJson.__head}` : ""}`;
        console.error("⚠️", msg);
        errorsCount++;
        if (sheets) await logError(sheets, runId, "fetchMatches", sheetName, msg, matchesJson.__head);
        await notifyTelegram(`⚠️ ${msg}`);
        continue;
      }

      const dataArr = extractArray(matchesJson);
      if (!Array.isArray(dataArr)) {
        const msg = `Grade ${gid}: geen array in response`;
        console.warn("⚠️", msg);
        errorsCount++;
        if (sheets) await logError(sheets, runId, "fetchMatches", sheetName, msg);
        continue;
      }

      console.log(`✅ ${sheetName}: ${dataArr.length} matches`);
      totalMatches += dataArr.length;

      const now = nowIso();
      const rows = dataArr.map((obj) => {
        // 1) haal betekenisvolle velden uit het ruwe object (deep)
        const keyFields = extractHashFieldsFromObj(obj, gid);

        // 2) flatten de rest
        const flat = flattenObject(obj);

        // 3) forceer hash/ID velden in de rij
        for (const [k, v] of Object.entries(keyFields)) flat[k] = v ?? "";

        // 4) metadata
        flat._grade = gid;
        flat._season = seasonInGrade || SEASON_ID || "";
        flat._run_id = runId;
        flat._fetched_at = now;

        return flat;
      });

      if (rows.length) {
        writeCsv(`${sheetName}.csv`, rows);
        if (sheets) {
          await clearAndWriteObjects(sheets, sheetName, rows);
          console.log(`📈 Sheet "${sheetName}" geüpdatet`);
        }
        masterRowsRaw.push(...rows);
      }

      processed++;
      if (REFRESH_REFERRER_EVERY > 0 && processed % REFRESH_REFERRER_EVERY === 0) {
        console.log("🔄 Sessieverversing: referrer herladen…", refPage);
        try { await page.goto(refPage, { waitUntil: "networkidle0" }); } catch {}
        await delay(SLOWDOWN_MS);
      }
      await delay(SLOWDOWN_MS);
    }
   

    /* 3) MASTER + CHANGES (diff t.o.v. vorige run) */
    if (masterRowsRaw.length) {
      console.log(`\n📊 Diff & MASTER opbouwen (input: ${masterRowsRaw.length} rijen)…`);

      // Vorige MASTER
      const masterTab = sheets ? await ensureSheet(sheets, MASTER_SHEET) : MASTER_SHEET;
      const prevMaster = sheets ? await readSheetAsObjects(sheets, masterTab) : [];
      const prevById = new Map();
      for (const r of prevMaster) {
        const id = getMatchIdFromRow(r);
        if (id) prevById.set(id, r);
      }

      console.log(`[Diff] Prev MASTER rows: ${prevMaster.length}`);
      console.log(`[Diff] New master candidates: ${masterRowsRaw.length}`);

      const now = nowIso();
      const masterById = new Map();
      const changeRows = [];

      for (const r of masterRowsRaw) {
        const id = getMatchIdFromRow(r);
        if (!id) continue;
        const newHash = calcRowHash(r);
        const prev = prevById.get(id);
        if (!prev) {
          changeRows.push([runId, now, id, "created", "*", "", newHash, getGradeIdFromRow(r)]);
          masterById.set(id, { ...r, _hash: newHash, _last_changed_at: now, _last_change_type: "created", _active: 1 });
        } else {
          const prevHash = String(prev._hash || "");
          if (prevHash !== newHash) {
            changeRows.push([runId, now, id, "updated", "*", prevHash, newHash, getGradeIdFromRow(r)]);
            masterById.set(id, { ...r, _hash: newHash, _last_changed_at: now, _last_change_type: "updated", _active: 1 });
          } else {
            masterById.set(id, { ...r, _hash: prevHash, _last_changed_at: prev._last_changed_at || "", _last_change_type: prev._last_change_type || "unchanged", _active: 1 });
          }
        }
      }

      // Deletions → behouden met _active=0
      for (const [id, prev] of prevById.entries()) {
        if (!masterById.has(id)) {
          const nowDel = nowIso();
          changeRows.push([runId, nowDel, id, "deleted", "*", String(prev._hash || ""), "", prev.grade_id || prev._grade || ""]);
          masterById.set(id, { ...prev, _run_id: runId, _fetched_at: nowDel, _last_changed_at: nowDel, _last_change_type: "deleted", _active: 0 });
        }
      }

      // CHANGES
      if (sheets && changeRows.length) {
        const changesTab = await ensureSheet(sheets, CHANGES_SHEET);
        console.log(`[Sheets] CHANGES to append: ${changeRows.length}`);
        await appendRows(sheets, changesTab, changeRows);
        console.log(`📝 CHANGES toegevoegd: ${changeRows.length} regels`);
      }

      // MASTER
      const masterOut = Array.from(masterById.values());
      console.log(`[Sheets] MASTER to write: ${masterOut.length}`);
      writeCsv("MASTER.csv", masterOut);
      if (sheets) {
        const masterResolved = await ensureSheet(sheets, MASTER_SHEET);
        await clearAndWriteObjects(sheets, masterResolved, masterOut);
        console.log("📈 MASTER sheet bijgewerkt");
      }
    }

    // Summary logging
    if (sheets) await logSummary(sheets, runId, gradesArr.length, totalMatches, errorsCount);

    console.log("\n✅ Klaar!");
  } catch (err) {
    console.error("🛑 FATAAL:", err.message);
    await notifyTelegram(`Fatal error: ${err.message}`);
    try {
      if (SPREADSHEET_ID) {
        const sheetsTmp = await getSheetsClient().catch(()=>null);
        if (sheetsTmp) {
          await ensureInfra(sheetsTmp);
          await appendRows(sheetsTmp, globalThis.__LOG_NAME__ || LOG_SHEET, [[newRunId(), nowIso(), SCRIPT_NAME, "main", "fatal", "-", "ERROR", err.message, ""]]);
        }
      }
    } catch {}
  } finally {
    try {
      // extra zichtbaarheid van “einde” (niet destructief)
      // NB: versie staat al in RUNS bij start & summary
      // hier alleen nog een laatste logSummary fallback
      // (heeft geen effect als sheets niet bestaat).
      // Je kunt dit ook verwijderen als je het dubbel vindt.
      // await logSummary(sheets, newRunId(), 0, 0, 0);
    } catch {}
    if (typeof browser !== "undefined" && browser) {
      await browser.close();
      console.log("✅ Browser gesloten");
    }
  }
})();
