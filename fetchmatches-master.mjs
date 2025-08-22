// fetchmatches-master.mjs
// Gebruikt jouw .env-namen (…_REFERRER_URL en …_JSON_API_ENDPOINT)
// Node 18+, Puppeteer nodig. Install: npm i puppeteer json2csv googleapis axios dotenv

import puppeteer from "puppeteer";
import fs from "fs";
import { parse } from "json2csv";
import { google } from "googleapis";
import { GoogleAuth } from "google-auth-library";
import dotenv from "dotenv";
import axios from "axios";
import { setTimeout as delay } from "node:timers/promises";
import { Impersonated } from 'google-auth-library';

dotenv.config();

/* ========= .env =========
SPREADSHEET_ID=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
SEASON_ID=19
RV_ID=134453
MATCH_REFERRER_URL=https://matchcentre.kncb.nl/matches/
GRADES_REFERRER_URL=https://matchcentre.kncb.nl/matches/
SEASONS_REFERRER_URL=https://matchcentre.kncb.nl/seasons/
API_URL=https://api.resultsvault.co.uk/rv/134453/?apiid=1002
MATCH_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/matches/?apiid=1002&action=ors&maxrecs=1000&strmflg=1
GRADES_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/grades/?apiid=1002&seasonid=19
MATCHSTATUS_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/lookup/?apiid=1002&sportid=1&lookupid=MATCH_BREAK_TYPES
SEASONS_JSON_API_ENDPOINT=https://api.resultsvault.co.uk/rv/134453/seasons/?apiid=1002
CSV_OUTPUT=matches.csv
SLOWDOWN_MS=2000
PUPPETEER_TIMEOUT_MS=30000
GRADE_IDS=73942,73943    # optioneel
========================== */

const {
  // referrers (let op typo 'REFFERRER' → we ondersteunen beide keys)
  MATCH_REFERRER_URL,
  GRADES_REFERRER_URL,
  SEASONS_REFERRER_URL,

  // JSON endpoints
  MATCH_JSON_API_ENDPOINT,
  GRADES_JSON_API_ENDPOINT,
  SEASONS_JSON_API_ENDPOINT,
  MATCHSTATUS_JSON_API_ENDPOINT,
  API_URL,

  // overige
  SEASON_ID,
  RV_ID,
  SPREADSHEET_ID,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  CSV_OUTPUT,
  GRADE_IDS,
} = process.env;

const SLOWDOWN_MS = Number(process.env.SLOWDOWN_MS ?? 1200);
const TIMEOUT_MS = Number(process.env.PUPPETEER_TIMEOUT_MS ?? 30000);
const VERBOSE = Number(process.env.VERBOSE ?? 0);
const DISABLE_SHEETS =  String(process.env.DISABLE_SHEETS ?? "").toLowerCase() === "1";
const RETRY_MAX = Number(process.env.RETRY_MAX ?? 4);
const RETRY_BASE_DELAY_MS = Number(process.env.RETRY_BASE_DELAY_MS ?? 1200);
const RETRY_JITTER_MS = Number(process.env.RETRY_JITTER_MS ?? 400);
const REFRESH_REFERRER_EVERY = Number(process.env.REFRESH_REFERRER_EVERY ?? 5);
const USE_NODE_FETCH_ON_401 =  String(process.env.USE_NODE_FETCH_ON_401 ?? "1") === "1";
const SHEETS_SCOPE = 'https://www.googleapis.com/auth/spreadsheets';

const jitter = (ms) => ms + Math.floor(Math.random() * RETRY_JITTER_MS);
const backoffMs = (attempt) =>
  jitter(RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1));

// ---------- Telegram (zonder parse_mode) ----------
async function notifyTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        chat_id: TELEGRAM_CHAT_ID,
        text,
        disable_web_page_preview: true,
      }
    );
  } catch (e) {
    console.error("❌ Telegram error:", e.response?.data || e.message || e);
  }
}

// ---------- Google Sheets ----------
// Deze sectie werd vervangen om gebruik te gaan maken van de Google Workflow identificatie

// ---------------- Google Sheets client (ADC + optional impersonation) ----------------

async function getSheetsClient() {
  const impersonate = process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT;

  if (impersonate && impersonate.trim()) {
    // 1) Source creds (moeten cloud-platform scope hebben om IAMCredentials te mogen aanroepen)
    const sourceAuth = new GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/cloud-platform']
    });
    const sourceClient = await sourceAuth.getClient();

    // 2) Impersonated token met expliciete targetScopes voor Sheets
    const impersonated = new Impersonated({
      sourceClient,
      targetPrincipal: impersonate,     // e.g. target-sa@project.iam.gserviceaccount.com
      delegates: [],                    // of een keten van SAs indien nodig
      lifetime: 3600,
      targetScopes: [SHEETS_SCOPE]      // eventueel ook DRIVE_SCOPE erbij
    });

    return google.sheets({ version: 'v4', auth: impersonated });
  }

 // Zonder impersonation: directe SA key (of ADC) met scopes
  const directAuth = new GoogleAuth({
    keyFile: process.env.GOOGLE_APPLICATION_CREDENTIALS, // of laat weg voor pure ADC
    scopes: [SHEETS_SCOPE]
  });
  return google.sheets({ version: 'v4', auth: directAuth });
}

console.log(process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT ? '🔐 Using impersonation' : '🔐 Using direct SA/ADC');


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
// Normaliseer een ResultsVault JSON endpoint:
// - moét api.resultsvault.co.uk zijn
// - forceer 'seasonid' (lowercase); vervang eventuele seasonId
function normalizeRvEndpoint(name, value) {
  if (!value) throw new Error(`${name} ontbreekt`);
  const u = new URL(value);
  if (!/api\.resultsvault\.co\.uk$/i.test(u.hostname)) {
    throw new Error(
      `${name} moet naar api.resultsvault.co.uk wijzen (nu: ${u.hostname})`
    );
  }
  if (u.searchParams.has("seasonId") && !u.searchParams.has("seasonid")) {
    const v = u.searchParams.get("seasonId");
    u.searchParams.delete("seasonId");
    if (v) u.searchParams.set("seasonid", v);
  }
  // Als SEASON_ID gezet is, overschrijf/zet seasonid daarop
  if (SEASON_ID) u.searchParams.set("seasonid", String(SEASON_ID));
  return u;
}
// Bouw per-grade match-URL op basis van MATCH_JSON_API_ENDPOINT
function buildMatchUrl(baseUrl, { gradeId, seasonId }) {
  const u = new URL(baseUrl.toString());
  // haal bestaande gradeid/seasonid weg, zet nieuwe
  u.searchParams.delete("gradeid");
  u.searchParams.delete("seasonId");
  u.searchParams.delete("seasonid");
  if (seasonId) u.searchParams.set("seasonid", String(seasonId));
  else if (SEASON_ID) u.searchParams.set("seasonid", String(SEASON_ID));
  u.searchParams.set("gradeid", String(gradeId));

  // defaults indien nog niet aanwezig
  if (!u.searchParams.has("action")) u.searchParams.set("action", "ors");
  if (!u.searchParams.has("maxrecs")) u.searchParams.set("maxrecs", "1000");
  if (!u.searchParams.has("strmflg")) u.searchParams.set("strmflg", "1");
  return u.toString();
}

function getGradeId(g) {
  // Probeer meerdere varianten
  const candidates = [
    g?.gradeId,
    g?.gradeID,
    g?.gradeid,
    g?.grade_id,
    g?.id,
    g?.Id,
    g?.ID,
    g?.grade?.id,
    g?.grade?.gradeId,
    g?.GradeId,
  ];
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

// Node fetch fallback – géén cookies nodig; endpoint is veelal publiek.
async function nodeFetchJson(url) {
  try {
    const res = await fetch(url, {
      headers: {
        accept: "application/json, text/plain;q=0.8, */*;q=0.5",
        "user-agent": "Mozilla/5.0 KNCB-Matchcentre", // vriendelijk UA
      },
    });
    const text = await res.text();
    if (!res.ok)
      return { __error: `HTTP ${res.status}`, __head: text.slice(0, 220) };
    // JSON guard
    const h = text.trim().charAt(0);
    if (h !== "{" && h !== "[")
      return { __error: "Non-JSON response", __head: text.slice(0, 220) };
    try {
      return JSON.parse(text);
    } catch (e) {
      return {
        __error: `JSON parse error: ${e.message}`,
        __head: text.slice(0, 220),
      };
    }
  } catch (e) {
    return { __error: `Fetch failed: ${e.message}` };
  }
}

async function pageFetchJsonWithRetry(page, url, { refPageUrl, label }) {
  for (let attempt = 1; attempt <= RETRY_MAX; attempt++) {
    const result = await page.evaluate(async (u) => {
      try {
        const res = await fetch(u, {
          credentials: "include",
          headers: {
            accept: "application/json, text/plain;q=0.8, */*;q=0.5",
            "x-requested-with": "XMLHttpRequest",
          },
          mode: "cors",
        });
        const ct = res.headers.get("content-type") || "";
        const text = await res.text();
        if (!res.ok)
          return {
            __error: `HTTP ${res.status}`,
            __status: res.status,
            __head: text.slice(0, 220),
          };
        if (!/application\/json/i.test(ct)) {
          const h = text.trim().charAt(0);
          if (h !== "{" && h !== "[")
            return { __error: "Non-JSON response", __head: text.slice(0, 220) };
          try {
            return JSON.parse(text);
          } catch (e) {
            return {
              __error: `JSON parse error: ${e.message}`,
              __head: text.slice(0, 220),
            };
          }
        }
        try {
          return JSON.parse(text);
        } catch (e) {
          return {
            __error: `JSON parse error: ${e.message}`,
            __head: text.slice(0, 220),
          };
        }
      } catch (e) {
        return { __error: `Fetch failed: ${e.message}` };
      }
    }, url);

    if (!result?.__error) return result;

    const code = Number(result.__status || 0);
    const transient = [401, 403, 408, 420, 429, 500, 502, 503, 504].includes(
      code
    );
    console.warn(
      `⚠️ [attempt ${attempt}/${RETRY_MAX}] ${label ?? ""} ${url} -> ${
        result.__error
      }${result.__head ? ` | head=${result.__head}` : ""}`
    );

    // Fallback met Node-fetch op 401/403 (soms stricter per browser/cookie)
    if (USE_NODE_FETCH_ON_401 && (code === 401 || code === 403)) {
      const alt = await nodeFetchJson(url);
      if (!alt?.__error) {
        console.log("🔁 Fallback via Node-fetch geslaagd voor", label ?? url);
        return alt;
      }
    }

    if (attempt < RETRY_MAX && transient) {
      // Sessie ververst houden: referrer herladen vóór volgende poging
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

// Following code is not used, as a similar function with retries is used (pageFetchJsonWithRetries)
async function pageFetchJson(page, url) {
  const result = await page.evaluate(async (u) => {
    try {
      const res = await fetch(u, {
        credentials: "include",
        headers: {
          accept: "application/json, text/plain;q=0.8, */*;q=0.5",
          "x-requested-with": "XMLHttpRequest",
        },
        mode: "cors",
      });
      const ct = res.headers.get("content-type") || "";
      const text = await res.text();
      if (!res.ok)
        return { __error: `HTTP ${res.status}`, __head: text.slice(0, 220) };
      if (!/application\/json/i.test(ct)) {
        const h = text.trim().charAt(0);
        if (h !== "{" && h !== "[")
          return { __error: "Non-JSON response", __head: text.slice(0, 220) };
        try {
          return JSON.parse(text);
        } catch (e) {
          return {
            __error: `JSON parse error: ${e.message}`,
            __head: text.slice(0, 220),
          };
        }
      }
      try {
        return JSON.parse(text);
      } catch (e) {
        return {
          __error: `JSON parse error: ${e.message}`,
          __head: text.slice(0, 220),
        };
      }
    } catch (e) {
      return { __error: `Fetch failed: ${e.message}` };
    }
  }, url);

  if (result?.__error) {
    console.error(
      `⚠️ pageFetchJson(${url}) -> ${result.__error}`,
      result.__head ? `\nHEAD: ${result.__head}` : ""
    );
  }
  return result;
}

(async () => {
  let browser;
  try {
    // normaliseer endpoints op basis van jouw env-namen
    const GRADES_URL = normalizeRvEndpoint(
      "GRADES_JSON_API_ENDPOINT",
      GRADES_JSON_API_ENDPOINT
    );
    const MATCH_URL = normalizeRvEndpoint(
      "MATCH_JSON_API_ENDPOINT",
      MATCH_JSON_API_ENDPOINT
    );
    // (optioneel) andere endpoints
    const SEASONS_URL = SEASONS_JSON_API_ENDPOINT
      ? normalizeRvEndpoint(
          "SEASONS_JSON_API_ENDPOINT",
          SEASONS_JSON_API_ENDPOINT
        )
      : null;

    const refPage = pickReferrer();

    /*
    const sheets = (SPREADSHEET_ID && GOOGLE_APPLICATION_CREDENTIALS)
      ? await getSheetsClient()
      : null;
*/ // Vervangen door volgende sectie:
    let sheets = null;
    if (process.env.DISABLE_SHEETS === "1") {
      console.log(
        "ℹ️ Sheets uitgeschakeld (DISABLE_SHEETS=1). CSV’s worden wel geschreven."
      );
    } else if (process.env.SPREADSHEET_ID) {
      try {
        console.log(
          "🔐 Init Google Sheets via ADC",
          process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT
            ? "(impersonation)"
            : ""
        );
        sheets = await getSheetsClient();
        await sheets.spreadsheets.get({ spreadsheetId: process.env.SPREADSHEET_ID });
        console.log('✅ Sheets access ok (scopes & permissions)');
      } catch (e) {
        console.warn(
          "⚠️ Sheets init mislukt; ga verder zonder Sheets:",
          e.message
        );
        sheets = null;
      }
    } else {
      console.log("ℹ️ Geen SPREADSHEET_ID gezet; ga verder zonder Sheets.");
    }

    console.log("🚀 Start puppeteer…");
    browser = await puppeteer.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-web-security",
        "--disable-site-isolation-trials",
      ],
    });
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(TIMEOUT_MS);
    page.on("console", (msg) => console.log("PAGE LOG →", msg.text()));
    page.on("requestfailed", (req) =>
      console.log("PAGE REQ FAIL →", req.failure()?.errorText, req.url())
    );

    // Referrer openen
    console.log("📥 Open referrer pagina…", refPage);
    await page.goto(refPage, { waitUntil: "networkidle0" });
    await delay(SLOWDOWN_MS);

    // Grades ophalen
    console.log("📥 Haal grades op…");
    // const gradesJson = await pageFetchJson(page, GRADES_URL.toString());
    // replace with retrying code
    const gradesJson = await pageFetchJsonWithRetry(
      page,
      GRADES_URL.toString(),
      {
        refPageUrl: refPage,
        label: "grades",
      }
    );

    if (gradesJson?.__error)
      throw new Error(`Grades endpoint error: ${gradesJson.__error}`);
    const gradesArrRaw = extractArray(gradesJson) || [];
    console.log(`▶ Gevonden ${gradesArrRaw.length} raw grades`);

    if (VERBOSE >= 1) {
      console.log("🧪 Voorbeeld grade[0..2]:");
      gradesArrRaw.slice(0, 3).forEach((g, i) => {
        console.log(`grade[${i}] keys=`, Object.keys(g));
        console.log(
          `grade[${i}] sample=`,
          JSON.stringify(g, null, 2).slice(0, 800)
        );
      });
      try {
        fs.writeFileSync(
          "grades_raw.debug.json",
          JSON.stringify(gradesArrRaw, null, 2)
        );
        console.log("💾 grades_raw.debug.json geschreven");
      } catch (e) {
        console.warn("⚠️ Kon grades_raw.debug.json niet schrijven:", e.message);
      }
    }

    // Filter op GRADE_IDS (optioneel)
    /*. Section replaced 
    const onlyIds = (GRADE_IDS || '').split(',').map(s => s.trim()).filter(Boolean);
    const gradesArr = gradesArrRaw.filter(g => {
      const gid = String(g?.id ?? g?.gradeId ?? '').trim();
      return gid && (!onlyIds.length || onlyIds.includes(gid));
    });
*/

    const onlyIdsRaw = (GRADE_IDS || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const onlyIds = onlyIdsRaw.map((x) => String(x));

    if (VERBOSE >= 1) {
      console.log("🔎 Filter GRADE_IDS =", onlyIds);
    }

    const gradesArr = gradesArrRaw.filter((g) => {
      const gid = getGradeId(g);
      if (!gid) {
        if (VERBOSE >= 1)
          console.log("⚠️ Grade zonder herkenbare id, skip:", g);
        return false; // of true, als je álle onbekende toch wilt verwerken
      }
      if (!onlyIds.length) return true;
      const keep = onlyIds.includes(gid);
      if (!keep && VERBOSE >= 1) {
        console.log(
          `🚫 Gefilterd weg: gradeId=${gid} (niet in ${onlyIds.join(",")})`
        );
      }
      return keep;
    });

    console.log(
      `🧮 Te verwerken grades: ${gradesArr.length}${
        onlyIds.length ? ` (gefilterd uit ${gradesArrRaw.length})` : ""
      }`
    );

    if (VERBOSE >= 1 && gradesArr.length) {
      const ids = gradesArr.map(getGradeId);
      console.log(
        "✅ IDs die we gaan verwerken:",
        ids.slice(0, 50).join(", "),
        ids.length > 50 ? "…" : ""
      );
    }

    console.log(
      `🧮 Te verwerken grades: ${gradesArr.length}${
        onlyIds.length ? ` (gefilterd uit ${gradesArrRaw.length})` : ""
      }`
    );

    const masterRows = [];

    let processed = 0;
    for (const g of gradesArr) {
      // const gid = String(g?.id ?? g?.gradeId ?? '').trim();      Replaced by next line
      const gid = getGradeId(g);

      //  const seasonInGrade = String(g?.seasonId ?? g?.seasonid ?? '').trim(); // indien aanwezig
      const seasonInGrade = getSeasonIdFromGrade(g) || SEASON_ID || "";
      if (VERBOSE >= 1)
        console.log(`ℹ️ grade ${gid} gebruikt seasonId=${seasonInGrade}`);

      if (!gid) {
        console.warn("⚠️ Grade zonder id, sla over");
        continue;
      }

      const sheetName = `Grade_${gid}`;
      console.log(
        `\n--- 🔁 Grade ${gid} (season ${
          seasonInGrade || SEASON_ID || "n/a"
        }) ---`
      );
      if (sheets) await ensureSheet(sheets, sheetName);

      const matchUrl = buildMatchUrl(MATCH_URL, {
        gradeId: gid,
        seasonId: seasonInGrade || SEASON_ID,
      });
      console.log("🌐 Fetch matches:", matchUrl);

      // Nieuwe functieaanroep
      const matchesJson = await pageFetchJsonWithRetry(page, matchUrl, {
        refPageUrl: refPage, // je eerder geopende MatchCentre-pagina
        label: `grade ${gid}`,
      });
      if (matchesJson?.__error) {
        const msg =
          `⚠️ Fout bij grade ${gid}: ${matchesJson.__error}` +
          (matchesJson.__head ? ` | head=${matchesJson.__head}` : "");
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
      if (dataArr.length) {
        console.log(`🧪 Eerste item:\n${JSON.stringify(dataArr[0], null, 2)}`);
      }

      const rows = dataArr.map((obj) => {
        const flat = flattenObject(obj);
        flat._grade = gid;
        flat._season = seasonInGrade || SEASON_ID || "";
        return flat;
      });

      // CSV per grade (en overall CSV_OUTPUT indien gevraagd)
      if (rows.length) {
        try {
          const fields = uniqueFields(rows);
          const csv = parse(rows, { fields, defaultValue: "" });
          fs.writeFileSync(`${sheetName}.csv`, csv, "utf8");
          console.log(`💾 ${sheetName}.csv geschreven (${rows.length} rijen)`);
          if (CSV_OUTPUT && gradesArr.length === 1) {
            fs.writeFileSync(CSV_OUTPUT, csv, "utf8");
            console.log(`💾 ${CSV_OUTPUT} geschreven`);
          }
        } catch (e) {
          console.warn(`⚠️ Kon ${sheetName}.csv niet schrijven:`, e.message);
        }
      }
      if (VERBOSE >= 1) {
        console.log(
          "🔎 Auth route:",
          process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT
            ? "ADC + SA impersonation"
            : "ADC direct"
        );
      }

      if (sheets && rows.length) {
        await writeValues(sheets, sheetName, rows);
        console.log(`📈 Sheet "${sheetName}" geüpdatet`);
      }

      masterRows.push(...rows);
      processed++;
      if (
        REFRESH_REFERRER_EVERY > 0 &&
        processed % REFRESH_REFERRER_EVERY === 0
      ) {
        console.log("🔄 Sessieverversing: referrer herladen…", refPage);
        try {
          await page.goto(refPage, { waitUntil: "networkidle0" });
        } catch {}
        await delay(SLOWDOWN_MS);
      }
      await delay(SLOWDOWN_MS);
    }

    // MASTER output
    if (masterRows.length) {
      console.log(`\n📊 MASTER opbouwen: ${masterRows.length} rijen`);
      try {
        const fields = uniqueFields(masterRows);
        const csv = parse(masterRows, { fields, defaultValue: "" });
        fs.writeFileSync(`MASTER.csv`, csv, "utf8");
        console.log("💾 MASTER.csv geschreven");
        if (CSV_OUTPUT && gradesArr.length > 1) {
          fs.writeFileSync(CSV_OUTPUT, csv, "utf8");
          console.log(`💾 ${CSV_OUTPUT} geschreven`);
        }
      } catch (e) {
        console.warn("⚠️ Kon MASTER.csv niet schrijven:", e.message);
      }
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
