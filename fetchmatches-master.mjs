// fetchmatches-master.mjs
// Merge van fetchmatch-werkt.mjs (sessie + logging) en fetchmatchesbygrade.mjs (per-grade + Sheets/CSV)
// Node 18+ (native fetch), Puppeteer voor sessie/cookies.

import puppeteer from 'puppeteer';
import fs from 'fs';
import { parse } from 'json2csv';
import { google } from 'googleapis';
import dotenv from 'dotenv';
import axios from 'axios';
import { setTimeout as delay } from 'node:timers/promises';

dotenv.config();

// ====== ENV / CONFIG (zelfde namen als in je bestaande scripts) ======
const {
  GRADES_API_URL,        // bv. https://matchcentre.kncb.nl/matches/  (referrer)
  MATCH_API_URL,         // optioneel referrer voor match-endpoint
  GRADES_API_ENDPOINT,   // bv. https://api.resultsvault.co.uk/rv/134453/grades/?apiid=1002&seasonid=19&action=ors
  MATCH_API_ENDPOINT,    // bv. https://api.resultsvault.co.uk/rv/134453/matches/?apiid=1002
  SPREADSHEET_ID,
  GOOGLE_APPLICATION_CREDENTIALS,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  GRADE_IDS,             // optioneel filter: "73942,73943"
} = process.env;

const SLOWDOWN_MS = Number(process.env.SLOWDOWN_MS ?? 1200);
const TIMEOUT_MS  = Number(process.env.PUPPETEER_TIMEOUT_MS ?? 30000);

// ====== Telegram (zonder parse_mode om 400s te vermijden) ======
async function notifyTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text,
      disable_web_page_preview: true
    });
  } catch (e) {
    console.error('❌ Telegram error:', e.response?.data || e.message || e);
  }
}

// ====== Google Sheets helpers ======
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: GOOGLE_APPLICATION_CREDENTIALS,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

async function getSheetTitles(sheets) {
  const resp = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  return (resp.data.sheets || []).map(s => s.properties.title);
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
  const headers = Object.keys(rows[0]);
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:ZZ`,
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: [headers] },
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A2`,
    valueInputOption: 'RAW',
    requestBody: { values: rows.map(r => headers.map(h => r[h])) },
  });
}

// ====== Data helpers ======
function flattenObject(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = (v !== null && typeof v === 'object') ? JSON.stringify(v) : v;
  }
  return out;
}

function uniqueFields(rows) {
  const set = new Set();
  rows.forEach(r => Object.keys(r).forEach(k => set.add(k)));
  return [...set];
}

// ====== Browser JSON fetch vanuit 1 referrer-pagina ======
async function pageFetchJson(page, url, { referrer }) {
  // Gebruik window.fetch vanuit de geladen referrer-pagina met credentials
  const result = await page.evaluate(async (u) => {
    try {
      const res = await fetch(u, {
        credentials: 'include',
        // referrer wordt automatisch de huidige pagina-URL; expliciet meegeven mag niet altijd.
        headers: {
          'accept': 'application/json, text/plain;q=0.8, */*;q=0.5',
          'x-requested-with': 'XMLHttpRequest'
        },
        mode: 'cors',
      });
      const ct = res.headers.get('content-type') || '';
      const text = await res.text(); // eerst text om betere foutmeldingen te kunnen geven
      if (!res.ok) return { __error: `HTTP ${res.status}`, __head: text.slice(0, 200) };
      if (!/application\/json/i.test(ct)) {
        // Val terug op JSON.parse-guard (soms ontbreekt correcte header)
        const h = text.trim().charAt(0);
        if (h !== '{' && h !== '[') return { __error: 'Non-JSON response', __head: text.slice(0, 200) };
        try {
          return JSON.parse(text);
        } catch (e) {
          return { __error: `JSON parse error: ${e.message}`, __head: text.slice(0, 200) };
        }
      }
      try {
        return JSON.parse(text);
      } catch (e) {
        return { __error: `JSON parse error: ${e.message}`, __head: text.slice(0, 200) };
      }
    } catch (e) {
      return { __error: `Fetch failed: ${e.message}` };
    }
  }, url);

  if (result?.__error) {
    console.error(`⚠️ pageFetchJson(${url}) -> ${result.__error}`, result.__head ? `\nHEAD: ${result.__head}` : '');
  }
  return result;
}

// Zoek array in verschillende JSON-vormen
function extractArray(any) {
  if (Array.isArray(any)) return any;
  if (any && typeof any === 'object') {
    // Probeer 'matches', 'data', 'items', of eerste array-veld
    for (const k of ['matches', 'data', 'items', 'rows']) {
      if (Array.isArray(any[k])) return any[k];
    }
    const arrKey = Object.keys(any).find(k => Array.isArray(any[k]));
    if (arrKey) return any[arrKey];
  }
  return null;
}

// Bouw match-URL op basis van MATCH_API_ENDPOINT + verplichte params.
function buildMatchUrl({ gradeId, seasonId }) {
  const base = (MATCH_API_ENDPOINT || '').split('?')[0];
  const u = new URL(base);
  // Neem eventuele bestaande query van MATCH_API_ENDPOINT mee
  const qStr = (MATCH_API_ENDPOINT || '').includes('?') ? MATCH_API_ENDPOINT.split('?')[1] : '';
  if (qStr) new URLSearchParams(qStr).forEach((v, k) => u.searchParams.set(k, v));

  // Minimale verplichte params
  if (!u.searchParams.has('action')) u.searchParams.set('action', 'ors');
  if (!u.searchParams.has('maxrecs')) u.searchParams.set('maxrecs', '500');
  if (!u.searchParams.has('strmflg')) u.searchParams.set('strmflg', '1');

  if (seasonId) u.searchParams.set('seasonid', String(seasonId));
  if (gradeId)  u.searchParams.set('gradeid', String(gradeId));

  return u.toString();
}

(async () => {
  let browser;
  try {
    if (!GRADES_API_ENDPOINT || !MATCH_API_ENDPOINT) {
      throw new Error('GRADES_API_ENDPOINT en MATCH_API_ENDPOINT zijn verplicht.');
    }
    if (!GRADES_API_URL && !MATCH_API_URL) {
      console.warn('⚠️ Geen GRADES_API_URL/MATCH_API_URL als referrer? Gebruik GRADES_API_URL indien mogelijk.');
    }

    const sheets = (SPREADSHEET_ID && GOOGLE_APPLICATION_CREDENTIALS)
      ? await getSheetsClient()
      : null;

    console.log('🚀 Start puppeteer…');
    browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-web-security',
        '--disable-site-isolation-trials',
      ],
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(TIMEOUT_MS);
    page.on('console', msg => console.log('PAGE LOG →', msg.text()));
    page.on('requestfailed', req => console.log('PAGE REQ FAIL →', req.failure()?.errorText, req.url()));

    // 1) Eén referrer-pagina openen voor sessie/cookies
    const refPage = GRADES_API_URL || MATCH_API_URL || 'https://matchcentre.kncb.nl/matches/';
    console.log('📥 Open referrer pagina…', refPage);
    await page.goto(refPage, { waitUntil: 'networkidle0' });
    await delay(SLOWDOWN_MS);

    // 2) Grades ophalen via fetch in browsercontext (geen page.goto naar API!)
    console.log('📥 Haal grades op…');
    const gradesJson = await pageFetchJson(page, GRADES_API_ENDPOINT, { referrer: refPage });
    if (gradesJson?.__error) {
      throw new Error(`Grades endpoint error: ${gradesJson.__error}`);
    }
    const gradesArrRaw = extractArray(gradesJson) || [];
    console.log(`▶ Gevonden ${gradesArrRaw.length} raw grades`);

    // Filter op GRADE_IDS (optioneel)
    const onlyIds = (GRADE_IDS || '').split(',').map(s => s.trim()).filter(Boolean);
    const gradesArr = gradesArrRaw.filter(g => {
      const gid = String(g?.id ?? g?.gradeId ?? '');
      return !onlyIds.length || (gid && onlyIds.includes(gid));
    });

    console.log(`🧮 Te verwerken grades: ${gradesArr.length}${onlyIds.length ? ` (gefilterd uit ${gradesArrRaw.length})` : ''}`);

    const masterRows = [];

    // 3) Per grade: matches ophalen + CSV + Sheets
    for (const g of gradesArr) {
      const gid = String(g?.id ?? g?.gradeId ?? '').trim();
      const seasonId = String(g?.seasonId ?? '').trim();
      if (!gid) {
        console.warn('⚠️ Grade zonder id, sla over:', g);
        continue;
      }
      const sheetName = `Grade_${gid}`;

      console.log(`\n--- 🔁 Grade ${gid} (season ${seasonId || 'n/a'}) ---`);
      if (sheets) await ensureSheet(sheets, sheetName);

      const matchUrl = buildMatchUrl({ gradeId: gid, seasonId });
      console.log('🌐 Fetch matches:', matchUrl);
      const matchesJson = await pageFetchJson(page, matchUrl, { referrer: refPage });
      if (matchesJson?.__error) {
        const msg = `⚠️ Fout bij grade ${gid}: ${matchesJson.__error}` + (matchesJson.__head ? ` | head=${matchesJson.__head}` : '');
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

      const rows = dataArr.map(obj => {
        const flat = flattenObject(obj);
        flat._grade = gid;
        flat._season = seasonId || '';
        return flat;
      });

      // CSV per grade
      if (rows.length) {
        try {
          const fields = uniqueFields(rows);
          const csv = parse(rows, { fields, defaultValue: '' });
          fs.writeFileSync(`${sheetName}.csv`, csv, 'utf8');
          console.log(`💾 ${sheetName}.csv geschreven (${rows.length} rijen)`);
        } catch (e) {
          console.warn(`⚠️ Kon ${sheetName}.csv niet schrijven:`, e.message);
        }
      }

      // Sheets per grade
      if (sheets && rows.length) {
        await writeValues(sheets, sheetName, rows);
        console.log(`📈 Sheet "${sheetName}" geüpdatet`);
      }

      masterRows.push(...rows);
      await delay(SLOWDOWN_MS);
    }

    // 4) MASTER bijwerken
    if (masterRows.length) {
      console.log(`\n📊 MASTER opbouwen: ${masterRows.length} rijen`);
      if (sheets) {
        await ensureSheet(sheets, 'MASTER');
        await writeValues(sheets, 'MASTER', masterRows);
        console.log('📈 MASTER sheet bijgewerkt');
      }
      try {
        const fields = uniqueFields(masterRows);
        const csv = parse(masterRows, { fields, defaultValue: '' });
        fs.writeFileSync(`MASTER.csv`, csv, 'utf8');
        console.log('💾 MASTER.csv geschreven');
      } catch (e) {
        console.warn('⚠️ Kon MASTER.csv niet schrijven:', e.message);
      }
    }

    console.log('\n✅ Klaar!');
  } catch (err) {
    console.error('🛑 FATAAL:', err.message);
    await notifyTelegram(`Fatal error: ${err.message}`);
  } finally {
    if (browser) {
      await browser.close();
      console.log('✅ Browser gesloten');
    }
  }
})();
