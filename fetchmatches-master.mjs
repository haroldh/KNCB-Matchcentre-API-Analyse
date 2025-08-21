// fetchmatches-master.mjs
// Merge van fetchmatch-werkt.mjs (werkende fetch + logging) en fetchmatchesbygrade.mjs (per-grade + Sheets/CSV).
// Node 18+ (native fetch), met Puppeteer in browsercontext voor geauthenticeerde fetches.
// Run: node fetchmatches-master.mjs

import puppeteer from 'puppeteer';
import fs from 'fs';
import { parse } from 'json2csv';
import { google } from 'googleapis';
import dotenv from 'dotenv';
import axios from 'axios';
import { setTimeout as delay } from 'node:timers/promises';

dotenv.config();

// ---- Env / Config ----
const {
  GRADES_API_ENDPOINT,
  MATCH_API_ENDPOINT,
  SPREADSHEET_ID,
  GOOGLE_APPLICATION_CREDENTIALS,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  // Optioneel: filter de op te halen grades via komma-gescheiden lijst, bv. "73942,73943"
  GRADE_IDS,
} = process.env;

const SLOWDOWN_MS = Number(process.env.SLOWDOWN_MS ?? 2000);         // wachttijd na elke navigatie
const TIMEOUT_MS  = Number(process.env.PUPPETEER_TIMEOUT_MS ?? 30000);

// ---- Telegram ----
async function notifyTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text,
      parse_mode: 'HTML',
    });
  } catch (e) {
    console.error('❌ Telegram error:', e.response?.data || e.message || e);
  }
}

// ---- Google Sheets helpers ----
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
    range: `${sheetName}!A:Z`,
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

// ---- Data helpers ----
function flattenObject(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = (v !== null && typeof v === 'object') ? JSON.stringify(v) : v;
  }
  return out;
}

// Vind de array met wedstrijden in diverse JSON-shapes.
// (Logica overgenomen/uitgebreid vanuit je werkende script.)
function extractArrayFromJson(json) {
  if (Array.isArray(json)) return json;
  if (json && typeof json === 'object') {
    const arrKey = Object.keys(json).find(k => Array.isArray(json[k]));
    if (arrKey) return json[arrKey];
  }
  return null;
}

// ---- Runner ----
(async () => {
  let browser;
  try {
    if (!GRADES_API_ENDPOINT || !MATCH_API_ENDPOINT) {
      throw new Error('GRADES_API_ENDPOINT en MATCH_API_ENDPOINT zijn verplicht.');
    }
    if (!SPREADSHEET_ID || !GOOGLE_APPLICATION_CREDENTIALS) {
      console.warn('⚠️ Geen Google Sheets credentials/ID? Sheets-synchronisatie wordt dan overgeslagen.');
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
        '--disable-features=IsolateOrigins,SitePerProcess',
        '--disable-site-isolation-trials',
      ],
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(TIMEOUT_MS);
    page.on('console', (msg) => console.log('PAGE LOG →', msg.text()));

    // 1) Ga eerst naar MatchCentre om sessie/cookies te krijgen (werkwijze uit fetchmatch-werkt)
    console.log('📥 Open MatchCentre voor sessie…');
    await page.goto('https://matchcentre.kncb.nl/matches/', { waitUntil: 'networkidle0' });
    await delay(3000);

    // 2) Haal grades op zoals in fetchmatchesbygrade
    console.log('📥 Haal grades op…');
    await page.goto(GRADES_API_ENDPOINT, { waitUntil: 'networkidle0' });
    await delay(SLOWDOWN_MS);

    const gradesJson = await page.evaluate(async url => {
      const r = await fetch(url, { credentials: 'include' });
      return r.ok ? r.json() : { __error: `HTTP ${r.status}` };
    }, GRADES_API_ENDPOINT);

    if (gradesJson?.__error) throw new Error(gradesJson.__error);

    // grades kunnen array of object zijn
    const gradesArr = Array.isArray(gradesJson)
      ? gradesJson
      : Object.values(gradesJson).filter(o => o && typeof o === 'object');

    console.log(`▶ Gevonden ${gradesArr.length} grades`);

    // Optioneel filteren via env GRADE_IDS
    const onlyIds = (GRADE_IDS || '')
      .split(',')
      .map(s => s.trim())
      .filter(Boolean);
    const filteredGrades = onlyIds.length
      ? gradesArr.filter(g => String(g.id || g.gradeId) && onlyIds.includes(String(g.id || g.gradeId)))
      : gradesArr;

    if (onlyIds.length) {
      console.log(`🧮 Filter actief: ${onlyIds.length} id's → ${filteredGrades.length} grades te verwerken`);
    }

    const masterRows = [];

    // 3) Per grade: matches ophalen, CSV + Sheets updaten
    for (const g of filteredGrades) {
      const gid = g.id || g.gradeId;
      const seasonId = g.seasonId || '';
      const sheetName = `Grade_${gid}`;

      console.log(`\n--- 🔁 Grade ${gid} ---`);
      if (sheets) await ensureSheet(sheets, sheetName);

      // URL opbouw zoals je by-grade script deed; behoudt werkende query's
      const base = MATCH_API_ENDPOINT.split('?')[0];
      const url =
        `${base}?seasonid=${encodeURIComponent(seasonId)}&gradeid=${encodeURIComponent(gid)}&action=ors&maxrecs=500&strmflg=1`;

      console.log(`🌐 Fetch: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle0' });
      await delay(SLOWDOWN_MS);

      const json = await page.evaluate(async u => {
        const r = await fetch(u, { credentials: 'include' });
        return r.ok ? r.json() : { __error: `HTTP ${r.status}` };
      }, url);

      if (json?.__error) {
        console.error(`⚠️ Fout bij grade ${gid}: ${json.__error}`);
        await notifyTelegram(`<b>Fout bij grade ${gid}:</b> ${json.__error}`);
        continue;
      }

      // Gebruik de "werkende" array-extractie uit fetchmatch-werkt
      const dataArr = extractArrayFromJson(json);
      if (!Array.isArray(dataArr)) {
        console.warn(`⚠️ Geen array gevonden voor grade ${gid}; sla over`);
        continue;
      }

      console.log(`✅ Grade_${gid}: ${dataArr.length} matches`);
      if (dataArr.length) {
        console.log(`🧪 Voorbeeld eerste item:\n${JSON.stringify(dataArr[0], null, 2)}`);
      }

      // Flatten + _grade toevoegen (zoals in je by-grade script)
      const rows = dataArr.map(obj => {
        const flat = flattenObject(obj);
        flat._grade = String(gid);
        return flat;
      });

      // CSV per grade
      if (rows.length) {
        const csv = parse(rows, { fields: Object.keys(rows[0]), defaultValue: '' });
        fs.writeFileSync(`${sheetName}.csv`, csv, 'utf8');
        console.log(`💾 ${sheetName}.csv geschreven (${rows.length} rijen)`);
      }

      // Sheets per grade
      if (sheets && rows.length) {
        await writeValues(sheets, sheetName, rows);
        console.log(`📈 Sheet "${sheetName}" geüpdatet`);
      }

      masterRows.push(...rows);
    }

    // 4) MASTER bijwerken
    if (masterRows.length) {
      console.log(`\n📊 MASTER opbouwen: ${masterRows.length} rijen`);
      if (sheets) {
        await ensureSheet(sheets, 'MASTER');
        await writeValues(sheets, 'MASTER', masterRows);
        console.log('📈 MASTER sheet bijgewerkt');
      }
      // Optioneel ook een CSV met alles:
      try {
        const csv = parse(masterRows, { fields: Object.keys(masterRows[0]), defaultValue: '' });
        fs.writeFileSync(`MASTER.csv`, csv, 'utf8');
        console.log('💾 MASTER.csv geschreven');
      } catch (e) {
        console.warn('⚠️ Kon MASTER.csv niet schrijven:', e.message);
      }
    }

    console.log('\n✅ Klaar!');
  } catch (err) {
    console.error('🛑 FATAAL:', err.message);
    await notifyTelegram(`<b>Fatal error:</b> ${err.message}`);
  } finally {
    if (browser) {
      await browser.close();
      console.log('✅ Browser gesloten');
    }
  }
})();
