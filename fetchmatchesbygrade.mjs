import puppeteer from 'puppeteer';
import fs from 'fs';
import { parse } from 'json2csv';
import { google } from 'googleapis';
import dotenv from 'dotenv';
import axios from 'axios';
import { setTimeout } from 'node:timers/promises';

dotenv.config();

const {
  GRADES_API_URL,
  MATCH_API_URL,
  GRADES_API_ENDPOINT,
  MATCH_API_ENDPOINT,
  SPREADSHEET_ID,
  GOOGLE_APPLICATION_CREDENTIALS,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
} = process.env;

async function notifyTelegram(text) {
  await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    chat_id: TELEGRAM_CHAT_ID,
    text,
    parse_mode: 'HTML',
  });
}

async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: GOOGLE_APPLICATION_CREDENTIALS,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

async function getSheetTitles(sheets) {
  const resp = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  return resp.data.sheets.map(s => s.properties.title);
}

async function ensureSheet(sheets, title) {
  const titles = await getSheetTitles(sheets);
  if (!titles.includes(title)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title } } }],
      },
    });
    console.log(`➕ Sheet tab "${title}" gemaakt`);
  }
}

async function writeValues(sheets, sheetName, rows) {
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

function flattenObject(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = (v !== null && typeof v === 'object') ? JSON.stringify(v) : v;
  }
  return out;
}

(async () => {
  let browser;
  try {
    const sheets = await getSheetsClient();
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

    await page.goto(GRADES_API_ENDPOINT, { waitUntil: 'networkidle0' });
    await setTimeout(2000);

    const gradesJson = await page.evaluate(async url => {
      const r = await fetch(url, { credentials: 'include' });
      return r.ok ? r.json() : { __error: `HTTP ${r.status}` };
    }, GRADES_API_ENDPOINT);

    if (gradesJson.__error) throw new Error(gradesJson.__error);
    const gradesArr = Array.isArray(gradesJson) ? gradesJson : Object.values(gradesJson).filter(o => o && typeof o === 'object');
    console.log(`▶ Found ${gradesArr.length} grades`);

    const masterRows = [];

    for (const g of gradesArr) {
      const gid = g.id || g.gradeId;
      const sheetName = `Grade_${gid}`;

      await ensureSheet(sheets, sheetName);
console.log(`▶ Fetching ${gid} grade`);
      const url = MATCH_API_ENDPOINT.split('?')[0]
        + `?seasonid=${g.seasonId || ''}&gradeid=${gid}&action=ors&maxrecs=500&strmflg=1`;
      await page.goto(url, { waitUntil: 'networkidle0' });
      await setTimeout(2000);

      const json = await page.evaluate(async u => {
        const r = await fetch(u, { credentials: 'include' });
        return r.ok ? r.json() : { __error: `HTTP ${r.status}` };
      }, url);
      if (json.__error) {
        console.error(`⚠ Error fetching grade ${gid}: ${json.__error}`);
        await notifyTelegram(`Error fetching grade ${gid}: ${json.__error}`);
        continue;
      }

      const arr = Array.isArray(json) ? json : Object.values(json).filter(v => Array.isArray(v));
      console.log(`▶ Grade_${gid}: ${arr.length} matches`);

      if (!arr.length) continue;

      const rows = arr.map(obj => {
        const flat = flattenObject(obj);
        flat._grade = gid.toString();
        return flat;
      });

      const csv = parse(rows, { fields: Object.keys(rows[0]), defaultValue: '' });
      fs.writeFileSync(`${sheetName}.csv`, csv);
      console.log(`✏ ${sheetName}.csv geschreven`);

      await writeValues(sheets, sheetName, rows);
      console.log(`📈 Sheet "${sheetName}" geüpdatet`);

      masterRows.push(...rows);
    }

    if (masterRows.length) {
      await ensureSheet(sheets, 'MASTER');
      await writeValues(sheets, 'MASTER', masterRows);
      console.log(`📈 MASTER sheet met ${masterRows.length} rijen bijgewerkt`);
    }

    console.log('✅ Klaar!');

  } catch (err) {
    console.error('🛑 FATAAL:', err.message);
    await notifyTelegram(`<b>Fatal error:</b> ${err.message}`);
  } finally {
    browser && await browser.close();
  }
})();
