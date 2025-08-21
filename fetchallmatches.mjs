import puppeteer from "puppeteer";
import fs from "fs";
import { parse } from "json2csv";
import { google } from "googleapis";
import dotenv from "dotenv";
import axios from "axios";
import { setTimeout } from "node:timers/promises";
// import flatten from "json2csv/transforms/flatten";
dotenv.config();

// Config
const {
  MATCH_API_URL, // gebruik zonder gradeid parameter
  GRADES_API_URL,
  SPREADSHEET_ID,
  GOOGLE_APPLICATION_CREDENTIALS,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  CSV_OUTPUT = "matches.csv",
} = process.env;

// Notificatie
async function notifyTelegram(text) {
  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: "HTML" }
    );
  } catch (e) {
    console.error("Telegram error", e.message);
  }
}

// Sheets client
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: GOOGLE_APPLICATION_CREDENTIALS,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  return google.sheets({ version: "v4", auth });
}

async function getSheetTitles(sheets) {
  const resp = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  return resp.data.sheets.map((s) => s.properties.title);
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
    console.log(`➕ Tab "${title}" aangemaakt`);
  }
}

async function flattenArray(arr) {
// Flatten
  const rows = arr.map((obj) => {
    const rec = {};
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      rec[k] = v !== null && typeof v === "object" ? JSON.stringify(v) : v;
    }
    // rec._grade = name;
    return rec;
  });
}

/**
 * Flatten nested objects/arrays: maakt enkele laag records met JSON-strings
 * @param {object} obj
 * @returns {object}
 */
function flattenObject(obj, prefix = '') {
  let result = {};
  for (const [k, v] of Object.entries(obj)) {
    const key = prefix ? `${prefix}_${k}` : k;
    if (v !== null && typeof v === 'object') {
      // Genest object of array: stringify
      result[key] = JSON.stringify(v);
    } else {
      result[key] = v;
    }
  }
  return result;
}


// Schrijven met header
async function writeValues(sheets, sheetName, rows) {
  const headers = Object.keys(rows[0]);
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:Z`,
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
    requestBody: { values: rows.map((r) => headers.map((h) => r[h])) },
  });
}

// Main
(async () => {
  const scriptName = "fetch_matches_by_grade";
  let browser;
  try {
    const sheets = await getSheetsClient();
    browser = await puppeteer.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-web-security",
        "--disable-features=IsolateOrigins,SitePerProcess",
        "--disable-site-isolation-trials",
      ],
    });
    const page = await browser.newPage();
    page.on("console", (msg) => console.log("PAGE →", msg.text()));

    // 1. Haal alle grades op
    await page.goto(GRADES_API_URL, { waitUntil: "networkidle0" });
    await setTimeout(2000);
    const gradesJson = await page.evaluate(async (url) => {
      const res = await fetch(url, { credentials: "include" });
      return res.ok ? res.json() : { __error: `HTTP ${res.status}` };
    }, GRADES_API_URL);
    if (gradesJson.__error)
      throw new Error(`Grades failed: ${gradesJson.__error}`);

    const gradesArr = Array.isArray(gradesJson)
      ? gradesJson
      : Object.values(gradesJson).filter((v) => v && typeof v === "object");
    console.log(`🧮 Found ${gradesArr.length} grades`);

    //

    //
    if (gradesArr.length) {
      const flattenedArray = flattenArray(gradesArr);

      await ensureSheet(sheets, "GRADES");
      await writeValues(sheets, "GRADES", flattenedArray);
      console.log(`📊 GRADES sheet with ${flattenedArray.length} rows updated`);

      const masterRows = [];

      for (const g of gradesArr) {
        const gid = g.id || g.gradeId;
        const name = g.name || g.gradeName || `G_${gid}`;
        console.log(`➡️ Grade ${name} (id=${gid})`);

        // MATCH_API_URL=https://api.resultsvault.co.uk/rv/134453/matches/?apiid=1002&seasonid=19&gradeid=71374&action=ors&maxrecs=1000&strmflg=1
        const url =
          MATCH_API_URL.split("?")[0] +
          `?apiid=1002&seasonid=${
            g.seasonId || "19"
          }&gradeid=${gid}&action=ors&maxrecs=500&strmflg=1`;
        console.log(`url: ${url} processing...`);
        await page.goto(url, { waitUntil: "networkidle0" });
        await setTimeout(2000);

        const json = await page.evaluate(async (u) => {
          const r = await fetch(u, { credentials: "include" });
          return r.ok ? r.json() : { __error: `HTTP ${r.status}` };
        }, url);

        if (json.__error) {
          console.error(`⚠️ Grades [${name}] fetch failed: ${json.__error}`);
          await notifyTelegram(`🧩 Grade ${name} fetch error: ${json.__error}`);
          continue;
        }

const rows = arr.map(obj => {
  const flat = flattenObject(obj);
  flat._grade = name;
  return flat;
});


        // CSV per grade
        const csvFile = `${name}.csv`;
        const csv = parse(rows, {
          fields: Object.keys(rows[0]),
          defaultValue: "",
        });
        fs.writeFileSync(csvFile, csv);
        console.log(`💾 Written ${csvFile}`);

        // Sheet per grade
        const sheetName = `Grade_${gid}`;
        await ensureSheet(sheets, sheetName);
        await writeValues(sheets, sheetName, rows);

        console.log(`📊 Google Sheet "${name}" updated`);

        masterRows.push(...rows);
      }

      // Master sheet combining all grades
      if (masterRows.length) {
        await ensureSheet(sheets, "MASTER");
        await writeValues(sheets, "MASTER", masterRows);
        console.log(`📊 MASTER sheet with ${masterRows.length} rows updated`);
      }

      console.log("🎉 Done.");
      await notifyTelegram(
        `✅ Completed fetching ${gradesArr.length} grades, total ${masterRows.length} matches.`
      );
    }
  } catch (e) {
    console.error("🛑 Fatal error:", e);
    await notifyTelegram(`<b>Fatal error in ${scriptName}:</b> ${e.message}`);
  } finally {
    if (browser) await browser.close();
  }
})();
