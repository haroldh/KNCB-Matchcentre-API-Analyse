// fetch_all_endpoints.mjs
import puppeteer from "puppeteer";
import fs from "fs";
import { parse } from "json2csv";
import dotenv from "dotenv";
import axios from "axios";
import { setTimeout } from "node:timers/promises";

dotenv.config();

import { google } from 'googleapis';
const auth = new google.auth.GoogleAuth({
  keyFile: process.env.GOOGLE_APPLICATION_CREDENTIALS,
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});
const sheets = google.sheets({ version: 'v4', auth });


// Haal alle *_API_URL variabelen
const env = process.env;
const endpoints = Object.entries(env)
  .filter(([k, v]) => k.endsWith("_API_ENDPOINT") && v)
  .map(([k, v]) => ({ name: k.replace("_API_ENDPOINT", ""), url: v }));

async function notifyTelegram(msg) {
  try {
    await axios.post(
      `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`,
      { chat_id: env.TELEGRAM_CHAT_ID, text: msg, parse_mode: "HTML" }
    );
  } catch (e) {
    console.error("❌ Telegram error:", e.response?.data || e);
  }
}

(async () => {
  let browser;
  try {
    console.log(`🚀 Start puppeteer-fetch voor ${endpoints.length} endpoints`);
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
    page.on("console", (msg) => console.log("PAGE LOG →", msg.text()));

    await page.goto("https://matchcentre.kncb.nl/matches/", {
      waitUntil: "networkidle0",
    });
    console.log("📥 Pagina geladen, wacht 3s...");
    await setTimeout(3000);

    for (const ep of endpoints) {
      console.log(`\n--- 🔁 Verwerking endpoint ${ep.name} → ${ep.url}`);
      try {
        // fetchen in browser met credentials
        const json = await page.evaluate(async (url) => {
          const resp = await fetch(url, { credentials: "include" });
          return resp.ok ? resp.json() : { __error: `HTTP ${resp.status}` };
        }, ep.url);

        if (json.__error) throw new Error(json.__error);

        let dataArr;
        if (Array.isArray(json)) {
          dataArr = json;
          console.log(`✅ JSON is een array met ${dataArr.length} items`);
        } else {
          const keys = Object.keys(json);
          const arrKey = keys.find((k) => Array.isArray(json[k]));
          if (arrKey) {
            dataArr = json[arrKey];
            console.log(
              `✅ Gevonden array "${arrKey}" met ${dataArr.length} items`
            );
          } else {
            console.warn(`⚠️ Geen array property gevonden in ${ep.name}`);
            continue;
          }
        }
console.log(`🧪 Eerste item:`, JSON.stringify(dataArr[0], null, 2));

//        console.log(`✅ Gevonden ${dataArr.length} items in key "${arrKey}"`);

        if (!dataArr.length) {
          console.warn(`⚠️ ${ep.name}: Lege dataset, ga door`);
          continue;
        }

        // Schrijf CSV
        const csv = parse(dataArr, {
          fields: Object.keys(dataArr[0]),
          defaultValue: "",
        });
        const fn = `${ep.name}.csv`;
        fs.writeFileSync(fn, csv, "utf8");
        console.log(`💾 ${fn} geschreven (${dataArr.length} rijen)`);
      } catch (err) {
        console.error(`❌ Fout bij ${ep.name}:`, err.message);
        await notifyTelegram(
          `<b>⚠️ Fout bij endpoint ${ep.name}:</b>\n${err.message}`
        );
      }
    }

    console.log("\n🎉 Alle endpoints verwerkt.");
  } catch (fatal) {
    console.error("🛑 FATALE FOUT:", fatal);
    await notifyTelegram(
      `<b>🚨 Fatal error tijdens fetch:</b>\n${fatal.message}`
    );
  } finally {
    if (browser) await browser.close();
    console.log("✅ Browser gesloten, klaar.");
  }
})();
