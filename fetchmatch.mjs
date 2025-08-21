import puppeteer from 'puppeteer';
import fs from 'fs';
import { parse } from 'json2csv';
import { google } from 'googleapis';
import dotenv from 'dotenv';
import axios from 'axios';
import { setTimeout } from 'node:timers/promises';

dotenv.config();

// Config
const {
  SPREADSHEET_ID,
  GOOGLE_APPLICATION_CREDENTIALS,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  CSV_OUTPUT // default CSV folder
} = process.env;

const json = JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS));
console.log('🧩 credentials.json contains keys:', Object.keys(json));


// Haal endpoints
const endpoints = Object.entries(process.env)
  .filter(([k,v])=>k.endsWith('_API_URL') && v)
  .map(([k,v])=>({ name: k.replace('_API_URL',''), url: v }));

async function notifyTelegram(text){
  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }
    );
  } catch(e){
    console.error('🔔 Telegram error:', e.response?.data||e.message);
  }
}

async function sheetsClient(){
  const auth = new google.auth.GoogleAuth({
    keyFile: GOOGLE_APPLICATION_CREDENTIALS,
    scopes:['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version:'v4', auth });
}

function timestamp(){ return new Date().toISOString(); }

async function writeValues(sheets, sheetName, rows){
  const headers = Object.keys(rows[0]);
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:Z`
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'RAW',
    requestBody:{ values: [headers] }
  });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A2`,
    valueInputOption: 'RAW',
    requestBody:{ values: rows.map(r => headers.map(h => r[h])) }
  });
}

async function logAction(sheets, record){
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range:'Log!A:F',
    valueInputOption:'RAW',
    requestBody:{ values:[[record.timestamp,record.script,record.func,record.action,record.table,record.result]] }
  });
}

async function run(){
  let browser, sheets;
  const scriptName = 'fetch_and_sheet_all';
  try{
    sheets = await sheetsClient();
    browser = await puppeteer.launch({
      headless:true,
      args:['--no-sandbox','--disable-web-security','--disable-features=IsolateOrigins,SitePerProcess','--disable-site-isolation-trials']
    });
    const page = await browser.newPage();
    page.on('console',msg=>console.log('PAGE →', msg.text()));
    await page.goto('https://matchcentre.kncb.nl/matches/',{waitUntil:'networkidle0'});
    await setTimeout(3000);

    for(const ep of endpoints){
      const func = 'fetchEndpoint';
      try{
        console.log(`➡️ Fetching ${ep.name}`);
        const json = await page.evaluate(async url=>{
          const res = await fetch(url,{credentials:'include'});
          if(!res.ok) return {__error:`HTTP ${res.status}`};
          return res.json();
        }, ep.url);
        if(json.__error) throw new Error(json.__error);

        let arr;
        if(Array.isArray(json)) { arr = json; }
        else {
          const key = Object.keys(json).find(k=>Array.isArray(json[k]));
          if(!key) throw new Error('No array data found');
          arr = json[key];
        }
// Flatten nested values
const rows = arr.map(obj => {
  const rec = {};
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    rec[k] = (v !== null && typeof v === 'object') ? JSON.stringify(v) : v;
  }
  return rec;
});

        
        console.log(`📦 ${ep.name}: ${arr.length} records`);
        const csvContent = parse(arr,{fields:Object.keys(arr[0]),defaultValue:''});
        const fname = `${ep.name}.csv`;
// Schrijf CSV met JSON-strings
const csv = parse(rows, { fields: Object.keys(rows[0]), defaultValue: '' });
fs.writeFileSync(`${ep.name}.csv`, csv);
        console.log(`💾 Written ${fname}`);

 // Schrijf naar Google Sheets met headers en JSON-strings intact
await writeValues(sheets, ep.name, rows);
        console.log(`🧩 Google Sheet tab "${ep.name}" updated`);

        await logAction(sheets,{
          timestamp:timestamp(),
          script:scriptName,
          func,
          action:'write',
          table:ep.name,
          result:arr.length
        });
      }catch(err){
        console.error(`❌ Error ${ep.name}:`, err.message);
        await notifyTelegram(`<b>Error on ${ep.name}:</b> ${err.message}`);
        await logAction(sheets,{
          timestamp:timestamp(),
          script:scriptName,
          func:'fetchEndpoint',
          action:'error',
          table:ep.name,
          result:err.message
        });
      }
    }

  }catch(f){
    console.error('🛑 Fatal:',f);
    await notifyTelegram(`<b>Fatal error:</b> ${f.message}`);
  }finally{
    if(browser) await browser.close();
  }
}

run();
