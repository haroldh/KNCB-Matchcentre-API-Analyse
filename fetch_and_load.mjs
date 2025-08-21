import fetch from 'node-fetch';
import fs from 'fs';
import { parse } from 'json2csv';
import { google } from 'googleapis';
import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

const {
  MATCH_API_URL,
  GRADES_API_URL,
  SEASONS_API_URL,
  SPREADSHEET_ID,
  GOOGLE_APPLICATION_CREDENTIALS,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID,
  CSV_OUTPUT = 'matches.csv'
} = process.env;

async function notifyTelegram(text) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text,
      parse_mode: 'HTML'
    });
  } catch (err) {
    console.error('❌ Telegram notification failed', err);
  }
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { 'User-Agent': 'Node.js' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} voor ${url}`);
  return res.json();
}

(async () => {
  try {
    console.log('🚀 Start ophalen van data');

    const [matchesJson, gradesJson, seasonsJson] = await Promise.all([
      fetchJson(MATCH_API_URL),
      fetchJson(GRADES_API_URL),
      fetchJson(SEASONS_API_URL)
    ]);

    const matches = matchesJson.matches || matchesJson.data?.matches || [];
    const grades = gradesJson.grades || gradesJson.data || [];
    const seasons = seasonsJson.seasons || seasonsJson.data || [];

    console.log(`→ Found ${matches.length} matches, ${grades.length} grades, ${seasons.length} seasons`);

    if (!matches.length) throw new Error('Geen matchdata opgehaald');

    const rows = matches.map(m => ({
      matchId:     m.matchId ?? '',
      date:        m.matchDate ?? '',
      season:      seasons.find(s => s.id === m.seasonId)?.name ?? '',
      grade:       grades.find(g => g.id === m.gradeId)?.name ?? '',
      home:        m.homeTeam?.name ?? '',
      away:        m.awayTeam?.name ?? '',
      venue:       m.venue?.name ?? '',
      status:      m.status ?? '',
      homeScore:   m.score?.home ?? '',
      awayScore:   m.score?.away ?? '',
      result:      m.score?.fullTime ?? '',
    }));

    // Schrijf naar CSV
    const csv = parse(rows, { fields: Object.keys(rows[0]), defaultValue: '' });
    fs.writeFileSync(CSV_OUTPUT, csv);
    console.log(`✅ ${CSV_OUTPUT} geschreven met ${rows.length} rijen`);

    // Schrijf naar Google Sheets
    const auth = new google.auth.GoogleAuth({
      keyFile: GOOGLE_APPLICATION_CREDENTIALS,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
    const sheets = google.sheets({ version: 'v4', auth });

    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Matches!A2:Z'
    });
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Matches!A2',
      valueInputOption: 'RAW',
      requestBody: {
        values: rows.map(r => Object.values(r))
      }
    });

    console.log(`✅ Google Sheet bijgewerkt: ${rows.length} rijen`);
  } catch (e) {
    console.error('🛑 Fout:', e.message);
    await notifyTelegram(`<b>!⚠️ Fout tijdens data verwerken:</b>\n${e.message}`);
    process.exit(1);
  }
})();
