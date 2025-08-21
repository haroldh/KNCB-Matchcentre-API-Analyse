import fetch from 'node-fetch';
import fs from 'fs';
import { parse } from 'json2csv';
import dotenv from 'dotenv';

dotenv.config();

const API_URL = process.env.API_URL;
const CSV_FILE = process.env.CSV_FILE || 'matches_api.csv';

async function fetchMatches() {
  const res = await fetch(API_URL, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; Node.js)',
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();
  return json.matches || json.data?.matches || json;
}

(async () => {
  try {
    console.log('🚀 Fetching matches from API...');
    const matches = await fetchMatches();
    console.log(`Fetched ${matches.length} matches`);

    const rows = matches.map(m => ({
      date:        m.matchDate ?? m.date ?? '',
      home:        m.homeTeam?.name ?? m.home ?? '',
      away:        m.awayTeam?.name ?? m.away ?? '',
      venue:       m.venue?.name ?? '',
      status:      m.status ?? m.matchStatus ?? '',
      score:       m.score?.fullTime ?? `${m.homeScore ?? ''}-${m.awayScore ?? ''}`,
      competition: m.competition?.name ?? m.grade ?? '',
    }));

    if (rows.length) {
      const csv = parse(rows, { fields: Object.keys(rows[0]), defaultValue: '' });
      fs.writeFileSync(CSV_FILE, csv);
      console.log(`✅ Wrote ${rows.length} rows to ${CSV_FILE}`);
    } else {
      console.warn('⚠️ No match data available to write');
    }
  } catch (e) {
    console.error('🛑 Error:', e.message);
    process.exit(1);
  }
})();
