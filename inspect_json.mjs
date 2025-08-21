import fetch from 'node-fetch';
import dotenv from 'dotenv';
dotenv.config();

(async () => {
  const res = await fetch(process.env.API_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Node.js)' }
  });
  const json = await res.json();

  console.log('JSON top-level keys:', Object.keys(json));
  console.log('🥽 Full JSON preview:', JSON.stringify(json, null, 2).slice(0, 1000), '…');
  process.exit(0);
})();
