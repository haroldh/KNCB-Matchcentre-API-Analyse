import { google } from 'googleapis';
import fs from 'fs';

const credPath = process.argv[2] || './.gcredentials.json';
const spreadsheetId = process.argv[3]; // optioneel

(async () => {
  const auth = new google.auth.GoogleAuth({
    keyFile: credPath,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const client = await auth.getClient();
  const token = await auth.getAccessToken(); // <- hier faalt 'invalid_grant' als key fout is
  console.log('✅ Access token verkregen (ingekort):', String(token).slice(0, 30), '...');

  if (spreadsheetId) {
    const sheets = google.sheets({ version: 'v4', auth });
    const meta = await sheets.spreadsheets.get({ spreadsheetId });
    console.log('✅ Sheets titel:', meta.data.properties?.title);
  }
})();
