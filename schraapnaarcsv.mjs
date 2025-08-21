import puppeteer from 'puppeteer';
import fs from 'fs';
import { parse } from 'json2csv';
import { setTimeout } from 'node:timers/promises';

(async () => {
  console.log('🚀 Start script');

  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();

  // Log alleen relevante berichten
  page.on('console', msg => {
    const text = msg.text();
    if (text.includes('Found rows') || text.includes('Selector1') || text.includes('Selector2')) {
      console.log('📋 PAGE LOG ->', text);
    }
  });

  // Stilzwijgend fouten negeren die irrelevant zijn
  page.on('pageerror', () => {});
  page.on('error', () => {});
  page.on('requestfailed', request => {
    const failure = request.failure();
    if (failure && failure.errorText.includes('ERR_INVALID_HANDLE')) {
      // negeren
    } else {
      console.warn('🔻 Request failed:', failure ? failure.errorText : request.url());
    }
  });

  await page.goto('https://matchcentre.kncb.nl/matches/', { waitUntil: 'domcontentloaded' });
  console.log('Pagina geladen, wacht 5s extra…');
  await setTimeout(5000);

  const counts = await page.evaluate(() => {
    const sel1 = document.querySelectorAll('div[role="row"]:not([aria-rowindex="1"])').length;
    console.log('Selector1 rows:', sel1);
    return sel1;
  });
  console.log('→ Aantal rows:', counts);

  if (counts === 0) {
    console.error('❌ Geen rijen gevonden!');
    await browser.close();
    process.exit(1);
  }

  const data = await page.evaluate(() => {
    const rows = document.querySelectorAll('div[role="row"]:not([aria-rowindex="1"])');
    console.log('Found rows:', rows.length);
    return Array.from(rows).map(r => {
      const cells = r.querySelectorAll('div[role="cell"]');
      return {
        date:  cells[0]?.innerText.trim(),
        teams: cells[1]?.innerText.trim(),
        score: cells[2]?.innerText.trim(),
        venue: cells[3]?.innerText.trim(),
      };
    });
  });

  console.log('Data:', data);
  await browser.close();

  if (data.length) {
    const csv = parse(data, { fields: ['date','teams','score','venue'] });
    fs.writeFileSync('matches_clean.csv', csv);
    console.log(`✅ CSV geschreven (${data.length} rijen)`);
  } else {
    console.error('⚠️ Lege data-array, geen CSV aangemaakt.');
  }
})();

