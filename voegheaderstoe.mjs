import { google } from "googleapis";
import { GoogleAuth, Impersonated } from "google-auth-library";
import dotenv from "dotenv";

dotenv.config();

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const SHEETS_SCOPE   = "https://www.googleapis.com/auth/spreadsheets";

// Gewenste headers
const HEADERS = {
  RUNS:    ["run_id", "start_time", "end_time", "script", "version", "grade_count", "match_count", "errors", "note"],
  LOG:     ["run_id", "timestamp", "script", "function", "action", "table", "level", "message", "detail"],
  CHANGES: ["run_id", "timestamp", "match_id", "change_type", "field", "old_hash", "new_hash", "grade_id"],
};

if (!SPREADSHEET_ID) {
  console.error("❌ SPREADSHEET_ID ontbreekt in .env");
  process.exit(1);
}

async function getSheetsClient() {
  const impersonate = process.env.GOOGLE_IMPERSONATE_SERVICE_ACCOUNT;
  if (impersonate && impersonate.trim()) {
    const auth = new GoogleAuth({ scopes: ["https://www.googleapis.com/auth/cloud-platform"] });
    const src  = await auth.getClient();
    const imp  = new Impersonated({
      sourceClient: src,
      targetPrincipal: impersonate,
      delegates: [],
      lifetime: 3600,
      targetScopes: [SHEETS_SCOPE],
    });
    return google.sheets({ version: "v4", auth: imp });
  }
  const auth = new GoogleAuth({ keyFile: process.env.GOOGLE_APPLICATION_CREDENTIALS, scopes: [SHEETS_SCOPE] });
  return google.sheets({ version: "v4", auth });
}

// Case-insensitief de tab vinden (en zo nodig aanmaken). Retourneert {title, sheetId}
async function ensureSheetCaseInsensitive(sheets, desiredTitle) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const list = (meta.data.sheets || []).map(s => ({ title: s.properties.title, id: s.properties.sheetId }));
  const wantedLower = desiredTitle.toLowerCase();

  const hit = list.find(s => s.title.toLowerCase() === wantedLower);
  if (hit) {
    if (hit.title !== desiredTitle) {
      console.log(`ℹ️ Gebruik bestaande tab "${hit.title}" (gevraagd "${desiredTitle}")`);
    } else {
      console.log(`ℹ️ Tab "${desiredTitle}" bestaat al`);
    }
    return { title: hit.title, sheetId: hit.id };
  }

  const resp = await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests: [{ addSheet: { properties: { title: desiredTitle } } }] },
  });
  const added = resp.data?.replies?.[0]?.addSheet?.properties;
  console.log(`➕ Tab "${desiredTitle}" aangemaakt`);
  return { title: desiredTitle, sheetId: added?.sheetId };
}

// Vergelijk eerste rij met gewenste headers (case-insensitive, trim). Let op: lengte moet kloppen.
function headerMatches(firstRow, expected) {
  if (!Array.isArray(firstRow)) return false;
  const norm = (s) => String(s ?? "").trim().toLowerCase();
  if (firstRow.length < expected.length) return false;
  for (let i = 0; i < expected.length; i++) {
    if (norm(firstRow[i]) !== norm(expected[i])) return false;
  }
  return true;
}

// Voeg (indien nodig) header bovenaan toe zonder data te verliezen
async function ensureHeaderAtTop(sheets, sheetTitle, sheetId, expectedHeader) {
  // Lees eerste rij (A1:Z1 is prima; A1:1:1 kan ook)
  const r = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetTitle}!1:1`,
  });
  const firstRow = r.data.values?.[0] || [];
  if (headerMatches(firstRow, expectedHeader)) {
    console.log(`✅ Header al aanwezig in "${sheetTitle}"`);
    return;
  }

  // Check of er data aanwezig is (meer dan 0 cellen met inhoud)
  const hasAnyData = firstRow.length > 0 && firstRow.some(c => String(c ?? "").trim() !== "");

  // Als er al data op rij 1 staat, eerst 1 lege rij bovenaan invoegen (row index 0)
  if (hasAnyData) {
    console.log(`↳ Invoegen lege rij bovenaan voor "${sheetTitle}" (InsertDimension)`);
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{
          insertDimension: {
            range: { sheetId, dimension: "ROWS", startIndex: 0, endIndex: 1 },
            inheritFromBefore: false,
          },
        }],
      },
    }); // InsertDimension: zie "Row & column operations" + Request ref. :contentReference[oaicite:1]{index=1}
  }

  // Schrijf headers in A1
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetTitle}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: [expectedHeader] },
  }); // values.update: zie Basic writing. :contentReference[oaicite:2]{index=2}

  console.log(`📝 Header toegevoegd aan "${sheetTitle}": ${expectedHeader.join(", ")}`);
}

(async () => {
  try {
    const sheets = await getSheetsClient();

    // RUNS
    {
      const { title, sheetId } = await ensureSheetCaseInsensitive(sheets, "RUNS");
      await ensureHeaderAtTop(sheets, title, sheetId, HEADERS.RUNS);
    }
    // LOG
    {
      const { title, sheetId } = await ensureSheetCaseInsensitive(sheets, "LOG");
      await ensureHeaderAtTop(sheets, title, sheetId, HEADERS.LOG);
    }
    // CHANGES
    {
      const { title, sheetId } = await ensureSheetCaseInsensitive(sheets, "CHANGES");
      await ensureHeaderAtTop(sheets, title, sheetId, HEADERS.CHANGES);
    }

    console.log("🎉 Klaar met headers plaatsen.");
  } catch (e) {
    console.error("🛑 Fout in header-script:", e?.response?.data || e.message || e);
    process.exit(1);
  }
})();
