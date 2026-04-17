import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const CNC_YEAR = parseInt(process.env.CNC_YEAR || "2026");
const UMBRELLA_SLUG = `city-nature-challenge-${CNC_YEAR}`;
const API_BASE = "https://api.inaturalist.org/v1";
const dryRun = process.argv.includes("--dry-run");

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_KEY.");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_KEY);

let lastRequestTime = 0;
async function rateLimitedFetch(url, retries = 3) {
  const elapsed = Date.now() - lastRequestTime;
  if (elapsed < 1100) await new Promise((r) => setTimeout(r, 1100 - elapsed));
  lastRequestTime = Date.now();

  let response;
  try {
    response = await fetch(url, { headers: { "User-Agent": "CNC-Umbrella-Sync/1.0" } });
  } catch (err) {
    if (retries > 0) {
      console.warn(`  Network error: ${err.message}. Retrying in 10s... (${retries} left)`);
      await new Promise((r) => setTimeout(r, 10000));
      lastRequestTime = Date.now();
      return rateLimitedFetch(url, retries - 1);
    }
    throw err;
  }

  if (response.status === 429) {
    console.warn("  Rate limited (429). Waiting 60s...");
    await new Promise((r) => setTimeout(r, 60000));
    lastRequestTime = Date.now();
    return rateLimitedFetch(url, retries);
  }

  if (!response.ok) throw new Error(`API error ${response.status}: ${url}`);
  return response.json();
}

async function main() {
  console.log(`\nCNC Umbrella Sync — ${UMBRELLA_SLUG}`);
  if (dryRun) console.log("*** DRY RUN ***");

  const slug = UMBRELLA_SLUG;
  const queries = [
    ["total_observations", `${API_BASE}/observations?project_id=${slug}&per_page=0`],
    ["total_species", `${API_BASE}/observations/species_counts?project_id=${slug}&per_page=0`],
    ["total_observers", `${API_BASE}/observations/observers?project_id=${slug}&per_page=0`],
    ["research_grade", `${API_BASE}/observations?project_id=${slug}&quality_grade=research&per_page=0`],
    ["needs_id", `${API_BASE}/observations?project_id=${slug}&quality_grade=needs_id&per_page=0`],
    ["total_identifiers", `${API_BASE}/observations/identifiers?project_id=${slug}&per_page=0`],
    ["threatened_species", `${API_BASE}/observations/species_counts?project_id=${slug}&threatened=true&quality_grade=research&per_page=0`],
    ["endemic_species", `${API_BASE}/observations/species_counts?project_id=${slug}&endemic=true&quality_grade=research&per_page=0`],
  ];

  const row = { year: CNC_YEAR, synced_at: new Date().toISOString() };
  for (const [key, url] of queries) {
    const data = await rateLimitedFetch(url);
    row[key] = data.total_results || 0;
    console.log(`  ${key}: ${row[key].toLocaleString()}`);
  }

  if (!dryRun) {
    const { error } = await db.from("cnc_umbrella_stats").upsert(row, { onConflict: "year" });
    if (error) throw new Error(`Supabase upsert cnc_umbrella_stats: ${error.message}`);
    console.log(`\nSaved umbrella stats for year ${CNC_YEAR}`);
  } else {
    console.log(`\n[DRY RUN] Would save umbrella stats for year ${CNC_YEAR}`);
  }
}

main().catch((err) => {
  console.error("\nFatal error:", err.message);
  process.exit(1);
});
