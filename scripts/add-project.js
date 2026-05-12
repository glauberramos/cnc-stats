import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const CNC_YEAR = parseInt(process.env.CNC_YEAR || "2026");
const API_BASE = "https://api.inaturalist.org/v1";

const slug = process.argv[2];
if (!slug) {
  console.error("Usage: node add-project.js <project-slug>");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_KEY in .env");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_KEY);

const res = await fetch(`${API_BASE}/projects/${slug}`, {
  headers: { "User-Agent": "CNC-Sync-Script/1.0" },
});
if (!res.ok) throw new Error(`iNat API ${res.status}`);
const data = await res.json();
const p = data.results[0];
if (!p) throw new Error(`Project not found: ${slug}`);

const placeParam = (p.search_parameters || []).find((sp) => sp.field === "place_id");
const placeIds = placeParam && Array.isArray(placeParam.value)
  ? placeParam.value
  : placeParam?.value ? [placeParam.value] : p.place_id ? [p.place_id] : [];

const row = {
  inat_project_id: p.id,
  slug: p.slug,
  title: p.title,
  icon_url: p.icon || null,
  year: CNC_YEAR,
  place_ids: placeIds,
  total_observations: 0,
};

const { error } = await db.from("cnc_projects").upsert(row, { onConflict: "slug" });
if (error) throw new Error(`Supabase upsert: ${error.message}`);

console.log(`Added project to year ${CNC_YEAR}:`);
console.log(`  ${p.title} (${p.slug})`);
console.log(`  iNat id: ${p.id}, place_ids: ${JSON.stringify(placeIds)}`);
console.log(`\nNext: load observations with`);
console.log(`  CNC_YEAR=${CNC_YEAR} node cnc-sync.js --project=${p.slug}`);
