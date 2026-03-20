import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

// ─── Config ───
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

// CLI args
const args = process.argv.slice(2);
const singleProject = args.find((a) => a.startsWith("--project="))?.split("=")[1];
const dryRun = args.includes("--dry-run");
const forceConsolidate = args.includes("--force");

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_KEY.");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_KEY);

// ─── Leaf species algorithm ───
function computeLeafSpecies(obs) {
  const allMinSpecies = new Set();
  const allAncestors = new Set();
  obs.forEach((o) => {
    if (o.min_species_taxon_id) allMinSpecies.add(o.min_species_taxon_id);
    if (o.ancestor_ids) {
      const ids = Array.isArray(o.ancestor_ids) ? o.ancestor_ids : JSON.parse(o.ancestor_ids);
      ids.forEach((id) => {
        if (id !== o.taxon_id && id !== o.min_species_taxon_id) {
          allAncestors.add(id);
        }
      });
    }
  });
  const leafSpecies = new Set();
  allMinSpecies.forEach((id) => {
    if (!allAncestors.has(id)) leafSpecies.add(id);
  });
  return leafSpecies;
}

// ─── Fetch all observations for a project from Supabase ───
async function fetchAllObservations(slug) {
  let allObs = [];
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data, error } = await db
      .from("cnc_observations")
      .select("*")
      .eq("project_slug", slug)
      .range(from, from + pageSize - 1);

    if (error) {
      console.error(`  Supabase error: ${error.message}`);
      break;
    }
    if (!data || data.length === 0) break;
    allObs = allObs.concat(data);
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return allObs;
}

// ─── Fetch taxa data from cnc_taxa ───
async function fetchTaxaMap(speciesIds) {
  const taxaMap = {};
  for (let i = 0; i < speciesIds.length; i += 1000) {
    const batch = speciesIds.slice(i, i + 1000);
    const { data, error } = await db
      .from("cnc_taxa")
      .select("*")
      .in("taxon_id", batch);
    if (error) {
      console.warn(`  Taxa fetch error: ${error.message}`);
      continue;
    }
    if (data) {
      data.forEach((t) => { taxaMap[t.taxon_id] = t; });
    }
  }
  return taxaMap;
}

// ─── Fetch project species data from cnc_project_species ───
async function fetchProjectSpeciesMap(slug) {
  const map = {};
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data, error } = await db
      .from("cnc_project_species")
      .select("*")
      .eq("project_slug", slug)
      .range(from, from + pageSize - 1);

    if (error) {
      console.warn(`  Project species fetch error: ${error.message}`);
      break;
    }
    if (!data || data.length === 0) break;
    data.forEach((r) => { map[r.taxon_id] = r; });
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return map;
}

// ─── Compute all metrics ───
function computeStats(allObs) {
  const leafSpecies = computeLeafSpecies(allObs);

  // --- Summary ---
  const observersSet = new Set();
  let rg = 0, needsId = 0, casual = 0;

  allObs.forEach((o) => {
    if (o.user_login) observersSet.add(o.user_login);
    if (o.quality_grade === "research") rg++;
    else if (o.quality_grade === "needs_id") needsId++;
    else casual++;
  });

  const summary = {
    total_observations: allObs.length,
    total_species: leafSpecies.size,
    total_observers: observersSet.size,
    research_grade: rg,
    needs_id: needsId,
    casual: casual,
  };

  // --- Most Commented ---
  const most_commented = [...allObs]
    .filter((o) => (o.comments_count || 0) > 0)
    .sort((a, b) => (b.comments_count || 0) - (a.comments_count || 0))
    .slice(0, 10)
    .map((o) => ({
      inat_id: o.inat_id,
      common_name: o.common_name,
      taxon_name: o.taxon_name,
      photo_url: o.photo_url,
      user_login: o.user_login,
      comments_count: o.comments_count || 0,
    }));

  return {
    summary,
    most_commented,
  };
}

// ─── Consolidate a single project ───
async function consolidateProject(slug, index, total) {
  const prefix = `[${index}/${total}] ${slug}`;
  console.log(`${prefix}: loading observations...`);

  const allObs = await fetchAllObservations(slug);
  if (allObs.length === 0) {
    console.log(`${prefix}: no observations, skipping`);
    return;
  }

  console.log(`${prefix}: computing stats for ${allObs.length} obs...`);
  const stats = computeStats(allObs);

  // --- Load taxa data from cnc_taxa ---
  const leafSpecies = computeLeafSpecies(allObs);
  const speciesIds = [...leafSpecies];

  // Build project obs counts per species
  const projectObsCount = {};
  const speciesInfo = {};
  const speciesHasRG = {};
  allObs.forEach((o) => {
    if (!o.min_species_taxon_id || !leafSpecies.has(o.min_species_taxon_id)) return;
    const tid = o.min_species_taxon_id;
    projectObsCount[tid] = (projectObsCount[tid] || 0) + 1;
    if (o.quality_grade === "research") speciesHasRG[tid] = true;
    if (!speciesInfo[tid] || o.taxon_rank === "species") {
      speciesInfo[tid] = {
        taxon_name: o.taxon_name, common_name: o.common_name, photo_url: o.photo_url,
      };
    }
  });

  console.log(`${prefix}: loading taxa & project species data...`);
  const taxaMap = await fetchTaxaMap(speciesIds);
  const projectSpeciesMap = await fetchProjectSpeciesMap(slug);

  // --- First observations ---
  const globalCounts = {};
  const localCounts = {};
  speciesIds.forEach((id) => {
    if (taxaMap[id]) globalCounts[id] = taxaMap[id].observations_count || 0;
    if (projectSpeciesMap[id]) localCounts[id] = projectSpeciesMap[id].local_obs_count;
  });

  const firstGlobalIds = new Set(
    speciesIds.filter((id) => globalCounts[id] !== undefined && (globalCounts[id] || 0) <= (projectObsCount[id] || 0))
  );
  const firstLocalIds = new Set(
    speciesIds.filter((id) => localCounts[id] !== undefined && localCounts[id] !== null && (localCounts[id] || 0) <= (projectObsCount[id] || 0))
  );

  const allFirstIds = new Set([...firstGlobalIds, ...firstLocalIds]);
  stats.first_observations = [...allFirstIds]
    .map((id) => ({
      taxon_id: id,
      taxon_name: speciesInfo[id]?.taxon_name || "Unknown",
      common_name: speciesInfo[id]?.common_name || null,
      photo_url: speciesInfo[id]?.photo_url || null,
      global_obs_count: globalCounts[id] || 0,
      local_obs_count: localCounts[id] ?? null,
      project_obs_count: projectObsCount[id] || 0,
      first_global_obs: firstGlobalIds.has(id),
      first_local_obs: firstLocalIds.has(id),
      research_grade: !!speciesHasRG[id],
    }))
    .sort((a, b) => {
      if (a.first_global_obs !== b.first_global_obs) return a.first_global_obs ? -1 : 1;
      return (a.global_obs_count || 0) - (b.global_obs_count || 0);
    });

  // --- Write computed stats ---
  if (!dryRun) {
    const { error } = await db
      .from("cnc_projects")
      .update({
        computed_stats: stats,
        computed_at: new Date().toISOString(),
        total_observations: stats.summary.total_observations,
      })
      .eq("slug", slug);

    if (error) {
      console.error(`${prefix}: update error: ${error.message}`);
      return;
    }
  }

  console.log(`${prefix}: ${allObs.length} obs -> ${stats.summary.total_species} species, ${stats.summary.total_observers} observers ${dryRun ? "[DRY RUN]" : ""}`);
}

// ─── Main ───
async function main() {
  console.log("\nCNC Consolidate");
  console.log(`Supabase: ${SUPABASE_URL}`);
  if (dryRun) console.log("*** DRY RUN ***");

  let projects;
  if (singleProject) {
    projects = [{ slug: singleProject }];
  } else {
    projects = [];
    let from = 0;
    const pageSize = 1000;
    while (true) {
      const { data, error } = await db
        .from("cnc_projects")
        .select("slug, total_observations, synced_at, computed_at")
        .order("total_observations", { ascending: false, nullsFirst: false })
        .range(from, from + pageSize - 1);

      if (error) throw new Error(`Fetch projects: ${error.message}`);
      if (!data || data.length === 0) break;
      projects = projects.concat(data);
      if (data.length < pageSize) break;
      from += pageSize;
    }
  }

  console.log(`\nConsolidating ${projects.length} projects...\n`);

  let consolidated = 0;
  for (let i = 0; i < projects.length; i++) {
    const project = projects[i];
    const prefix = `[${i + 1}/${projects.length}] ${project.slug}`;

    // Skip if already up to date (unless --force or single project)
    if (!singleProject && !forceConsolidate) {
      if (project.computed_at && project.synced_at &&
          new Date(project.computed_at) >= new Date(project.synced_at)) {
        console.log(`${prefix}: up to date, skipping`);
        continue;
      }
    }

    await consolidateProject(project.slug, i + 1, projects.length);
    consolidated++;
  }

  console.log(`\nDone! Consolidated ${consolidated} projects.`);
}

main().catch((err) => {
  console.error("\nFatal error:", err.message);
  process.exit(1);
});
