import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

// ─── Config ───
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const CNC_YEAR = parseInt(process.env.CNC_YEAR || "2025");
const UMBRELLA_SLUG = `city-nature-challenge-${CNC_YEAR}`;
const API_BASE = "https://api.inaturalist.org/v1";
const PER_PAGE = 200;
const PROJECT_BATCH_SIZE = 10;
const TAXA_BATCH_SIZE = 30;
const BATCH_LIMIT = 20;
const SPECIES_OR_BELOW = new Set(["species", "subspecies", "variety", "form", "hybrid", "infraspecies"]);

// CLI args
const args = process.argv.slice(2);
const singleProject = args.find((a) => a.startsWith("--project="))?.split("=")[1];
const projectsOnly = args.includes("--projects-only");
const statsOnly = args.includes("--stats-only");
const dryRun = args.includes("--dry-run");
const fullSync = args.includes("--full");
const updatedSinceOverride = args.find((a) => a.startsWith("--updated-since="))?.split("=")[1];
const refreshProjects = args.includes("--refresh-projects");
const onlyTaxa = args.includes("--only-taxa");
const onlyLocalCounts = args.includes("--only-local-counts");

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_KEY. Copy .env.example to .env and fill in values.");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_KEY);

// ─── Rate limiter ───
let lastRequestTime = 0;

async function rateLimitedFetch(url, retries = 3) {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < 1100) {
    await new Promise((r) => setTimeout(r, 1100 - elapsed));
  }
  lastRequestTime = Date.now();

  let response;
  try {
    response = await fetch(url, {
      headers: { "User-Agent": "CNC-Sync-Script/1.0" },
    });
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

  if (!response.ok) {
    throw new Error(`API error ${response.status}: ${url}`);
  }

  return response.json();
}

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

// ─── Step 1: Fetch all sub-project slugs ───
async function fetchProjects() {
  console.log(`\n=== Step 1: Fetching sub-projects from ${UMBRELLA_SLUG} ===\n`);

  const data = await rateLimitedFetch(`${API_BASE}/projects/${UMBRELLA_SLUG}`);
  const umbrella = data.results[0];
  const rules = umbrella.project_observation_rules || [];
  const projectIds = rules.map((r) => r.operand_id);

  console.log(`Found ${projectIds.length} sub-projects`);

  // Batch-fetch project details
  const projects = [];
  for (let i = 0; i < projectIds.length; i += PROJECT_BATCH_SIZE) {
    const batch = projectIds.slice(i, i + PROJECT_BATCH_SIZE);
    const batchData = await rateLimitedFetch(`${API_BASE}/projects/${batch.join(",")}`);
    for (const p of batchData.results) {
      // Extract place_ids from search_parameters
      const placeParam = (p.search_parameters || []).find((sp) => sp.field === "place_id");
      const placeIds = placeParam && Array.isArray(placeParam.value)
        ? placeParam.value
        : p.place_id ? [p.place_id] : [];

      projects.push({
        inat_project_id: p.id,
        slug: p.slug,
        title: p.title,
        icon_url: p.icon || null,
        year: CNC_YEAR,
        place_ids: placeIds,
      });
    }
    console.log(`  Fetched ${Math.min(i + PROJECT_BATCH_SIZE, projectIds.length)}/${projectIds.length} project details`);
  }

  // Get existing observation counts from Supabase (avoid 664 API calls)
  console.log(`\nLoading existing observation counts from Supabase...`);
  const existingCounts = {};
  const { data: existingProjects } = await db
    .from("cnc_projects")
    .select("slug, total_observations")
    .eq("year", CNC_YEAR);
  if (existingProjects) {
    existingProjects.forEach((p) => { existingCounts[p.slug] = p.total_observations || 0; });
  }

  // New projects get count 0 — actual counts will be populated during observation sync
  const newProjects = projects.filter((p) => existingCounts[p.slug] === undefined);
  if (newProjects.length > 0) {
    console.log(`${newProjects.length} new projects (counts will be set during sync)`);
  }

  projects.forEach((p) => { p.total_observations = existingCounts[p.slug] || 0; });

  // Sort by size ascending
  projects.sort((a, b) => a.total_observations - b.total_observations);

  if (!dryRun) {
    // Remove projects no longer in the umbrella before upserting
    const currentSlugs = new Set(projects.map((p) => p.slug));
    const staleSlugs = Object.keys(existingCounts).filter((s) => !currentSlugs.has(s));
    if (staleSlugs.length > 0) {
      const { error: delError } = await db
        .from("cnc_projects")
        .delete()
        .eq("year", CNC_YEAR)
        .in("slug", staleSlugs);
      if (delError) console.warn(`Warning: failed to remove stale projects: ${delError.message}`);
      else console.log(`Removed ${staleSlugs.length} stale projects: ${staleSlugs.join(", ")}`);
    }

    // Upsert to Supabase
    const { error } = await db.from("cnc_projects").upsert(projects, {
      onConflict: "slug",
    });
    if (error) throw new Error(`Supabase upsert cnc_projects: ${error.message}`);
    console.log(`\nSaved ${projects.length} projects to Supabase`);
  } else {
    console.log(`\n[DRY RUN] Would save ${projects.length} projects`);
    const total = projects.reduce((s, p) => s + p.total_observations, 0);
    console.log(`Total observations across all cities: ${total.toLocaleString()}`);
    console.log(`Smallest: ${projects[0].slug} (${projects[0].total_observations})`);
    console.log(`Largest: ${projects[projects.length - 1].slug} (${projects[projects.length - 1].total_observations})`);
  }

  return projects;
}

// ─── Step 2: Sync observations for a single project ───
function mapObservation(obs, projectSlug) {
  const photo =
    obs.photos && obs.photos[0] ? obs.photos[0].url.replace("square", "small") : null;
  return {
    inat_id: obs.id,
    project_slug: projectSlug,
    user_login: obs.user ? obs.user.login : null,
    user_icon: obs.user ? obs.user.icon : null,
    observed_on: obs.observed_on || null,
    quality_grade: obs.quality_grade || null,
    taxon_id: obs.taxon ? obs.taxon.id : null,
    taxon_name: obs.taxon ? obs.taxon.name : null,
    taxon_rank: obs.taxon ? obs.taxon.rank : null,
    min_species_taxon_id: obs.taxon ? obs.taxon.min_species_taxon_id : null,
    ancestor_ids: obs.taxon ? obs.taxon.ancestor_ids : null,
    common_name: obs.taxon ? obs.taxon.preferred_common_name : null,
    iconic_taxon_name: obs.taxon ? obs.taxon.iconic_taxon_name : null,
    photo_url: photo,
    comments_count: obs.comments_count || 0,
    created_at: obs.created_at || null,
  };
}

async function getSyncLog(projectSlug) {
  const { data, error } = await db
    .from("cnc_sync_log")
    .select("*")
    .eq("project_slug", projectSlug)
    .limit(1);

  if (error) {
    console.warn(`  Could not read sync log: ${error.message}`);
    return null;
  }
  return data.length > 0 ? data[0] : null;
}

async function updateSyncLog(projectSlug, updates) {
  await db.from("cnc_sync_log").upsert(
    { project_slug: projectSlug, ...updates },
    { onConflict: "project_slug" }
  );
}

async function fullSyncProject(projectSlug, resumeIdAbove = 0) {
  let idAbove = resumeIdAbove;
  let totalFetched = 0;
  const syncStartedAt = new Date().toISOString();

  if (resumeIdAbove > 0) {
    console.log(`  Resuming full sync from id_above=${resumeIdAbove}`);
  }

  await updateSyncLog(projectSlug, {
    status: "in_progress",
    started_at: new Date().toISOString(),
  });

  while (true) {
    const url = `${API_BASE}/observations?project_id=${projectSlug}&per_page=${PER_PAGE}&order_by=id&order=asc${idAbove ? `&id_above=${idAbove}` : ""}`;
    const data = await rateLimitedFetch(url);
    const results = data.results || [];

    if (results.length === 0) break;

    const rows = results.map((obs) => mapObservation(obs, projectSlug));

    if (!dryRun) {
      const { error } = await db.from("cnc_observations").upsert(rows, {
        onConflict: "inat_id",
      });
      if (error) throw new Error(`Supabase upsert: ${error.message}`);
    }

    totalFetched += results.length;
    idAbove = results[results.length - 1].id;

    if (!dryRun) {
      await updateSyncLog(projectSlug, {
        last_id_above: idAbove,
        total_fetched: totalFetched + (resumeIdAbove > 0 ? 1 : 0),
      });
    }

    process.stdout.write(`  ${totalFetched} obs fetched (id_above=${idAbove})\r`);

    if (results.length < PER_PAGE) break;
  }

  console.log(`  ${totalFetched} observations synced (full)`);

  if (!dryRun) {
    await updateSyncLog(projectSlug, {
      status: "done",
      total_fetched: totalFetched,
      completed_at: syncStartedAt,
    });
  }

  return totalFetched;
}

async function incrementalSyncProject(projectSlug, completedAt) {
  let totalFetched = 0;
  let idAbove = 0;
  const syncStartedAt = new Date().toISOString();

  console.log(`  Incremental sync since ${completedAt}`);

  while (true) {
    const url = `${API_BASE}/observations?project_id=${projectSlug}&per_page=${PER_PAGE}&order_by=id&order=asc${idAbove ? `&id_above=${idAbove}` : ""}&updated_since=${encodeURIComponent(completedAt)}`;
    const data = await rateLimitedFetch(url);
    const results = data.results || [];

    if (results.length === 0) break;

    const rows = results.map((obs) => mapObservation(obs, projectSlug));

    if (!dryRun) {
      const { error } = await db.from("cnc_observations").upsert(rows, {
        onConflict: "inat_id",
      });
      if (error) throw new Error(`Supabase upsert: ${error.message}`);
    }

    totalFetched += results.length;
    idAbove = results[results.length - 1].id;
    process.stdout.write(`  ${totalFetched} obs updated (id_above=${idAbove})\r`);

    if (results.length < PER_PAGE) break;
  }

  console.log(`  ${totalFetched} observations updated (incremental)`);

  if (!dryRun && totalFetched >= 0) {
    await updateSyncLog(projectSlug, {
      completed_at: syncStartedAt,
    });
  }

  return totalFetched;
}

async function syncProject(projectSlug, index, total) {
  const prefix = `[${index}/${total}] ${projectSlug}`;
  console.log(`\n${prefix}`);

  if (dryRun) {
    console.log(`  [DRY RUN] Would sync observations`);
    return 0;
  }

  try {
    if (fullSync) {
      return await fullSyncProject(projectSlug);
    }

    const log = await getSyncLog(projectSlug);

    if (!log || log.status === "pending") {
      return await fullSyncProject(projectSlug);
    } else if (log.status === "in_progress") {
      return await fullSyncProject(projectSlug, log.last_id_above || 0);
    } else if (log.status === "done" && (updatedSinceOverride || log.completed_at)) {
      return await incrementalSyncProject(projectSlug, updatedSinceOverride || log.completed_at);
    } else if (log.status === "done") {
      return await fullSyncProject(projectSlug);
    }

    return 0;
  } catch (err) {
    console.warn(`  Skipping ${projectSlug}: ${err.message}`);
    return 0;
  }
}


// ─── Step 3: Sync taxa data to cnc_taxa ───
async function syncTaxa(projects) {
  console.log(`\n=== Step 3: Syncing taxa data ===\n`);

  // Collect all unique species IDs across synced projects
  const allSpeciesIds = new Set();
  for (let pi = 0; pi < projects.length; pi++) {
    const project = projects[pi];
    console.log(`  [${pi + 1}/${projects.length}] Loading observations for ${project.slug}...`);
    const obs = await fetchAllObservations(project.slug);
    const leafSpecies = computeLeafSpecies(obs);
    leafSpecies.forEach((id) => allSpeciesIds.add(id));
    console.log(`  [${pi + 1}/${projects.length}] ${project.slug}: ${obs.length} obs, ${leafSpecies.size} leaf species`);
  }

  const speciesIds = [...allSpeciesIds];
  console.log(`\n  Total unique species: ${speciesIds.length}`);

  // Check which are already in cnc_taxa
  console.log(`  Checking cached taxa...`);
  const existingIds = new Set();
  for (let i = 0; i < speciesIds.length; i += 1000) {
    const batch = speciesIds.slice(i, i + 1000);
    const { data } = await db
      .from("cnc_taxa")
      .select("taxon_id")
      .in("taxon_id", batch);
    if (data) data.forEach((r) => existingIds.add(r.taxon_id));
  }

  const missingIds = speciesIds.filter((id) => !existingIds.has(id));
  console.log(`  Already cached: ${existingIds.size}, need to fetch: ${missingIds.length}`);

  // Batch-fetch from iNat API
  const now = new Date().toISOString();
  let fetched = 0;

  for (let i = 0; i < missingIds.length; i += TAXA_BATCH_SIZE) {
    const batch = missingIds.slice(i, i + TAXA_BATCH_SIZE);
    console.log(`  Fetching new taxa ${Math.min(i + TAXA_BATCH_SIZE, missingIds.length)}/${missingIds.length}...`);
    const data = await rateLimitedFetch(`${API_BASE}/taxa/${batch.join(",")}`);

    if (data.results) {
      const rows = data.results.map((t) => ({
        taxon_id: t.id,
        observations_count: t.observations_count || 0,
        synced_at: now,
      }));

      if (!dryRun && rows.length > 0) {
        const { error } = await db.from("cnc_taxa").upsert(rows, { onConflict: "taxon_id" });
        if (error) console.warn(`  Taxa upsert error: ${error.message}`);
      }

      fetched += rows.length;
    }
  }

  console.log(`\n  New taxa cached: ${fetched}`);

  // Refresh stale taxa (>7 days old OR observations_count=0) to detect taxon swaps and fix bad data
  const staleDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const staleIdSet = new Set();
  for (let i = 0; i < speciesIds.length; i += 1000) {
    const batch = speciesIds.slice(i, i + 1000);
    // Stale by date
    const { data: staleByDate } = await db
      .from("cnc_taxa")
      .select("taxon_id")
      .in("taxon_id", batch)
      .lt("synced_at", staleDate);
    if (staleByDate) staleByDate.forEach((r) => staleIdSet.add(r.taxon_id));
    // Stale by zero count (likely bad data)
    const { data: zeroCount } = await db
      .from("cnc_taxa")
      .select("taxon_id")
      .in("taxon_id", batch)
      .eq("observations_count", 0);
    if (zeroCount) zeroCount.forEach((r) => staleIdSet.add(r.taxon_id));
  }
  const staleIds = [...staleIdSet];

  if (staleIds.length > 0) {
    console.log(`  Refreshing ${staleIds.length} stale taxa (>7 days old)...`);
    let refreshed = 0;
    let swapped = 0;

    for (let i = 0; i < staleIds.length; i += TAXA_BATCH_SIZE) {
      const batch = staleIds.slice(i, i + TAXA_BATCH_SIZE);
      const progress = Math.min(i + TAXA_BATCH_SIZE, staleIds.length);
      console.log(`  Refreshing ${progress}/${staleIds.length} taxa...`);
      const data = await rateLimitedFetch(`${API_BASE}/taxa/${batch.join(",")}`);
      const returnedIds = new Set();

      if (data.results) {
        for (const t of data.results) {
          returnedIds.add(t.id);

          if (t.is_active === false && t.current_synonymous_taxon_id) {
            // Taxon was swapped — fetch the new taxon
            const newId = t.current_synonymous_taxon_id;
            console.log(`  Taxon swap: ${t.name} (${t.id}) → ${newId}`);
            try {
              const newData = await rateLimitedFetch(`${API_BASE}/taxa/${newId}`);
              const newTaxon = newData.results && newData.results[0];
              if (newTaxon) {
                // Update observations referencing the old taxon
                if (!dryRun) {
                  await db.from("cnc_observations")
                    .update({
                      min_species_taxon_id: newTaxon.id,
                      taxon_id: newTaxon.id,
                      taxon_name: newTaxon.name,
                      common_name: newTaxon.preferred_common_name || null,
                    })
                    .eq("min_species_taxon_id", t.id);

                  // Replace old taxa entry with new one
                  await db.from("cnc_taxa").delete().eq("taxon_id", t.id);
                  await db.from("cnc_taxa").upsert({
                    taxon_id: newTaxon.id,
                    observations_count: newTaxon.observations_count || 0,
                    synced_at: now,
                  }, { onConflict: "taxon_id" });
                }
                swapped++;
              }
            } catch (e) {
              console.warn(`  Failed to resolve swap for ${t.id}: ${e.message}`);
            }
          } else {
            // Active taxon — just refresh counts
            if (!dryRun) {
              await db.from("cnc_taxa").upsert({
                taxon_id: t.id,
                observations_count: t.observations_count || 0,
                synced_at: now,
              }, { onConflict: "taxon_id" });
            }
            refreshed++;
          }
        }
      }

      // Log taxa that weren't returned (fully deleted)
      for (const id of batch) {
        if (!returnedIds.has(id)) {
          console.warn(`  Taxon ${id} not found on iNat (may be deleted)`);
        }
      }
    }

    console.log(`  Refreshed: ${refreshed}, swapped: ${swapped}`);
  }

  console.log(`\n=== Taxa sync complete ===`);
}

// ─── Step 4: Sync local counts to cnc_project_species ───
async function syncLocalCountsAndEndemics(projects) {
  console.log(`\n=== Step 4: Syncing local counts ===\n`);

  for (let pi = 0; pi < projects.length; pi++) {
    const project = projects[pi];
    const prefix = `[${pi + 1}/${projects.length}] ${project.slug}`;

    // Load place_ids from DB if not on project object
    let placeIds = project.place_ids;
    if (!placeIds || placeIds.length === 0) {
      const { data } = await db
        .from("cnc_projects")
        .select("place_ids")
        .eq("slug", project.slug)
        .single();
      placeIds = data?.place_ids || [];
    }

    if (placeIds.length === 0) {
      console.log(`${prefix}: no place_ids, skipping`);
      continue;
    }

    // Get leaf species for this project
    const obs = await fetchAllObservations(project.slug);
    if (obs.length === 0) {
      console.log(`${prefix}: no observations, skipping`);
      continue;
    }

    const leafSpecies = computeLeafSpecies(obs);
    const speciesIds = [...leafSpecies];
    const now = new Date().toISOString();

    // Filter to species rank or below for local counts
    const speciesRankIds = new Set();
    obs.forEach((o) => {
      if (o.min_species_taxon_id && SPECIES_OR_BELOW.has(o.taxon_rank)) {
        speciesRankIds.add(o.min_species_taxon_id);
      }
    });
    const localSpeciesIds = speciesIds.filter((id) => speciesRankIds.has(id));

    // Batch-fetch local counts
    const localCounts = {};
    console.log(`${prefix}: fetching local counts for ${localSpeciesIds.length} species...`);
    for (let i = 0; i < localSpeciesIds.length; i += TAXA_BATCH_SIZE) {
      const batch = localSpeciesIds.slice(i, i + TAXA_BATCH_SIZE);
      const data = await rateLimitedFetch(
        `${API_BASE}/observations/species_counts?place_id=${placeIds.join(",")}&taxon_id=${batch.join(",")}&per_page=500`
      );
      if (data.results) {
        data.results.forEach((r) => {
          localCounts[r.taxon.id] = r.count || 0;
        });
      }
      batch.forEach((id) => {
        if (localCounts[id] === undefined) localCounts[id] = 0;
      });
      console.log(`${prefix}: local counts ${Math.min(i + TAXA_BATCH_SIZE, localSpeciesIds.length)}/${localSpeciesIds.length}`);
    }

    // Build rows for cnc_project_species
    const rows = speciesIds.map((id) => ({
      project_slug: project.slug,
      taxon_id: id,
      local_obs_count: localCounts[id] ?? null,
      synced_at: now,
    }));

    // Upsert in batches
    if (!dryRun && rows.length > 0) {
      for (let i = 0; i < rows.length; i += 500) {
        const batch = rows.slice(i, i + 500);
        console.log(`${prefix}: saving species ${Math.min(i + 500, rows.length)}/${rows.length}...`);
        const { error } = await db.from("cnc_project_species").upsert(batch, {
          onConflict: "project_slug,taxon_id",
        });
        if (error) console.warn(`  ${prefix}: project_species upsert error: ${error.message}`);
      }
    }

    console.log(`${prefix}: done — ${localSpeciesIds.length} local counts saved`);
  }

  console.log(`\n=== Local counts sync complete ===`);
}



// ─── Main ───
async function main() {
  const syncStartTime = new Date().toISOString();
  console.log(`\nCNC Sync — ${UMBRELLA_SLUG}`);
  console.log(`Supabase: ${SUPABASE_URL}`);
  if (dryRun) console.log("*** DRY RUN — no writes to Supabase ***");
  if (singleProject) console.log(`Single project: ${singleProject}`);
  console.log("");

  // Step 1: Seed project list from iNat (only if Supabase is empty or --refresh-projects)
  if (!singleProject) {
    const { count } = await db
      .from("cnc_projects")
      .select("*", { count: "exact", head: true })
      .eq("year", CNC_YEAR);

    if (refreshProjects || !count || count === 0) {
      console.log(count === 0 ? `No ${CNC_YEAR} projects in Supabase, seeding from iNat...` : "Refreshing project list from iNat...");
      await fetchProjects();
    } else {
      console.log(`Using ${count} projects from Supabase (year ${CNC_YEAR})`);
    }
  }

  if (projectsOnly) {
    console.log("\n--projects-only flag set. Done.");
    return;
  }

  // Select which projects to process this run
  let projects;
  if (singleProject) {
    projects = [{ slug: singleProject }];
  } else {
    // Pick the BATCH_LIMIT projects with the oldest synced_at (nulls first = never synced)
    const { data, error } = await db
      .from("cnc_projects")
      .select("slug, place_ids, total_observations, synced_at")
      .eq("year", CNC_YEAR)
      .order("synced_at", { ascending: true, nullsFirst: true })
      .limit(BATCH_LIMIT);
    if (error) throw new Error(`Fetch projects batch: ${error.message}`);
    projects = data;
    console.log(`\nSelected ${projects.length} projects to sync (oldest synced_at first)`);
    if (projects.length > 0) {
      const oldest = projects[0].synced_at || "never";
      const newest = projects[projects.length - 1].synced_at || "never";
      console.log(`  Range: ${oldest} → ${newest}`);
    }
  }

  // Handle targeted runs (--only-* flags)
  if (onlyTaxa || onlyLocalCounts) {
    if (onlyTaxa) await syncTaxa(projects);
    if (onlyLocalCounts) await syncLocalCountsAndEndemics(projects);

    if (!dryRun) {
      for (const p of projects) {
        await db.from("cnc_projects").update({ synced_at: syncStartTime }).eq("slug", p.slug);
      }
    }
    console.log("\nDone!");
    return;
  }

  // Step 2: Sync observations
  if (!statsOnly) {
    console.log(`\n=== Step 2: Syncing observations for ${projects.length} projects ===`);
    let totalSynced = 0;
    for (let i = 0; i < projects.length; i++) {
      const count = await syncProject(projects[i].slug, i + 1, projects.length);
      totalSynced += count;
    }
    console.log(`\n=== Observations sync complete: ${totalSynced.toLocaleString()} total ===`);
  }

  // Step 3: Sync taxa
  await syncTaxa(projects);

  // Step 4: Sync local counts
  await syncLocalCountsAndEndemics(projects);

  // Update synced_at for all processed projects
  if (!dryRun) {
    for (const p of projects) {
      await db.from("cnc_projects").update({ synced_at: syncStartTime }).eq("slug", p.slug);
    }
  }

  console.log("\nDone!");
}

main().catch((err) => {
  console.error("\nFatal error:", err.message);
  process.exit(1);
});
