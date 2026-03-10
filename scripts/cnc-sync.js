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
const skipProjects = args.includes("--skip-projects");
const onlyTaxa = args.includes("--only-taxa");
const onlyLocalCounts = args.includes("--only-local-counts");
const onlyIdentifications = args.includes("--only-identifications");

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_KEY. Copy .env.example to .env and fill in values.");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_KEY);

// ─── Rate limiter ───
let lastRequestTime = 0;

async function rateLimitedFetch(url) {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < 1000) {
    await new Promise((r) => setTimeout(r, 1000 - elapsed));
  }
  lastRequestTime = Date.now();

  const response = await fetch(url, {
    headers: { "User-Agent": "CNC-Sync-Script/1.0" },
  });

  if (response.status === 429) {
    console.warn("  Rate limited (429). Waiting 60s...");
    await new Promise((r) => setTimeout(r, 60000));
    lastRequestTime = Date.now();
    return rateLimitedFetch(url);
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

  // Get observation count for each project (to sort by size)
  console.log(`\nFetching observation counts...`);
  for (let i = 0; i < projects.length; i++) {
    const p = projects[i];
    const countData = await rateLimitedFetch(
      `${API_BASE}/observations?project_id=${p.slug}&per_page=1`
    );
    p.total_observations = countData.total_results || 0;
    if ((i + 1) % 50 === 0 || i === projects.length - 1) {
      console.log(`  Counted ${i + 1}/${projects.length} projects`);
    }
  }

  // Sort by size ascending
  projects.sort((a, b) => a.total_observations - b.total_observations);

  if (!dryRun) {
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
    faves_count: obs.faves_count || 0,
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

  if (fullSync) {
    return fullSyncProject(projectSlug);
  }

  const log = await getSyncLog(projectSlug);

  if (!log || log.status === "pending") {
    return fullSyncProject(projectSlug);
  } else if (log.status === "in_progress") {
    return fullSyncProject(projectSlug, log.last_id_above || 0);
  } else if (log.status === "done" && (updatedSinceOverride || log.completed_at)) {
    return incrementalSyncProject(projectSlug, updatedSinceOverride || log.completed_at);
  } else if (log.status === "done") {
    return fullSyncProject(projectSlug);
  }

  return 0;
}

// ─── Step 3: Fetch aggregate stats per project ───
async function syncProjectStats(project, index, total) {
  const prefix = `[${index}/${total}] ${project.slug}`;
  process.stdout.write(`${prefix}: fetching stats...\r`);

  const [observersData, speciesData] = await Promise.all([
    rateLimitedFetch(
      `${API_BASE}/observations/observers?project_id=${project.slug}&per_page=10`
    ),
    rateLimitedFetch(
      `${API_BASE}/observations/species_counts?project_id=${project.slug}&per_page=10&rank=species`
    ),
  ]);

  await new Promise((r) => setTimeout(r, 1000));

  const topObservers = (observersData.results || []).map((r) => ({
    login: r.user.login,
    icon: r.user.icon,
    observation_count: r.observation_count,
    species_count: r.species_count,
  }));

  const topSpecies = (speciesData.results || []).map((r) => ({
    taxon_id: r.taxon.id,
    name: r.taxon.name,
    common_name: r.taxon.preferred_common_name,
    iconic_taxon_name: r.taxon.iconic_taxon_name,
    photo_url: r.taxon.default_photo ? r.taxon.default_photo.square_url : null,
    count: r.count,
  }));

  if (!dryRun) {
    const { error } = await db
      .from("cnc_projects")
      .update({
        top_observers: topObservers,
        top_species: topSpecies,
      })
      .eq("slug", project.slug);

    if (error) console.warn(`  Stats update error for ${project.slug}: ${error.message}`);
  }

  console.log(`${prefix}: ${topObservers.length} observers, ${topSpecies.length} species`);
}

// ─── Step 4: Sync taxa data to cnc_taxa ───
async function syncTaxa(projects) {
  console.log(`\n=== Step 4: Syncing taxa data ===\n`);

  // Collect all unique species IDs across synced projects
  const allSpeciesIds = new Set();
  for (const project of projects) {
    const obs = await fetchAllObservations(project.slug);
    const leafSpecies = computeLeafSpecies(obs);
    leafSpecies.forEach((id) => allSpeciesIds.add(id));
    console.log(`  ${project.slug}: ${leafSpecies.size} leaf species`);
  }

  const speciesIds = [...allSpeciesIds];
  console.log(`\n  Total unique species: ${speciesIds.length}`);

  // Check which are already in cnc_taxa
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
  const iucnNumToCode = { 50: "CR", 40: "EN", 30: "VU", 20: "NT", 10: "LC" };
  const now = new Date().toISOString();
  let fetched = 0;

  for (let i = 0; i < missingIds.length; i += TAXA_BATCH_SIZE) {
    const batch = missingIds.slice(i, i + TAXA_BATCH_SIZE);
    const data = await rateLimitedFetch(`${API_BASE}/taxa/${batch.join(",")}`);

    if (data.results) {
      const rows = data.results.map((t) => {
        let status = null;
        let statusName = null;
        if (t.conservation_status && t.conservation_status.status) {
          status = t.conservation_status.status.toUpperCase();
          statusName = t.conservation_status.status_name || t.conservation_status.status;
        }
        return {
          taxon_id: t.id,
          observations_count: t.observations_count || 0,
          conservation_status: status,
          conservation_status_name: statusName,
          synced_at: now,
        };
      });

      if (!dryRun && rows.length > 0) {
        const { error } = await db.from("cnc_taxa").upsert(rows, { onConflict: "taxon_id" });
        if (error) console.warn(`  Taxa upsert error: ${error.message}`);
      }

      fetched += rows.length;
    }

    if ((i + TAXA_BATCH_SIZE) % 300 === 0 || i + TAXA_BATCH_SIZE >= missingIds.length) {
      console.log(`  Fetched ${Math.min(i + TAXA_BATCH_SIZE, missingIds.length)}/${missingIds.length} taxa`);
    }
  }

  console.log(`\n=== Taxa sync complete: ${fetched} new taxa cached ===`);
}

// ─── Step 5: Sync local counts + endemic species to cnc_project_species ───
async function syncLocalCountsAndEndemics(projects) {
  console.log(`\n=== Step 5: Syncing local counts & endemic species ===\n`);

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
    }

    // Fetch endemic species
    const endemicSet = new Set();
    let endemicPage = 1;
    let endemicTotal = 0;
    do {
      const data = await rateLimitedFetch(
        `${API_BASE}/observations/species_counts?project_id=${project.slug}&endemic=true&per_page=500&page=${endemicPage}`
      );
      if (data.results) {
        data.results.forEach((r) => {
          if (r.taxon && leafSpecies.has(r.taxon.id)) {
            endemicSet.add(r.taxon.id);
          }
        });
      }
      endemicTotal = data.total_results || 0;
      endemicPage++;
    } while ((endemicPage - 1) * 500 < endemicTotal);

    // Build rows for cnc_project_species
    const rows = speciesIds.map((id) => ({
      project_slug: project.slug,
      taxon_id: id,
      local_obs_count: localCounts[id] ?? null,
      is_endemic: endemicSet.has(id),
      synced_at: now,
    }));

    // Upsert in batches
    if (!dryRun && rows.length > 0) {
      for (let i = 0; i < rows.length; i += 500) {
        const batch = rows.slice(i, i + 500);
        const { error } = await db.from("cnc_project_species").upsert(batch, {
          onConflict: "project_slug,taxon_id",
        });
        if (error) console.warn(`  ${prefix}: project_species upsert error: ${error.message}`);
      }
    }

    console.log(`${prefix}: ${localSpeciesIds.length} local counts, ${endemicSet.size} endemic species`);
  }

  console.log(`\n=== Local counts & endemic sync complete ===`);
}

// ─── Step 6: Sync identification stats ───
async function syncIdentificationStats(projects) {
  console.log(`\n=== Step 6: Syncing identification stats ===\n`);

  for (let i = 0; i < projects.length; i++) {
    const project = projects[i];
    const prefix = `[${i + 1}/${projects.length}] ${project.slug}`;

    try {
      const [catData, identifiersData] = await Promise.all([
        rateLimitedFetch(`${API_BASE}/observations/identification_categories?project_id=${project.slug}`),
        rateLimitedFetch(`${API_BASE}/observations/identifiers?project_id=${project.slug}&per_page=10`),
      ]);

      await new Promise((r) => setTimeout(r, 1000));

      const idCounts = { improving: 0, supporting: 0, leading: 0, maverick: 0 };
      let totalIds = 0;
      for (const result of catData.results || []) {
        if (result.category in idCounts) {
          idCounts[result.category] = result.count || 0;
          totalIds += idCounts[result.category];
        }
      }

      const topIdentifiers = (identifiersData.results || []).map((r) => ({
        login: r.user?.login || "?",
        icon: r.user?.icon_url || r.user?.icon || "",
        count: r.count || 0,
      }));

      const identificationStats = {
        total: totalIds,
        total_identifiers: identifiersData.total_results || 0,
        top_identifiers: topIdentifiers,
        ...idCounts,
      };

      if (!dryRun) {
        const { error } = await db
          .from("cnc_projects")
          .update({ identification_stats: identificationStats })
          .eq("slug", project.slug);
        if (error) console.warn(`  ${prefix}: update error: ${error.message}`);
      }

      console.log(`${prefix}: ${totalIds} IDs, ${topIdentifiers.length} top identifiers`);
    } catch (err) {
      console.warn(`${prefix}: identification stats failed: ${err.message}`);
    }
  }

  console.log(`\n=== Identification stats sync complete ===`);
}

// ─── Main ───
async function main() {
  const syncStartTime = new Date().toISOString();
  console.log(`\nCNC Sync — ${UMBRELLA_SLUG}`);
  console.log(`Supabase: ${SUPABASE_URL}`);
  if (dryRun) console.log("*** DRY RUN — no writes to Supabase ***");
  if (singleProject) console.log(`Single project: ${singleProject}`);
  console.log("");

  // Step 1: Fetch/update full project list from iNat
  if (!singleProject && !skipProjects) {
    await fetchProjects();
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
  if (onlyTaxa || onlyLocalCounts || onlyIdentifications) {
    if (onlyTaxa) await syncTaxa(projects);
    if (onlyLocalCounts) await syncLocalCountsAndEndemics(projects);
    if (onlyIdentifications) await syncIdentificationStats(projects);

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

  // Step 3: Fetch aggregate stats
  console.log(`\n=== Step 3: Fetching aggregate stats for ${projects.length} projects ===`);
  for (let i = 0; i < projects.length; i++) {
    await syncProjectStats(projects[i], i + 1, projects.length);
  }
  console.log(`\n=== Stats sync complete ===`);

  // Step 4: Sync taxa
  await syncTaxa(projects);

  // Step 5: Sync local counts & endemic species
  await syncLocalCountsAndEndemics(projects);

  // Step 6: Sync identification stats
  await syncIdentificationStats(projects);

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
