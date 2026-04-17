-- CNC Sync tables

-- Projects
create table cnc_projects (
  id serial primary key,
  inat_project_id int unique not null,
  slug text unique not null,
  title text,
  icon_url text,
  year int not null default 2025,
  total_observations int default 0,
  top_observers jsonb,
  top_species jsonb,
  place_ids jsonb,
  identification_stats jsonb,
  computed_stats jsonb,
  computed_at timestamptz,
  synced_at timestamptz
);

create index idx_cnc_projects_year on cnc_projects(year);

-- Observations
create table cnc_observations (
  id serial primary key,
  inat_id int unique not null,
  project_slug text not null,
  user_login text,
  user_icon text,
  observed_on date,
  quality_grade text,
  taxon_id int,
  taxon_name text,
  taxon_rank text,
  min_species_taxon_id int,
  ancestor_ids jsonb,
  common_name text,
  iconic_taxon_name text,
  photo_url text,
  faves_count int default 0,
  comments_count int default 0,
  created_at timestamptz
);

create index idx_cnc_obs_project on cnc_observations(project_slug);
create index idx_cnc_obs_user on cnc_observations(user_login);
create index idx_cnc_obs_taxon on cnc_observations(taxon_id);
create index idx_cnc_obs_date on cnc_observations(observed_on);
create index idx_cnc_obs_iconic on cnc_observations(iconic_taxon_name);

-- Sync log
create table cnc_sync_log (
  project_slug text primary key,
  last_id_above int default 0,
  total_fetched int default 0,
  status text default 'pending',
  started_at timestamptz,
  completed_at timestamptz
);

-- Taxa cache (shared across projects)
create table cnc_taxa (
  taxon_id int primary key,
  observations_count int default 0,
  conservation_status text,
  conservation_status_name text,
  synced_at timestamptz
);

-- Per-project species data (local counts, endemic flag)
create table cnc_project_species (
  project_slug text not null,
  taxon_id int not null,
  local_obs_count int,
  is_endemic boolean default false,
  synced_at timestamptz,
  primary key (project_slug, taxon_id)
);

create index idx_cnc_project_species_slug on cnc_project_species(project_slug);

-- Umbrella stats cache (one row per year, populated by sync)
create table cnc_umbrella_stats (
  year int primary key,
  total_observations int default 0,
  total_species int default 0,
  total_observers int default 0,
  research_grade int default 0,
  needs_id int default 0,
  total_identifiers int default 0,
  threatened_species int default 0,
  endemic_species int default 0,
  synced_at timestamptz
);

-- Migration: add columns to cnc_projects
-- alter table cnc_projects add column place_ids jsonb;
-- alter table cnc_projects add column identification_stats jsonb;
