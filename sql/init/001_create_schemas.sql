-- ============================================================
-- SkillPulse Warehouse Schema Initialization
-- Phase 3 - Step 1: Database Setup
-- ============================================================

-- PURPOSE:
-- Initializes the three-layer data warehouse structure:
-- 1. RAW - Immutable layer containing JSON metadata and references
-- 2. STAGING - Cleaned and standardized job posting records
-- 3. MARTS - Analytics-ready facts and dimensions for reporting

-- LAYER 1: RAW DATA LAYER
-- ============================================================
-- Stores immutable copies of API responses and ingestion metadata.
-- Tables created here: raw_ingestion_manifest, raw_job_postings_* (Phase 3, Step 3)
-- 
CREATE SCHEMA IF NOT EXISTS raw;

-- LAYER 2: STAGING LAYER
-- ============================================================
-- Intermediate layer for data cleaning and transformation.
-- Flattens JSON into relational columns with standardized types.
-- Tables created here: stg_job_postings, stg_companies, etc. (Phase 3, Steps 5-6)
-- Built using dbt models (Phase 3, Step 4)
--
CREATE SCHEMA IF NOT EXISTS staging;

-- LAYER 3: MARTS LAYER (Analytics-Ready)
-- ============================================================
-- Final layer with dimensional model for business analytics.
-- Includes fact and dimension tables.
-- Tables created here:
--   - fact_job_postings (fact table with business keys)
--   - dim_date (date dimension for time-based analysis)
--   - dim_company (company dimension)
--   - dim_location (location dimension)
--   - dim_source (data source dimension: Adzuna, RemoteOK)
--   - dim_skill (job skill requirements - Phase 3, Step 8+)
-- Built using dbt models (Phase 3, Steps 7-8)
--
CREATE SCHEMA IF NOT EXISTS marts;

-- SCHEMA COMMENTS
-- ============================================================
COMMENT ON SCHEMA raw IS 'Immutable layer: Raw API responses and ingestion metadata';
COMMENT ON SCHEMA staging IS 'Intermediate layer: Cleaned and standardized data';
COMMENT ON SCHEMA marts IS 'Analytics layer: Dimensional model for reporting';