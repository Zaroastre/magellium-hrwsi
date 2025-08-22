----------------------------------
------------- INPUT --------------
----------------------------------

--- COLD TABLES ---

/*
Table for the raster types.
List all product types.
*/
CREATE TABLE hrwsi.raster_type (

  product_type text NOT NULL UNIQUE,
  description text NOT NULL,
  processing_level text
);
INSERT INTO hrwsi.raster_type VALUES
  ('S2_MAJA_L2A', 'Sentinel-2 MAJA computation of Sentinel-2-like images L2A', 'L2A'),
  ('S2_FSC_L2B', 'Fractional Snow Cover L2B', 'L2B'),
  ('IW_GRDH_1S', 'Sentinel-1 level-1 SAR product with VV-VH polarisation', 'L1S'),
  ('S2MSI1C', 'Sentinel-2 L1C raster from the Sentinel-2 satelite constelation', 'L1C'),
  ('S2_CC_L2B', 'Cloud Classification raster', 'L2B'),
  ('S2_WICS2_L2B', 'Water and Ice Coverage L2B', 'L2B'),
  ('S1_WICS1_L2B', 'Water and Ice Coverage L2B', 'L2B'),
  ('S1_WDS_L2B', 'Water Dry Snow', 'L2B'),
  ('S1_NRB_L2A', 'Sentinel-1 Normalised Radar Backscatter 10m', 'L2A'),
  ('S1_SWS_L2B', 'Sentinel-1 SAR Wet Snow', 'L2B'),
  ('GFSC_L2C', 'Gap-filled Fractional Snow Cover', 'L2C'),
  ('COMB_WICS1S2', 'WICS1 and WICS2 combination', 'L2C')
;

/*
Table for processing routines.
List the various type of processing routines used for the HR-WSI production.
Processing routines are defined by:
   - a name (str)
   - a product_type_code (str)
   - a product_type_name (str)
   - a number of CPUs (int)
   - a number of GB of RAM (int)
   - a number of GB of storage space (int)
   - a duration in minutes (int)
   - a Docker image name (str)

The processing routines provide information about scientific softwares execution context.
*/
CREATE TABLE hrwsi.processing_routine (

    name text UNIQUE,
    product_type_code text REFERENCES hrwsi.raster_type(product_type) NOT NULL,
    cpu smallint NOT NULL,
    ram smallint NOT NULL, -- Used to force Nomad to allocate 1 job max pet worker.
    storage_space smallint NOT NULL,
    duration smallint NOT NULL,
    docker_image text NOT NULL,
    flavour text NOT NULL
);
-- INSERT TO UPDATE
INSERT INTO hrwsi.processing_routine VALUES
  ('FSC', 'S2_FSC_L2B', 4, 29879, 16, 10, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/fsc:1.0', 'hma.large'),
  ('CC', 'S2_CC_L2B', 4, 29879, 32, 13, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/cc:1.0', 'hma.large'),
  ('SIG0', 'S1_NRB_L2A', 4, 29879, 32, 10, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/sig0:1.0', 'hma.large'),
  ('WICS2', 'S2_WICS2_L2B', 4, 29879, 16, 10, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/wics2:1.0', 'hma.large'),
  ('WDS', 'S1_WDS_L2B', 2, 7383, 2, 2, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/wds:1.0', 'eo1.large'),
  ('SWS', 'S1_SWS_L2B', 2, 7383, 2, 2, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/sws:1.0', 'eo1.large'),
  ('WICS1', 'S1_WICS1_L2B', 2, 7383, 2, 2, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/wics1:1.0', 'eo1.large'),
  ('GFSC', 'GFSC_L2C', 2, 7383, 2, 2, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/gfsc:1.0', 'eo1.large'),
  ('WICS1S2', 'COMB_WICS1S2', 2, 7383, 32, 10, 'registry-ext.magellium.com:443/eea_hr-wsi/nrt_production_system/wics1s2:1.0', 'eo1.large')
;

/*
Table for triggering condition types.
List the various type of processing conditions used for the HRWSI production.
Processing conditions are defined by:
   - a name (str)
   - a processing routine name (see table processing_routine)
   - a description (str)

Processing conditions define the conditions upon which processing routines
must be launched.
*/
CREATE TABLE hrwsi.triggering_condition (

    name text UNIQUE,
    processing_routine_name text REFERENCES hrwsi.processing_routine(name) NOT NULL,
    description text
);
INSERT INTO hrwsi.triggering_condition VALUES -- Caution: values must be the same as in the input_type.py enum module.
  ('FSC_TC', 'FSC', 'Conditions to calculate Fractional Snow Cover. We want an L2A image not already processed, with creation date in the last 7 days.'),
  ('CC_TC', 'CC', 'Conditions to execute Cloud Cover. We want an L1C image not already processed, with measuration date in the last 30 days, publication date in the last 7 days, covering an EEA38 restricted tile, no CC processing task already referencing this raster and the preceding CC processing task on this tile ended successfully.'),
  ('Backscatter_10m_TC', 'SIG0', 'Conditions to execute Backscatter_10m. We want a Sentinel-1 GRD slice intersecting the footprint with a S2 tile and all the consecutive GRD intersecting the respective S2 tile have to be available.'),
  ('WICS2_TC', 'WICS2', 'Conditions to execute Water and Ice Coverage Sentinel-2. We want an L2A image not already processed, with creation date in the last 7 days.'),
  ('WDS_TC', 'WDS', 'Conditions to execute Wet Dry Snow Sentinel-1. We want a backscatter_10m and FSC produced in the last 7 days, sharing the same tile, the same measurement_day and with no WDS processing task already referencing the basckscatter 10m.'),
  ('SWS_TC', 'SWS', 'Conditions to execute SAR Wet Snow Sentinel-1. We want a backscatter_10m in the last 7 days covering an eligible tile, with no SWS processing task already referencing the basckscatter 10m.'),
  ('WICS1_TC', 'WICS1', 'Conditions to execute Water and Ice Coverage Sentinel-1. We want a backscatter_10m in the last 7 days with no WICS1 processing task already referencing the basckscatter 10m.'),
  ('GFSC_TC', 'GFSC', 'Conditions to execute Gap-filled Fractional Snow Cover. We want an FSC and SWS produced the day before. If it does not exist, we can retrieve an FSC and/or SWS produced on previous days for a maximum of 7 days in the past.'),
  ('WICS1S2_TC', 'WICS1S2', 'Conditions to execute WICS1S2. It is triggered and one WICS1 and at least one WICS2 were produced on the same tile on the same day. If two WICS2 have been produced it means they are partial, both are taken. If two WICS1 are produced it means they were one acquisiton in the morning and another in the afternoon, two WICS1S2 are processed.')
;


--- CORE TABLES ---
/*
Table for the input met during the operation of the HRWSI production system.
List all the inputs ever met alongside their core informations.
Multiple inputs can refer to the same input_path if there processing condition or
tile are different.

It is a representation of the Input class in the Python code.
*/
CREATE TABLE hrwsi.raw_inputs (

  id text UNIQUE,
  product_type_code text REFERENCES hrwsi.raster_type(product_type) NOT NULL,
  start_date timestamp NOT NULL,
  publishing_date timestamp NOT NULL,
  tile text NOT NULL,
  measurement_day bigint NOT NULL, -- origin date
  input_path text NOT NULL,
  is_partial bool NOT NULL,
  relative_orbit_number integer,
  harvesting_date timestamp NOT NULL
  );

/*
Table for the triggering validation.

*/
CREATE TABLE hrwsi.trigger_validation (

  id bigserial UNIQUE,
  triggering_condition_name text REFERENCES hrwsi.triggering_condition(name) NOT NULL,
  validation_date timestamp NOT NULL,
  is_nrt bool NOT NULL,
  artificial_measurement_day int8 NULL
);

/*
Table for the raw2valid

*/
CREATE TABLE hrwsi.raw2valid (

  trigger_validation_id bigserial REFERENCES hrwsi.trigger_validation(id) NOT NULL,
  raw_input_id text REFERENCES hrwsi.raw_inputs(id) NOT NULL
);


--- FUNCTIONS ---

CREATE FUNCTION hrwsi.get_input_by_input_path (path text)
    RETURNS setof hrwsi.raw_inputs AS $$
-- To be used when searching for the Inputs that were created mentioning a specific
-- file or directory.
BEGIN RETURN query SELECT
    ri.id,
    ri.product_type_code,
    ri.harvesting_date,
    ri.publishing_date,
    ri.tile,
    ri.measurement_day,
    ri.relative_orbit_number,
    ri.input_path,
    ri.is_partial
FROM hrwsi.raw_inputs ri WHERE ri.input_path = path;
END $$ LANGUAGE plpgsql stable;

CREATE FUNCTION hrwsi.get_input_by_measurement_day (day bigint)
    RETURNS setof hrwsi.raw_inputs AS $$
-- To be used when searching for the Inputs that were created on data measured
-- on a specific day.
BEGIN RETURN query SELECT
    ri.id,
    ri.product_type_code,
    ri.harvesting_date,
    ri.publishing_date,
    ri.tile,
    ri.measurement_day,
    ri.relative_orbit_number,
    ri.input_path,
    ri.is_partial
FROM hrwsi.raw_inputs ri WHERE ri.measurement_day = day;
END $$ LANGUAGE plpgsql stable;

CREATE FUNCTION hrwsi.get_input_by_input_type (in_type text)
    RETURNS setof hrwsi.raw_inputs AS $$
-- To be used when searching for the Inputs of a specific input_type.
BEGIN RETURN query SELECT
    ri.id,
    ri.product_type_code,
    ri.harvesting_date,
    ri.publishing_date,
    ri.tile,
    ri.measurement_day,
    ri.relative_orbit_number,
    ri.input_path,
    ri.is_partial
FROM hrwsi.raw_inputs ri
INNER JOIN hrwsi.raster_type rt
ON ri.product_type_code = rt.product_type
INNER JOIN hrwsi.processing_routine pr
ON pr.product_type_code = rt.product_type
INNER JOIN hrwsi.triggering_condition tc
ON tc.processing_routine_name = pr.name
WHERE pr.product_type_code = in_type;
END $$ LANGUAGE plpgsql stable;

CREATE FUNCTION hrwsi.get_input_without_processing_task ()
    RETURNS setof hrwsi.raw_inputs AS $$
-- To be used when searching for the Inputs that have not a processing task
-- allocated yet.
-- Refer to [2_init_database_processing_tasks.sql] about processing tasks.
BEGIN RETURN query SELECT
    ri.id,
    ri.product_type_code,
    ri.harvesting_date,
    ri.publishing_date,
    ri.tile,
    ri.measurement_day,
    ri.relative_orbit_number,
    ri.input_path,
    ri.is_partial
FROM hrwsi.raw_inputs ri
INNER JOIN hrwsi.raw2valid rv
ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.trigger_validation tv
ON tv.id = rv.trigger_validation_id
LEFT OUTER JOIN hrwsi.processing_tasks pt
ON pt.trigger_validation_fk_id = tv.id
WHERE pt.id IS NULL;
END $$ LANGUAGE plpgsql stable;