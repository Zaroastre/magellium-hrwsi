-----------------------------------------------------------------------------
-- HR-WSI PostgreSQL configuration
-- This file contains the systemparams schema declaration
-----------------------------------------------------------------------------
CREATE SCHEMA systemparams;

-----------------------------------------------------------------------------

/*
Table for the wekeo_api_manager parameters.
*/
CREATE TABLE systemparams.wekeo_api_manager (
    triggering_condition_name text NOT NULL,
    input_type text NOT NULL,
    collection text NOT NULL,
    max_day_since_publication_date smallint NOT NULL,
    max_day_since_measurement_date smallint NOT NULL,
    tile_list_file text,
    geometry_file text,
    polarisation text,
    timeliness text,
    nrt_harvest_start_date text,
    archive_harvest_start_date text,
    archive_harvest_end_date text,
    UNIQUE (triggering_condition_name, timeliness)
);

INSERT INTO systemparams.wekeo_api_manager
VALUES ('L1C_TC', 'S2MSI1C', 'Sentinel2', 5, 5, 'HRWSI_System/settings/tile_list.yaml', 'HRWSI_System/settings/geometry.yaml', NULL, NULL, NULL, NULL, NULL),
       ('GRD_TC', 'IW_GRDH_1S', 'Sentinel1', 5, 5, 'HRWSI_System/settings/tile_list.yaml', 'HRWSI_System/settings/geometry.yaml', 'VV%26VH', 'NRT-3h', NULL, NULL, NULL),
       ('GRD_TC', 'IW_GRDH_1S', 'Sentinel1', 5, 5, 'HRWSI_System/settings/tile_list.yaml', 'HRWSI_System/settings/geometry.yaml', 'VV%26VH', 'Fast-24h', NULL, NULL, NULL)
;

/*
Table for the triggerer config parameters.
*/
CREATE TABLE systemparams.triggerer_config (
    product_type text PRIMARY KEY,
    last_processing_date bigint
);

INSERT INTO systemparams.triggerer_config VALUES ('GFSC_L2C', NULL);

/*
Table for the Launcher config parameters.
*/
CREATE TABLE systemparams.launcher_config (
    param text PRIMARY KEY,
    value text
);

/*
This eligible_tiles is used for testing purposes and need to be update according to our needs
 */
INSERT INTO systemparams.launcher_config VALUES ('pt_reprocessing_waiting_time', '600'),
                                                ('interval', '{"months": 1, "days": 15}'),
                                                ('eligible_tiles', '32TPR,32TPS');
