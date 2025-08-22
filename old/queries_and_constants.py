# ----------------------------
#  constant config parameters
# ----------------------------

CONFIG_FILE_PATH = """configuration/settings/config.yaml"""
TILE_LIST_FILE_PATH = """configuration/settings/tile_list.yaml"""
CONFIG_CREATE_PRODUCTS_FILE_PATH = """configuration/apimanager/config_create_products.yaml"""
CONFIG_CREATE_INPUT_FILE_PATH = """configuration/apimanager/config_create_input.yaml"""
GET_WEKEO_API_MANAGER_PARAMS = """SELECT triggering_condition_name, input_type, collection, max_day_since_publication_date,
    max_day_since_measurement_date, tile_list_file, geometry_file, polarisation, timeliness, nrt_harvest_start_date,
    archive_harvest_start_date, archive_harvest_end_date FROM systemparams.wekeo_api_manager"""
SQL_NOTIFY_REQ = "NOTIFY processing_task_insertion, '{}';"

# -----------------------------
#  Harvester config parameters
# -----------------------------

GET_UNPROCESSED_PRODUCTS_REQUEST = """SELECT id, product_type_code, product_path,
TO_CHAR(creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date,
TO_CHAR(catalogue_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS catalogue_date, kpi_file_path
FROM hrwsi.products p WHERE p.id NOT IN (SELECT ri.id FROM hrwsi.raw_inputs ri)"""

INSERT_CANDIDATE_REQUEST = """INSERT INTO hrwsi.raw_inputs (id, product_type_code, start_date, publishing_date,
tile, measurement_day, relative_orbit_number, input_path, is_partial, harvesting_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) ON CONFLICT (id) DO NOTHING"""

LISTEN_PRODUCTS_REQUEST = """LISTEN product_insertion"""

SET_DATA_HARVESTING_START_DATE = """UPDATE systemparams.wekeo_api_manager
    SET harvest_start_date='{}' WHERE triggering_condition_name='{}';"""

INPUT_TYPE_LIST_REQUEST = """SELECT DISTINCT product_type_code FROM hrwsi.processing_routine;"""

CANDIDATE_ALREADY_IN_DATABASE_REQUEST = """SELECT input_path FROM hrwsi.raw_inputs ri WHERE ri.measurement_day>={} AND ri.product_type_code='{}';"""

GRD_CANDIDATE_ALREADY_IN_DATABASE_REQUEST = """SELECT ri.tile, ri.start_date FROM hrwsi.raw_inputs ri WHERE ri.measurement_day>={} AND ri.product_type_code='{}';"""

PROCESSING_TASK_UNPROCESSED_REQUEST: str = """SELECT count(task_id)
FROM (SELECT pt.trigger_validation_fk_id AS task_id FROM hrwsi.processing_tasks pt
WHERE pt.creation_date>'%s') AS x, hrwsi.is_one_processing_task_processed_for_an_input(task_id)
WHERE is_one_processing_task_processed_for_an_input=false;"""

ELIGIBLE_PRODUCT_LIST = ['GFSC_L2C', 'S1_NRB_L2A', 'S1_SWS_L2B', 'S1_WDS_L2B', 'S1_WICS1_L2B', 'S2_CC_L2B', 'S2_FSC_L2B', 'S2_MAJA_L2A', 'S2_WICS2_L2B', 'COMB_WICS1S2']

UNSET_HARVEST_START_DATES = """UPDATE systemparams.wekeo_api_manager SET {}
WHERE CONCAT(triggering_condition_name, timeliness) = '{}';"""

# ------------------------------------------------------------
#  Api manager - hrwsi_database_api_manager config parameters
# ------------------------------------------------------------

# Not yet used - TODO: to update
PRODUCT_TYPE_ID_WHO_CAN_CREATE_INPUT_REQUEST = """SELECT DISTINCT pt.id AS product_type_id
FROM hrwsi.product_type pt
LEFT JOIN hrwsi.processing_routine pr ON pr.input_type=pt.data_type WHERE pr.input_type is NOT NULL;"""

# Not yet used - TODO: to update
COLLECT_PRODUCTS_THAT_BECOME_INPUTS_REQUEST = """SELECT product_path, creation_date, tile, measurement_day, mission
FROM hrwsi.products p
INNER JOIN hrwsi.input i ON i.id = p.input_fk_id WHERE p.creation_date >= '%s' AND p.product_type_id IN %s;"""

# -----------------------------
#  Triggerer config parameters
# -----------------------------

GET_UNPROCESSED_RAW_INPUTS_REQUEST = """SELECT id, product_type_code, TO_CHAR(start_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS start_date,
TO_CHAR(publishing_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS publishing_date, tile, measurement_day, relative_orbit_number,
input_path, is_partial, TO_CHAR(harvesting_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS harvesting_date
FROM hrwsi.raw_inputs ri
WHERE ri.product_type_code IN ({})
AND ri.id NOT IN (
    SELECT ri.id FROM hrwsi.trigger_validation tv
    INNER JOIN hrwsi.raw2valid r2v ON r2v.trigger_validation_id=tv.id
    INNER JOIN hrwsi.raw_inputs ri ON ri.id=r2v.raw_input_id
    WHERE tv.triggering_condition_name = '{}'
)
ORDER BY ri.harvesting_date DESC;"""

INSERT_TRIGGER_VALIDATION = """INSERT INTO hrwsi.trigger_validation (triggering_condition_name, validation_date, is_nrt, artificial_measurement_day)
VALUES (%s, %s, %s, %s) RETURNING id"""

INSERT_RAW2VALID = "INSERT INTO hrwsi.raw2valid (trigger_validation_id, raw_input_id) VALUES (%s, %s)"

LISTEN_INPUT_INSERTION_REQUEST = "LISTEN input_insertion"

IS_L2A_EXISTS_REQUEST = """SELECT id, product_type_code, measurement_day FROM hrwsi.raw_inputs
WHERE product_type_code = 'S2_MAJA_L2A' AND tile = '{}'
AND measurement_day BETWEEN {} AND {} ORDER BY measurement_day DESC LIMIT 1"""

IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT = """SELECT NOT EXISTS (SELECT tv.id as validation_id FROM hrwsi.trigger_validation tv
INNER JOIN hrwsi.raw2valid r2v ON tv.id = r2v.trigger_validation_id
WHERE r2v.raw_input_id = '%s' AND tv.triggering_condition_name = '%s') AS result"""

IS_ONE_PROCESSING_TASK_EXISTS_FOR_THIS_TRIGGERING_CONDITION_TODAY_ON_SAME_TILE_AND_MEASUREMENT_DAY = """
SELECT EXISTS (SELECT pt.id as task_id FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON tv.id = pt.trigger_validation_fk_id
INNER JOIN hrwsi.raw2valid r2v ON tv.id = r2v.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON r2v.raw_input_id = ri.id
WHERE tv.triggering_condition_name = '{tc_name}' AND pt.creation_date >= date_trunc('day', NOW())
AND ri.tile = '{tile}' AND ri.measurement_day = {measurement_day}) AS result"""

IS_INPUT_SHARE_SAME_TILE_AND_MEASUREMENT_DAY = """SELECT ri.id,
to_char(ri.harvesting_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS harvesting_date, ri.input_path, ri.is_partial,
ri.measurement_day, ri.product_type_code, to_char(ri.publishing_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS publishing_date,
ri.relative_orbit_number, to_char(ri.start_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS start_date, ri.tile
FROM hrwsi.raw_inputs ri
WHERE ri.product_type_code = '%s' AND ri.measurement_day = %s AND ri.tile = '%s' AND ri.harvesting_date >= '%s'"""

GET_LAST_PROCESSING_DATE = """SELECT last_processing_date FROM systemparams.triggerer_config WHERE product_type = '{}'"""

IS_PREVIOUS_L2A_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL = """SELECT EXISTS (
SELECT 1 FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.products p ON p.trigger_validation_fk_id=tv.id
INNER JOIN hrwsi.raw2valid rv ON rv.trigger_validation_id = tv.id
INNER JOIN hrwsi.raw_inputs ri ON ri.id = rv.raw_input_id
WHERE tv.triggering_condition_name = 'CC_TC' AND ri.tile = '{}'
AND ri.measurement_day BETWEEN {} AND {}) AS result"""

UNPROCESSED_TV_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL = """SELECT EXISTS (
SELECT 1 FROM hrwsi.trigger_validation tv
INNER JOIN hrwsi.raw2valid rv ON rv.trigger_validation_id = tv.id
INNER JOIN hrwsi.raw_inputs ri ON ri.id = rv.raw_input_id
WHERE tv.triggering_condition_name = 'CC_TC' AND ri.tile = '{}'
AND ri.measurement_day BETWEEN {} AND {}
AND NOT EXISTS (
    SELECT 1
    FROM hrwsi.processing_tasks pt
    WHERE tv.id = pt.trigger_validation_fk_id
    )
) AS result"""

GET_L1C_UNPROCESSED = """SELECT ri.id, ri.product_type_code, ri.publishing_date, ri.start_date, ri.tile,
    ri.measurement_day, ri.relative_orbit_number, ri.input_path, ri.is_partial, ri.harvesting_date
    FROM hrwsi.raw_inputs ri
    WHERE ri.product_type_code = 'S2MSI1C'
    AND NOT EXISTS (
        SELECT 1
        FROM hrwsi.raw2valid rv
        WHERE ri.id = rv.raw_input_id
    )
    ORDER BY ri.measurement_day"""

COUNT_UNFINISHED_CC_PT_ON_TILE_AND_DATE_INTERVAL = """SELECT DISTINCT(tile)
FROM (
    SELECT ri.tile, MAX(psw.exit_code) max_code
    -- SELECT DISTINCT(njd.id)
    FROM hrwsi.processing_tasks pt
    INNER JOIN hrwsi.trigger_validation tv ON tv.id=pt.trigger_validation_fk_id
    INNER JOIN hrwsi.raw2valid r2v ON r2v.trigger_validation_id=tv.id
    INNER JOIN hrwsi.raw_inputs ri ON ri.id=r2v.raw_input_id AND ri.measurement_day>{}
    INNER JOIN hrwsi.processingtask2nomad p2n on p2n.processing_task_id=pt.id
    INNER JOIN hrwsi.nomad_job_dispatch njd ON njd.id=p2n.nomad_job_id
    INNER JOIN hrwsi.processing_status_workflow psw ON psw.nomad_job_dispatch_fk_id=njd.id
    WHERE pt.has_ended=false
    AND tv.triggering_condition_name='CC_TC'
    GROUP BY ri.tile
    ORDER BY ri.tile ASC
) WHERE max_code IS NOT NULL
ORDER BY tile ASC;"""

COUNT_UNDISPATCHED_CC_PT_ON_TILE_AND_DATE_INTERVAL = """SELECT tile
FROM (
    SELECT ri.tile tile FROM hrwsi.processing_tasks pt
    JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
    JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
    JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
    WHERE tv.triggering_condition_name = 'CC_TC'
    AND ri.measurement_day > {}
    AND NOT EXISTS (
        SELECT 1
        FROM hrwsi.processingtask2nomad p2n
        WHERE p2n.processing_task_id = pt.id
    )
)"""

COUNT_TO_BE_CREATED_CC_PT_ON_TILE_AND_DATE_INTERVAL = """SELECT tile
FROM (
    SELECT ri.tile tile
    FROM hrwsi.trigger_validation tv
    JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
    JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
    WHERE tv.triggering_condition_name = 'CC_TC'
    AND ri.measurement_day > {}
    AND NOT EXISTS (
        SELECT 1
        FROM hrwsi.processing_tasks pt
        WHERE pt.trigger_validation_fk_id = tv.id
    )
)"""

GET_GRDH_UNPROCESSED = """SELECT ri.id, ri.product_type_code, ri.harvesting_date, ri.publishing_date, ri.tile,
    ri.measurement_day , ri.relative_orbit_number, ri.input_path, ri.is_partial
    FROM hrwsi.raw_inputs ri
    WHERE ri.product_type_code = 'IW_GRDH_1S' AND ri.is_partial is True
    AND ri.harvesting_date >= NOW() - INTERVAL '7 days'
    AND NOT EXISTS (
        SELECT 1
        FROM hrwsi.raw2valid rv
        WHERE ri.id = rv.raw_input_id
    );"""

IS_NRT_FROM_NOW_HARVEST_DATE_REQ = """SELECT
    CASE
        WHEN publishing_date IS NOT NULL
             AND harvesting_date IS NOT NULL
             AND harvesting_date BETWEEN publishing_date AND publishing_date + INTERVAL '3 hours'
        THEN TRUE
        ELSE FALSE
    END AS is_within_3hours
FROM hrwsi.raw_inputs
WHERE id = '{}';"""

IS_NRT_FROM_PAST_HARVEST_DATE_REQ = """SELECT
    CASE
        WHEN measurement_day >= {}
        THEN TRUE
        ELSE FALSE
    END AS is_measurement_day_valid
FROM hrwsi.raw_inputs
WHERE id = '{}';"""

GET_PAST_HARVEST_DATE_REQ = """SELECT wam.nrt_harvest_start_date FROM systemparams.wekeo_api_manager wam
WHERE wam.input_type = '{}'"""

NRT_PRODUCT_LIST = ['IW_GRDH_1S', 'S1_NRB_L2A', 'S1_WDS_L2B', 'S1_SWS_L2B', 'S1_WICS1_L2B', 'S2MSI1C', 'S2_CC_L2B', 'S2_MAJA_L2A', 'S2_WICS2_L2B', 'S2_FSC_L2B']

GET_UNPROCESSED_PRODUCT = """SELECT ri.id, ri.tile FROM hrwsi.raw_inputs ri
INNER JOIN hrwsi.raw2valid rv ON ri.id = rv.raw_input_id
INNER JOIN hrwsi.trigger_validation tv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.processing_tasks pt ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.processingtask2nomad pn ON pn.processing_task_id = pt.id
INNER JOIN hrwsi.nomad_job_dispatch njd ON njd.id = pn.nomad_job_id
INNER JOIN hrwsi.processing_status_workflow psw ON njd.id = psw.nomad_job_dispatch_fk_id
WHERE ri.measurement_day = {}
AND ri.product_type_code IN ({})
AND pt.has_ended IS FALSE
AND psw.processing_status_id IN (0, 2)"""

GET_ALL_FSC_AND_SWS_IN_THE_LAST_7_DAYS = """SELECT ri.id, ri.product_type_code FROM hrwsi.raw_inputs ri
WHERE ri.product_type_code IN ('S2_FSC_L2B', 'S1_SWS_L2B') AND ri.tile = '{}' AND ri.measurement_day BETWEEN {} AND {}"""

SET_LAST_GFSC_PROCESSING_DATE = """UPDATE systemparams.triggerer_config
SET last_processing_date = {} WHERE product_type ='GFSC_L2C'"""

GET_PROCESSING_DATE_GFSC_PT = """SELECT tv.triggering_condition_name, tv.artificial_measurement_day FROM hrwsi.trigger_validation tv
INNER JOIN hrwsi.raw2valid rv ON rv.trigger_validation_id = tv.id
INNER JOIN hrwsi.raw_inputs ri ON ri.id = rv.raw_input_id
WHERE ri.id = '{}' AND tv.id = {}."""

GET_WICS1S2_PAIRS_REQUEST = """SELECT
    a.id AS id_wics1,
    a.measurement_day,
    array_agg(b.id) AS wics2_ids
FROM
    hrwsi.raw_inputs a
JOIN
    hrwsi.raw_inputs b
    ON a.tile = b.tile 
    AND a.measurement_day = b.measurement_day
    AND a.product_type_code = 'S1_WICS1_L2B'
    AND b.product_type_code = 'S2_WICS2_L2B'
GROUP BY a.id, a.measurement_day"""

# --------------------------------
#  Orchestrator config parameters
# --------------------------------

GET_UNPROCESSED_RAW2VALID_INPUTS_REQUEST = """SELECT *
FROM hrwsi.raw2valid r2v
WHERE NOT EXISTS (
    SELECT 1
    FROM hrwsi.processing_tasks pt
    WHERE pt.trigger_validation_fk_id = r2v.trigger_validation_id
)"""

PROCESSING_ROUTINE_REQUEST = """SELECT tc.name as tc_name, pr.name, pr.product_type_code, pr.cpu, pr.ram,
pr.storage_space, pr.duration, pr.docker_image, pr.flavour FROM hrwsi.processing_routine pr
INNER JOIN hrwsi.triggering_condition tc ON pr.name = tc.processing_routine_name"""

ADD_PROCESSING_TASK_REQUEST = """INSERT INTO hrwsi.processing_tasks
(trigger_validation_fk_id, creation_date, has_ended, processing_date) VALUES (%s, %s, %s, %s)"""

PT_ALREADY_IN_DATABASE_REQUEST = """SELECT pt.id AS processing_task_id FROM hrwsi.processing_tasks pt
WHERE pt.trigger_validation_fk_id = {}"""

LISTEN_RAW2VALID_REQUEST = """LISTEN raw2valid_insertion"""

NOTIFY_RAW2VALID_REQUEST = """NOTIFY raw2valid_insertion, '{}';"""

# ----------------------------
#  Launcher config parameters
# ----------------------------

LISTEN_LAUNCHER_PT_REQUEST = """LISTEN processing_task_insertion"""

GET_CONFIG_PT_REPROCESSING_WAITING_TIME_PARAM = "SELECT param, value FROM systemparams.launcher_config"

GET_OLDEST_MEASUREMENT_DATE_FROM_UNPROCESSED_PT_REQUEST = """SELECT MIN(ri.measurement_day)
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.processing_routine pr ON ri.product_type_code = pr.product_type_code
LEFT JOIN hrwsi.processingtask2nomad ptn ON ptn.processing_task_id = pt.id
WHERE tv.is_nrt = FALSE
AND pr.flavour = '{}'
AND ri.tile IN ({})
AND ri.measurement_day < 20250115
AND ptn.processing_task_id IS NULL;
"""

GET_UNDISPATCHED_PT_REQUEST = """SELECT pt.id, pt.trigger_validation_fk_id, pt.virtual_machine_id,
TO_CHAR(pt.creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date,
pt.processing_date AS processing_date,
pt.preceding_input_id, pt.has_ended, pt.intermediate_files_path, pr.flavour
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.triggering_condition tc ON tc."name" = tv.triggering_condition_name
INNER JOIN hrwsi.processing_routine pr ON tc.processing_routine_name = pr."name"
WHERE pr.flavour = '{}'
AND ri.measurement_day >= 20250115
AND NOT EXISTS (
    SELECT 1
    FROM hrwsi.processingtask2nomad ptn
    WHERE ptn.processing_task_id = pt.id
)"""

GET_IN_ERROR_PT_REQUEST = """SELECT DISTINCT(pt.id), pt.trigger_validation_fk_id, pt.virtual_machine_id,
TO_CHAR(pt.creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date,
pt.processing_date AS processing_date,
pt.preceding_input_id, pt.has_ended, pt.intermediate_files_path, pr.flavour
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.triggering_condition tc ON tc."name" = tv.triggering_condition_name
INNER JOIN hrwsi.processing_routine pr ON tc.processing_routine_name = pr."name"
INNER JOIN hrwsi.processingtask2nomad p2n ON p2n.processing_task_id=pt.id
INNER JOIN hrwsi.nomad_job_dispatch njd ON njd.id=p2n.nomad_job_id
INNER JOIN hrwsi.processing_status_workflow psw ON psw.nomad_job_dispatch_fk_id=njd.id
WHERE pr.flavour = '{}'
AND ri.measurement_day >= 20250115
AND pt.has_ended=FALSE
AND psw.processing_status_id IN (4,5);
"""

GET_UNFINISHED_PT_REQUEST = """SELECT pt.id, pt.trigger_validation_fk_id, pt.virtual_machine_id,
TO_CHAR(pt.creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date,
pt.processing_date AS processing_date,
pt.preceding_input_id, pt.has_ended, pt.intermediate_files_path, pr.flavour,
njd.dispatch_date, njd.id as nomad_job_uuid, pr.duration
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.triggering_condition tc ON tc."name" = tv.triggering_condition_name
INNER JOIN hrwsi.processing_routine pr ON tc.processing_routine_name = pr."name"
INNER JOIN hrwsi.processingtask2nomad p2n ON p2n.processing_task_id=pt.id
INNER JOIN hrwsi.nomad_job_dispatch njd ON njd.id=p2n.nomad_job_id
WHERE pr.flavour = '{}'
AND pt.has_ended=FALSE
AND ri.measurement_day >= 20250115
"""

GET_UNFINISHED_WITH_CALLBACK_PT_REQUEST = """SELECT pt.id, pt.trigger_validation_fk_id, pt.virtual_machine_id,
TO_CHAR(pt.creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date,
pt.processing_date AS processing_date,
pt.preceding_input_id, pt.has_ended, pt.intermediate_files_path, pr.flavour,
njd.dispatch_date, njd.id as nomad_job_uuid, pr.duration
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.triggering_condition tc ON tc."name" = tv.triggering_condition_name
INNER JOIN hrwsi.processing_routine pr ON tc.processing_routine_name = pr."name"
INNER JOIN hrwsi.processingtask2nomad p2n ON p2n.processing_task_id=pt.id
INNER JOIN hrwsi.nomad_job_dispatch njd ON njd.id=p2n.nomad_job_id
INNER JOIN hrwsi.processing_status_workflow psw ON psw.nomad_job_dispatch_fk_id=njd.id
WHERE pr.flavour = '{}'
AND pt.has_ended=FALSE
AND ri.measurement_day >= 20250115
"""

GET_UNFINISHED_PT_WITH_EXIT_CODE = """SELECT DISTINCT(pt.id), pt.trigger_validation_fk_id, pt.virtual_machine_id,
TO_CHAR(pt.creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date,
pt.processing_date AS processing_date,
pt.preceding_input_id, pt.has_ended, pt.intermediate_files_path, pr.flavour,
njd.dispatch_date, njd.id as nomad_job_uuid, pr.duration
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.triggering_condition tc ON tc."name" = tv.triggering_condition_name
INNER JOIN hrwsi.processing_routine pr ON tc.processing_routine_name = pr."name"
INNER JOIN hrwsi.processingtask2nomad p2n ON p2n.processing_task_id=pt.id
INNER JOIN hrwsi.nomad_job_dispatch njd ON njd.id=p2n.nomad_job_id
INNER JOIN hrwsi.processing_status_workflow psw ON psw.nomad_job_dispatch_fk_id=njd.id
WHERE pr.flavour = 'eo1.large'
AND pt.has_ended=FALSE
AND ri.measurement_day >= 20250115
AND psw.exit_code IS NOT NULL
"""

GET_UNPROCESSED_ARCHIVE_PT_REQUEST = """SELECT pt.id, pt.trigger_validation_fk_id, 
TO_CHAR(pt.creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date,
pt.processing_date AS processing_date,
pt.has_ended, pt.intermediate_files_path, pr.flavour
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON rv.raw_input_id = ri.id
INNER JOIN hrwsi.processing_routine pr ON ri.product_type_code = pr.product_type_code
LEFT JOIN hrwsi.processingtask2nomad ptn ON ptn.processing_task_id = pt.id
WHERE pr.flavour = '{}'
AND ri.tile IN ({})
AND ri.measurement_day BETWEEN {} AND {} 
AND ptn.processing_task_id IS NULL"""

WORKER_SCRIPT_PATH = "HRWSI_System/launcher/worker_script.sh"

HCL_INFO_REQUEST = """SELECT DISTINCT(ri.id) as raw_input_id, pr.flavour, pt.trigger_validation_fk_id as trigger_validation_id, pt.id as processing_task_id,
pr.product_type_code, ri.tile, ri.measurement_day, ri.harvesting_date, ri.relative_orbit_number,
pr.name as processing_routine_name, pr.ram as ram,ri.input_path,
pr.docker_image, pr.duration, pt.preceding_input_id, pt.intermediate_files_path,
pt.processing_date as processing_date
FROM hrwsi.processing_tasks pt
INNER JOIN hrwsi.trigger_validation tv ON tv.id = pt.trigger_validation_fk_id
INNER JOIN hrwsi.raw2valid rv ON tv.id = rv.trigger_validation_id
INNER JOIN hrwsi.raw_inputs ri ON ri.id = rv.raw_input_id
INNER JOIN hrwsi.triggering_condition tc ON tv.triggering_condition_name  = tc.name
INNER JOIN hrwsi.processing_routine pr ON pr.name = tc.processing_routine_name
WHERE pt.trigger_validation_fk_id = %s;
"""

NOMAD_JOB_DISPATCH_REQUEST = """INSERT INTO hrwsi.nomad_job_dispatch (id)
VALUES (%s);"""

PT_2_NOMAD_REQUEST = """INSERT INTO hrwsi.processingtask2nomad (nomad_job_id, processing_task_id)
VALUES (%s, %s);"""

PROCESSING_STATUS_WORKFLOW_REQUEST = """INSERT INTO hrwsi.processing_status_workflow (nomad_job_dispatch_fk_id, processing_status_id, date)
VALUES (%s, (SELECT id FROM hrwsi.processing_status WHERE name = %s), %s);"""

IS_THIS_PT_CURRENTLY_DEPLOYED = """SELECT EXISTS(
  SELECT 1
  FROM hrwsi.processing_tasks pt
  INNER JOIN hrwsi.processingtask2nomad p2n ON p2n.processing_task_id = pt.id
  WHERE pt.id = {}
  AND pt.has_ended=FALSE
  AND NOT EXISTS(
    SELECT 1
    FROM hrwsi.processing_status_workflow psw
    WHERE p2n.nomad_job_id = psw.nomad_job_dispatch_fk_id
    AND psw.exit_code IS NOT NULL
  )
) AS result;
"""

HCL_TEMPLATE = '''
job "processing_task_name" {
  type = "batch"

  reschedule {
    attempts = 0
    unlimited = false
  }

  group "processing_task_group" {
    constraint {
      attribute = "${meta.group}"
      value     = "worker-group"
    }

    restart {
      attempts = 0
      mode     = "fail"
    }

    task "processing_task_name" {
      resources {
        memory = "ram"
      }

      kill_timeout = "timeout_max"
      driver       = "raw_exec"

      config {
        command = "/bin/bash"
        args    = ["-c", "./local/worker_script.sh", "${ENV}"]
      }

      logs {
        max_files     = 10
        max_file_size = 10
        disabled      = false
      }

      env {
        NOMAD_TOKEN = "${NOMAD_TOKEN}"
      }

      template {
        destination = "/local/task_config.yaml"
        data        = <<EOF
routine_config
flavour: flavour_content
processing_task_id: id_processing_task
input_id: id_of_input
processing_routine_name: name_of_processing_routine
input_path: path_of_input
docker_image: image_docker
nomad_job_uuid: uuid_job
trigger_validation_id: id_trigger_validation
product_type_code: code_product_type
out_code: code_out
start_time: time_of_starting
EOF
      }

      template {
        destination = "/local/s3cfg_HRWSI.txt"
        data        = <<EOF
s3cmd_hrwsi_config
EOF
      }

      template {
        destination = "/local/s3cfg_EODATA.txt"
        data        = <<EOF
s3cmd_eodata_config
EOF
      }

      template {
        destination = "/local/s3cfg_CATALOGUE.txt"
        data        = <<EOF
s3cmd_catalogue_config
EOF
      }

      template {
        destination = "/local/worker_script.sh"
        perms       = "0777"
        data        = <<EOF
wait_script
EOF
      }

      service {
        name     = "processing-routine"
        provider = "nomad"
      }
    }
  }

  constraint {
    attribute = "${attr.unique.hostname}"
    operator  = "regexp"
    value     = "^flavour_content.*"
  }
}

'''


# ----------------------------------------
#  RabbitMQ RPC Consumer config parameters
# ----------------------------------------

INSERT_PROCESSING_STATUS_WORKFLOW = """
INSERT INTO hrwsi.processing_status_workflow (nomad_job_dispatch_fk_id, processing_status_id, date, exit_code)
VALUES (%s, %s, %s, %s);"""

# TODO: remove the ' ON CONFLICT (id) DO NOTHING' part as soon as the use case with 2 or more L1C ON the same tile with the same measurement date will be handled.
# psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint "products_id_key"
# DETAIL:  Key (id)=(CLMS_WSI_CC_020m_T32TMP_20241211T102339_S2B_V100) already exists.
# CC_WORKER
# python3 /opt/wsi/processing_routine/cc_post_processing.py --l2a_name SENTINEL2B_20241211-102847-940_L2A_T32TMP_C_V1-0 --product_name CLMS_WSI_CC_020m_T32TMP_20241211T102339_S2B_V100
INSERT_PRODUCT =  """INSERT INTO hrwsi.products (id, trigger_validation_fk_id, product_type_code, product_path, creation_date, catalogue_date, kpi_file_path) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;"""

NB_OF_ERROR_REQUEST = """
SELECT count(nomad_job_id) as number_of_error_by_pt
FROM hrwsi.processingtask2nomad p2n
INNER JOIN hrwsi.processing_status_workflow psw
ON p2n.nomad_job_id = psw.nomad_job_dispatch_fk_id
WHERE p2n.processing_task_id=%s
AND psw.processing_status_id IN (4,5)
AND psw.exit_code NOT IN (109, 404); """

UUID_FOR_PROCESSING_TASK_REQUEST = """
SELECT nomad_job_id FROM hrwsi.processingtask2nomad p2n
INNER JOIN hrwsi.nomad_job_dispatch njd ON njd.id=p2n.nomad_job_id
WHERE p2n.processing_task_id=%s
ORDER BY njd.dispatch_date DESC;"""

LAST_STATUS_REQUEST = """
SELECT processing_status_id FROM hrwsi.processing_status_workflow
WHERE nomad_job_dispatch_fk_id = {}
ORDER BY date DESC
LIMIT 1;"""

GFSC_TC_ALREADY_EXIST = """
SELECT EXISTS (
    WITH selected_inputs AS (
        SELECT unnest(ARRAY{}) AS raw_input_id
    ),
    candidate_triggers AS (
        SELECT r2v.trigger_validation_id
        FROM hrwsi.raw2valid r2v
        JOIN hrwsi.trigger_validation tv ON tv.id = r2v.trigger_validation_id
        WHERE r2v.raw_input_id IN (SELECT raw_input_id FROM selected_inputs)
          AND tv.triggering_condition_name = '{}'
          AND tv.artificial_measurement_day = {}
        GROUP BY r2v.trigger_validation_id
        HAVING COUNT(*) = (SELECT COUNT(*) FROM selected_inputs)
    ),
    exact_match_triggers AS (
        SELECT ct.trigger_validation_id
        FROM candidate_triggers ct
        JOIN hrwsi.raw2valid r2v ON r2v.trigger_validation_id = ct.trigger_validation_id
        GROUP BY ct.trigger_validation_id
        HAVING
            COUNT(*) = (SELECT COUNT(*) FROM selected_inputs) AND
            BOOL_AND(r2v.raw_input_id IN (SELECT raw_input_id FROM selected_inputs))
    )
    SELECT 1 FROM exact_match_triggers
);
"""

NB_OF_NOT_SUCCESSFULLY_PROCESSED_TASK_FOR_A_DAY_AND_SPECIFICS_ROUTINES = """
WITH relevant_tasks AS (
  SELECT pt.id AS task_id,
         p2n.nomad_job_id
  FROM hrwsi.processing_tasks pt
  INNER JOIN hrwsi.trigger_validation tv ON pt.trigger_validation_fk_id = tv.id
  INNER JOIN hrwsi.raw2valid rv ON rv.trigger_validation_id = tv.id
  INNER JOIN hrwsi.raw_inputs ri ON ri.id = rv.raw_input_id
  LEFT JOIN hrwsi.processingtask2nomad p2n ON p2n.processing_task_id = pt.id
  WHERE tv.triggering_condition_name IN {}
    AND ri.measurement_day = {}
    AND pt.has_ended = false
),

-- Count tasks without linked nomad_job
task_count AS (
  SELECT COUNT(*) AS count1
  FROM relevant_tasks
  WHERE nomad_job_id IS NULL
),

-- Count latest non-launch statuses
status_count AS (
  SELECT COUNT(*) AS count2
  FROM (
    SELECT DISTINCT ON (nomad_job_dispatch_fk_id) processing_status_id
    FROM hrwsi.processing_status_workflow
    WHERE nomad_job_dispatch_fk_id IN (
      SELECT nomad_job_id FROM relevant_tasks WHERE nomad_job_id IS NOT NULL
    )
    ORDER BY nomad_job_dispatch_fk_id, date DESC
  ) latest_status
  WHERE latest_status.processing_status_id NOT IN (2, 6)
)

-- Final sum of both counts
SELECT task_count.count1 + status_count.count2 AS total
FROM task_count, status_count;
"""


GET_LAST_MEASUREMENT_DATE_INPUT="""SELECT ri.start_date FROM hrwsi.raw_inputs ri
WHERE ri.product_type_code='{}'
ORDER BY ri.start_date DESC LIMIT 1;"""

GET_LAST_PUBLISHING_DATE_INPUT = """SELECT ri.publishing_date FROM hrwsi.raw_inputs ri
WHERE ri.product_type_code='{}'
ORDER BY ri.start_date DESC LIMIT 1;"""
