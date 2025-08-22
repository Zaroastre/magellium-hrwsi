----------------------------------
----- PROCESSING TASKS -----------
----------------------------------

--- COLD TABLES ---

/*
Table for the processing status codes.
List all processing status code that describe the evolution of the processing tasks.
These processing status are representative of what we expect to encoutner for processing
tasks when monitoring the HRWSI oprationnal production system.
Terminated status is used after number of error >= nb_max_of_error for an input.
Input with Terminated status will no longer be restarted.
*/
CREATE TABLE hrwsi.processing_status (

  id smallint UNIQUE,
  code bigint NOT NULL UNIQUE,
  name text NOT NULL UNIQUE,
  description text
);
INSERT INTO hrwsi.processing_status VALUES
  (1, 0, 'started', ''),
  (2, 1, 'processed', ''),
  (3, 2, 'pending', ''),
  (4, 110, 'internal_error', ''),
  (5, 210, 'external_error', ''),
  (6, 99, 'terminated', '');

--- CORE TABLES ---

/*
Table for the processing tasks.
List all the processing tasks, linked to an intput, with all the informations needed.
One input can be linked to several processing tasks.
One processing tasks is associated with one nomad job dispatch.
*/
CREATE TABLE hrwsi.processing_tasks (

  id bigserial UNIQUE,
  trigger_validation_fk_id bigserial REFERENCES hrwsi.trigger_validation(id) UNIQUE,
  creation_date timestamp NOT NULL,
  processing_date bigint,
  preceding_input_id text,
  has_ended boolean,
  intermediate_files_path text
);

/*
Table for nomad job dispatch.
List all nomad job dispatch, linked to the processing task corresponding.
A nomad job dispatch has several processing status workflow depending on the evolution of the processing task.
*/
CREATE TABLE hrwsi.nomad_job_dispatch (

  id uuid UNIQUE,
  nomad_job_dispatch text,
  dispatch_date timestamp,
  log_path text
);

/*
Intermediate table for the processingtask2nomad

*/
CREATE TABLE hrwsi.processingtask2nomad (

  nomad_job_id uuid REFERENCES hrwsi.nomad_job_dispatch(id) NOT NULL,
  processing_task_id bigserial REFERENCES hrwsi.processing_tasks(id) NOT NULL
);

/*
Table for the processing status workflow.
List all processing status, linked to the nomad job dispatch of which they describe the evolution.
*/
CREATE TABLE hrwsi.processing_status_workflow (

  id bigserial UNIQUE,
  nomad_job_dispatch_fk_id uuid REFERENCES hrwsi.nomad_job_dispatch(id) ON DELETE CASCADE NOT NULL,
  processing_status_id smallint REFERENCES hrwsi.processing_status(id) NOT NULL,
  date timestamp NOT NULL,
  message text,
  exit_code int
);



--- TYPES ---

CREATE TYPE hrwsi.nomad_job_dispatch_info AS
  (
    nomad_job_id uuid,
    nomad_job_dispatch text,
    nomad_job_dispatch_creation_date timestamp,
    nomad_job_dispatch_log_file_path text
  );

CREATE TYPE hrwsi.processing_status_history AS
  (
    id bigint,
    processing_status_date timestamp,
    processing_status_code bigint,
    processing_status_name text,
    processing_status_message text
  );

--- FUNCTIONS ---

/*
Not already used because the error handling is not yet implemented
*/
CREATE FUNCTION hrwsi.get_number_of_error_statuses_by_processing_task (processing_task_id_param bigint)
  RETURNS bigint AS $$
-- processing_task_id renamed as processing_task_id_param processing_task_id' to avoid any ambiguity.
  declare
    error_statuses_number bigint;
-- To be used to know if a processing task can't be processed cause of to many errors
  BEGIN
    SELECT COUNT(psw.id) INTO error_statuses_number
    FROM hrwsi.processing_tasks pt
    JOIN hrwsi.processingtask2nomad pn ON pn.processing_task_id = pt.id
    JOIN hrwsi.processing_status_workflow psw ON psw.nomad_job_dispatch_fk_id = pn.nomad_job_id
    WHERE pt.id = processing_task_id_param
    AND psw.processing_status_id in (4,5);

    RETURN error_statuses_number;
  END $$ LANGUAGE plpgsql stable;


CREATE FUNCTION hrwsi.is_processing_task_processed (processing_task_id_param bigint)
  RETURNS boolean AS $$
-- To be used to know if a processing task is processed
-- This function is used in other functions
-- processing_task_id renamed as processing_task_id_param processing_task_id' to avoid any ambiguity.
  BEGIN 
    RETURN CASE WHEN EXISTS (
      SELECT
          pt.id
      FROM hrwsi.processing_tasks pt
      JOIN hrwsi.processingtask2nomad pn ON pn.processing_task_id = pt.id
      JOIN hrwsi.processing_status_workflow psw ON psw.nomad_job_dispatch_fk_id = pn.nomad_job_id
      WHERE psw.processing_status_id = 2
      AND pt.id = processing_task_id_param
    )
    THEN CAST(1 AS bit)
    ELSE CAST(0 AS bit)
    END CASE;
  END $$ LANGUAGE plpgsql stable;


CREATE FUNCTION hrwsi.get_processing_tasks_not_finished ()
    RETURNS setof hrwsi.processing_tasks AS $$
-- To be used to collect all the processing tasks not ended
-- This function is used by the Launcher : if all the tasks are finished then it can stop
BEGIN RETURN query SELECT
    pt.id,
    pt.trigger_validation_fk_id,
    pt.creation_date,
    pt.has_ended,
    pt.intermediate_files_path
FROM hrwsi.processing_tasks pt
WHERE pt.has_ended = False;
END $$ LANGUAGE plpgsql stable;

/*
Not already used because the error handling is not yet implemented
*/
CREATE FUNCTION hrwsi.get_status_history_by_processing_task_id (processing_task_id_param bigint)
    RETURNS setof hrwsi.processing_status_history AS $$
-- To be used to know the status history of a processing task thanks to it's id
-- processing_task_id renamed as processing_task_id_param processing_task_id' to avoid any ambiguity.
BEGIN RETURN query SELECT
    psw.id,
    psw.date,
    ps.code,
    ps.name,
    psw.message
FROM hrwsi.processingtask2nomad pn
JOIN hrwsi.processing_status_workflow psw ON psw.nomad_job_dispatch_fk_id = pn.nomad_job_id
JOIN hrwsi.processing_status ps ON ps.id = psw.processing_status_id
WHERE pn.processing_task_id = processing_task_id_param;
END $$ LANGUAGE plpgsql stable;

/*
Not used yet
*/
CREATE FUNCTION hrwsi.get_nomad_job_dispatch_list_by_processing_task_id (processing_task_id_param bigint)
    RETURNS setof hrwsi.nomad_job_dispatch_info AS $$
-- To be used to know the list of all the nomad job dispatch of a processing task thanks to it's id
-- processing_task_id renamed as processing_task_id_param processing_task_id' to avoid any ambiguity.
BEGIN RETURN query SELECT
    njd.id,
    njd.nomad_job_dispatch,
    njd.dispatch_date,
    njd.log_path
FROM hrwsi.nomad_job_dispatch njd
INNER JOIN hrwsi.processingtask2nomad pn ON pn.nomad_job_id = njd.id
WHERE pn.processing_task_id = processing_task_id_param;
END $$ LANGUAGE plpgsql stable;

/*
Not used yet
*/
CREATE FUNCTION hrwsi.get_current_nomad_job_dispatch_by_processing_task_id (processing_task_id_param bigint)
    RETURNS setof hrwsi.nomad_job_dispatch_info AS $$
-- To be used to know the current nomad job dispatch of a processing task thanks to it's id
-- processing_task_id renamed as processing_task_id_param processing_task_id' to avoid any ambiguity.
BEGIN RETURN query SELECT
    njd.id,
    njd.nomad_job_dispatch,
    njd.dispatch_date,
    njd.log_path
FROM hrwsi.nomad_job_dispatch njd
INNER JOIN hrwsi.processingtask2nomad pn ON njd.id = pn.nomad_job_id
WHERE pn.processing_task_id = processing_task_id_param
ORDER BY njd.id DESC
LIMIT 1;
END $$ LANGUAGE plpgsql stable;

/*
Not used yet
*/
CREATE FUNCTION hrwsi.are_all_processing_tasks_in_vm_processed (vm_id uuid)
  RETURNS setof boolean AS $$
-- To be used to know if a virtual machine has processed all it's processing tasks
  BEGIN RETURN query SELECT
      bool_and(is_processing_task_processed)
      FROM
      (
        SELECT pt.id AS task_id
        FROM hrwsi.processing_tasks pt
        WHERE pt.virtual_machine_id = vm_id
      ) AS x,
      hrwsi.is_processing_task_processed(task_id);
  END $$ LANGUAGE plpgsql stable;

/*
Not used yet
*/
CREATE FUNCTION hrwsi.get_preceding_processing_task (processing_task_id bigint)
    RETURNS setof hrwsi.processing_tasks AS $$
-- To be used to know the preceding processing task of a processing task
-- thanks to it's id
-- This function is used in another function
    BEGIN RETURN query SELECT
        pt.id,
        pt.trigger_validation_fk_id,
        pt.virtual_machine_id,
        pt.creation_date,
        pt.preceding_input_id,
        pn.nomad_job_id,
        pt.has_ended,
        pt.intermediate_files_path
    FROM hrwsi.processing_tasks pt,
    (
      SELECT pt.preceding_input_id AS piid
      FROM hrwsi.processing_tasks pt
      WHERE pt.id = processing_task_id
    ) AS x
    INNER JOIN hrwsi.processingtask2nomad pn ON pt.id = pn.processing_task_id
    WHERE pt.input_fk_id = piid;
    END $$ LANGUAGE plpgsql stable;

/*
Not used yet
*/
CREATE FUNCTION hrwsi.is_preceding_processing_task_processed (processing_task_id bigint)
  RETURNS setof boolean AS $$
-- To be used to know if the preceding processing task of a processing task is processed
-- This function is used in another function
  BEGIN RETURN query SELECT
      is_processing_task_processed
      FROM
      (
        SELECT id AS precedingid
        FROM hrwsi.get_preceding_processing_task(processing_task_id)
      ) AS x,
      hrwsi.is_processing_task_processed(precedingid);
  END $$ LANGUAGE plpgsql stable;


CREATE FUNCTION hrwsi.is_one_processing_task_processed_for_an_input (input_id text)
  RETURNS setof boolean AS $$
-- To be used to know if at least one processing task is processed for a given input
-- This function is used in another function
  BEGIN RETURN query SELECT  
      bool_or(is_processing_task_processed)
      FROM 
      (
        SELECT pt.id AS task_id
        FROM hrwsi.processing_tasks pt
        INNER JOIN hrwsi.raw2valid r2v ON r2v.trigger_validation_id = pt.trigger_validation_fk_id
        WHERE r2v.raw_input_id = input_id
      ) AS x, 
      hrwsi.is_processing_task_processed(task_id);
  END $$ LANGUAGE plpgsql stable;


CREATE FUNCTION hrwsi.are_all_processing_tasks_ended_for_an_input (input_id text)
  RETURNS boolean AS $$
-- To be used to know if all processing tasks of a given input are ended
-- This function is used in another function
  BEGIN 
    RETURN CASE WHEN EXISTS (
      SELECT
          id
      FROM hrwsi.get_processing_tasks_not_finished() 
      INNER JOIN hrwsi.raw2valid rv on trigger_validation_fk_id = rv.trigger_validation_id
      WHERE rv.raw_input_id = input_id
    )
    THEN CAST(0 AS bit)
    WHEN EXISTS (
      SELECT
          pt.id
      FROM hrwsi.processing_tasks pt
      INNER JOIN hrwsi.raw2valid rv on pt.trigger_validation_fk_id = rv.trigger_validation_id
      WHERE rv.raw_input_id = input_id
    )
    THEN CAST(1 AS bit)
    ELSE CAST(0 AS bit)
    END CASE;
  END $$ LANGUAGE plpgsql stable;


/*
Not used yet
*/
CREATE FUNCTION hrwsi.get_ids_of_processing_tasks_ready_to_be_launched ()
    RETURNS setof bigint AS $$
-- To be used to get ids of processing tasks without Nomad job and with 
-- preceding task processed or NULL
-- The Launcher uses this function to know for which tasks he must create a nomad job
    BEGIN RETURN query SELECT
        pt.id 
        FROM hrwsi.processing_tasks pt 
        LEFT JOIN hrwsi.nomad_job_dispatch njd
        ON pt.id=njd.processing_task_fk_id
        WHERE pt.preceding_input_id is NULL AND njd.id is NULL
        UNION
        SELECT ptid
        FROM
        (
            SELECT pt.id AS ptid, pt.preceding_input_id AS ptpid, pt.nomad_job_id AS ptjid
            FROM hrwsi.processing_tasks pt
            LEFT JOIN hrwsi.nomad_job_dispatch njd
            ON pt.id=njd.processing_task_fk_id
            WHERE njd.id is NULL
        ) AS x, 
        hrwsi.is_preceding_processing_task_processed(ptid)
        WHERE is_preceding_processing_task_processed=true; 
    END $$ LANGUAGE plpgsql stable;


CREATE FUNCTION hrwsi.get_id_of_unprocessed_inputs_with_all_pt_ended (pt_creation_date timestamp)
    RETURNS setof text AS $$
-- To be used to give id of input with all processing tasks created since the date "pt_creation_date" 
-- ended but no one is processed.
-- Th Orchestrator uses this function to identify tasks that need to be planned
    BEGIN RETURN query SELECT 
        raw_input_id
        FROM 
        (
            SELECT r2v.raw_input_id AS raw_input_id
            FROM hrwsi.raw2valid r2v
            INNER JOIN hrwsi.processing_tasks pt ON pt.trigger_validation_fk_id = r2v.trigger_validation_id
            WHERE pt.has_ended=True
            AND pt.creation_date > pt_creation_date
        ) AS x,
        hrwsi.is_one_processing_task_processed_for_an_input(raw_input_id),
        hrwsi.are_all_processing_tasks_ended_for_an_input(raw_input_id)
        WHERE is_one_processing_task_processed_for_an_input = False 
        AND are_all_processing_tasks_ended_for_an_input = True;
    END $$ LANGUAGE plpgsql stable;