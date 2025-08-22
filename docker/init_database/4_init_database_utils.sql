-----------------------------------------------------------------------------

/*
Log level status, expressed as an integer with the same values as in logging.py,
so we can request only the messages with e.g. status > WARNING
*/
CREATE TABLE hrwsi.log_levels (
  id smallint NOT null UNIQUE,
  name text NOT null UNIQUE
);
INSERT INTO hrwsi.log_levels VALUES
  (50, 'CRITICAL'),
  (40, 'ERROR'),
  (30, 'WARNING'),
  (20, 'INFO'),
  (10, 'DEBUG');

--- TRIGGERS ---

/*
Add trigger to capture input insertion on Database in real time
*/
CREATE function hrwsi.notify_input_function()
RETURNS trigger as $$
  BEGIN
    perform pg_notify('input_insertion', row_to_json(new_table)::text)
    FROM new_table;
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_input_trigger
AFTER INSERT ON hrwsi.raw_inputs
REFERENCING NEW TABLE AS new_table 
FOR EACH STATEMENT EXECUTE FUNCTION hrwsi.notify_input_function();

/*
Add trigger to capture processing tasks processed on Database in real time
*/
CREATE function hrwsi.notify_processing_task_processed_function()
RETURNS trigger as $$
  BEGIN
    IF NEW.processing_status_id = 2 THEN
        perform pg_notify('processing_tasks_state_processed', NEW.id::text);
        RETURN NEW;
    ELSE
      RETURN NULL;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_processing_task_processed_trigger
AFTER INSERT ON hrwsi.processing_status_workflow
FOR EACH ROW EXECUTE FUNCTION hrwsi.notify_processing_task_processed_function();

CREATE TRIGGER notify_processing_task_updated_trigger
AFTER UPDATE ON hrwsi.processing_status_workflow
FOR EACH ROW
WHEN (OLD.processing_status_id IS DISTINCT FROM NEW.processing_status_id)
EXECUTE FUNCTION hrwsi.notify_processing_task_processed_function();

/*
Add trigger to capture products on Database in real time
*/
CREATE OR REPLACE FUNCTION hrwsi.product_insert_function()
    RETURNS TRIGGER AS $$
BEGIN
    perform pg_notify('product_insertion', row_to_json(new_table)::text)
    FROM new_table;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_product_trigger
AFTER INSERT ON hrwsi.products
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE FUNCTION hrwsi.product_insert_function();

/*
Add trigger to capture raw_inputs ready to be processed on Database in real time
*/
CREATE OR REPLACE FUNCTION hrwsi.raw2valid_notify_function()
    RETURNS TRIGGER AS $$
BEGIN
    perform pg_notify('raw2valid_insertion', row_to_json(new_table)::text)
    FROM new_table;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_raw2valid_trigger
AFTER INSERT ON hrwsi.raw2valid
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE FUNCTION hrwsi.raw2valid_notify_function();

/*
Add trigger to capture processing tasks on Database in real time
*/
CREATE OR REPLACE FUNCTION hrwsi.processing_task_notify_function()
    RETURNS TRIGGER AS $$
DECLARE
    row RECORD;
    flavour_value text;
BEGIN
    -- Loop through each new row in the new_table
    FOR row IN SELECT * FROM new_table LOOP
        -- Retrieve the flavour value for the current processing task
        SELECT pr.flavour
        INTO flavour_value
        FROM hrwsi.processing_routine pr
        INNER JOIN hrwsi.triggering_condition tc ON pr.name = tc.processing_routine_name
        INNER JOIN hrwsi.trigger_validation tv ON tc.name = tv.triggering_condition_name
        WHERE tv.id = row.id;

        -- Add the flavour value to the notification payload
        PERFORM pg_notify('processing_task_insertion', json_build_object('processing_task', row_to_json(row), 'flavour', flavour_value)::text);
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Ensure the trigger is using the updated function
CREATE TRIGGER notify_processing_task_trigger
AFTER INSERT ON hrwsi.processing_tasks
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE FUNCTION hrwsi.processing_task_notify_function();


/*
Add trigger to capture processing status workflow in error status on Database in real time
*/
CREATE OR REPLACE FUNCTION hrwsi.processing_status_workflow_notify_function()
    RETURNS TRIGGER AS $$
DECLARE
    notification_payload TEXT;
BEGIN
  IF NEW.processing_status_id = 4 OR NEW.processing_status_id = 5 THEN
  -- Collect task corresponding to new status
    SELECT row_to_json(result)
    INTO notification_payload
    FROM (
      SELECT pt.*
      FROM hrwsi.processing_tasks pt
      INNER JOIN hrwsi.processingtask2nomad p2n 
        ON pt.id = p2n.processing_task_id 
      INNER JOIN hrwsi.nomad_job_dispatch njd 
        ON njd.id = p2n.nomad_job_id 
      INNER JOIN hrwsi.processing_status_workflow psw
        ON psw.nomad_job_dispatch_fk_id = njd.id
      WHERE psw.id = NEW.id
    ) AS result;
    perform pg_notify('processing_task_insertion', notification_payload::text);
  END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_processing_status_workflow_trigger
AFTER INSERT ON hrwsi.processing_status_workflow
FOR EACH ROW EXECUTE FUNCTION hrwsi.processing_status_workflow_notify_function();
