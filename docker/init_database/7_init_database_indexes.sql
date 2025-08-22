---------------
--- INDEXES ---
---------------

-- hrwsi.raw_inputs
CREATE INDEX IF NOT EXISTS ix_raw_inputs_id_product_type_code ON hrwsi.raw_inputs USING btree (product_type_code, id);
CREATE UNIQUE INDEX IF NOT EXISTS raw_inputs_id_key ON hrwsi.raw_inputs USING btree (id);

-- hrwsi.triggering_condition
CREATE UNIQUE INDEX IF NOT EXISTS triggering_condition_name_key ON hrwsi.triggering_condition USING btree (name);

-- hrwsi.processing_routine
CREATE UNIQUE INDEX IF NOT EXISTS processing_routine_name_key ON hrwsi.processing_routine USING btree (name);

-- hrwsi.trigger_validation
CREATE INDEX IF NOT EXISTS ix_trigger_validation_id_triggering_condition_name ON hrwsi.trigger_validation USING btree (triggering_condition_name, id);
CREATE UNIQUE INDEX IF NOT EXISTS trigger_validation_id_key ON hrwsi.trigger_validation USING btree (id);

-- hrwsi.products
CREATE INDEX IF NOT EXISTS ix_products_id_product_type_code ON hrwsi.products USING btree (product_type_code, id);
CREATE UNIQUE INDEX IF NOT EXISTS products_id_key ON hrwsi.products USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS products_product_path_key ON hrwsi.products USING btree (product_path);

-- hrwsi.raster_type
CREATE UNIQUE INDEX IF NOT EXISTS raster_type_product_type_key ON hrwsi.raster_type USING btree (product_type);

-- hrwsi.processing_tasks
CREATE UNIQUE INDEX IF NOT EXISTS processing_tasks_id_key ON hrwsi.processing_tasks USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS processing_tasks_trigger_validation_fk_id_key ON hrwsi.processing_tasks USING btree (trigger_validation_fk_id);

-- hrwsi.nomad_job_dispatch
CREATE UNIQUE INDEX IF NOT EXISTS nomad_job_dispatch_id_key ON hrwsi.nomad_job_dispatch USING btree (id);

-- hrwsi.processing_status_workflow
CREATE UNIQUE INDEX IF NOT EXISTS processing_status_workflow_id_key ON hrwsi.processing_status_workflow USING btree (id);

-- hrwsi.processing_status
CREATE UNIQUE INDEX IF NOT EXISTS processing_status_code_key ON hrwsi.processing_status USING btree (code);
CREATE UNIQUE INDEX IF NOT EXISTS processing_status_id_key ON hrwsi.processing_status USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS processing_status_name_key ON hrwsi.processing_status USING btree (name);

-- hrwsi.indexation_failure_type
CREATE UNIQUE INDEX IF NOT EXISTS indexation_failure_type_code_key ON hrwsi.indexation_failure_type USING btree (code);
CREATE UNIQUE INDEX IF NOT EXISTS indexation_failure_type_id_key ON hrwsi.indexation_failure_type USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS indexation_failure_type_name_key ON hrwsi.indexation_failure_type USING btree (name);

-- hrwsi.indexation_file_type
CREATE UNIQUE INDEX IF NOT EXISTS indexation_file_type_id_key ON hrwsi.indexation_file_type USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS indexation_file_type_name_key ON hrwsi.indexation_file_type USING btree (name);

-- hrwsi.indexation_json
CREATE UNIQUE INDEX IF NOT EXISTS indexation_json_id_key ON hrwsi.indexation_json USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS indexation_json_path_key ON hrwsi.indexation_json USING btree (path);

-- hrwsi.indexation_workflow
CREATE UNIQUE INDEX IF NOT EXISTS indexation_workflow_id_key ON hrwsi.indexation_workflow USING btree (id);
