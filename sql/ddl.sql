DROP TABLE IF EXISTS job;
CREATE TABLE job
(
  job_id int not null auto_increment,
  job_name varchar(50),
  folder varchar(20),
  application varchar(50),
  sub_application varchar(50),
  run_as varchar(50),
  node_id varchar(50),
  max_wait tinyint,
  cmd_line varchar(500),
  create_date datetime,
  update_date datetime,
  PRIMARY KEY (job_id)
);

DROP TABLE IF EXISTS job_params;
CREATE TABLE job_params
(
  job_id int not null,
  param_type varchar(50),
  param_name varchar(50),
  param_value varchar(1000),
  create_date datetime,
  update_date datetime,
  PRIMARY KEY(job_id, param_type, param_name)
);

DROP TABLE IF EXISTS job_execution_ajf;
CREATE TABLE job_execution_ajf
(
  job_id int not null,
  job_name varchar(50),
  order_id varchar(10),
  folder varchar(20),
  application varchar(50),
  sub_application varchar(50),
  order_date varchar(10),
  start_time varchar(30),
  end_time varchar(30),
  host varchar(50),
  run_as varchar(50),
  execution_status varchar(200),
  source_origin varchar(50),
  create_date datetime,
  update_date datetime,
  PRIMARY KEY (job_name, order_id, order_date)
);

DROP TABLE IF EXISTS job_execution_cstat;
CREATE TABLE job_execution_cstat
(
  job_id int not null,
  job_name varchar(50),
  order_id varchar(10),
  folder varchar(20),
  application varchar(50),
  sub_application varchar(50),
  order_date varchar(10),
  start_time varchar(30),
  end_time varchar(30),
  host varchar(50),
  run_as varchar(50),
  execution_status varchar(200),
  source_origin varchar(50),
  create_date datetime,
  update_date datetime,
  PRIMARY KEY (job_name, order_id, order_date, start_time)
);

DROP TABLE IF EXISTS job_temporary;
CREATE TABLE job_temporary
(
  execution_date varchar(10),
  job_name varchar(50),
  uuaa varchar(10),
  job_frecuency varchar(10),
  job_type varchar(20),
  job_sub_type varchar(20),
  sub_application varchar(50),
  dynamic_job_name varchar(20),
  json_name varchar(100),
  job_size varchar(10),
  description varchar(200),
  order_type varchar(20),
  calendar_type varchar(50),
  order_time varchar(10),
  order_day varchar(10),
  order_max_days tinyint,
  job_incondition varchar(50),
  job_outcondition varchar(50),
  automation_request varchar(500),
  job_params varchar(500),
  execution_account varchar(10),
  success_email varchar(200),
  fail_email varchar(200),
  employee_code varchar(10),
  scrum_team varchar(100),
  scrum_master varchar(100),
  responsible varchar(100),
  create_date datetime,
  update_date datetime,
  PRIMARY KEY (execution_date, job_name)
);

DROP TABLE IF EXISTS folder_inventory;
CREATE TABLE folder_inventory
(
  reservation_date varchar(20),
  folder varchar(50),
  uuaa varchar(10),
  frecuency varchar(20),
  scrum_team varchar(200),
  responsible varchar(100),
  status varchar(20),
  create_date datetime,
  update_date datetime,
  PRIMARY KEY (reservation_date, folder, scrum_team)
);

DROP TABLE IF EXISTS job_inventory;
CREATE TABLE job_inventory
(
  job_name varchar(50),
  job_type varchar(50),
  json_name varchar(1000),
  frecuency varchar(20),
  source varchar(1000),
  description varchar(4000),
  input_path varchar(3000),
  output_path varchar(3000),
  storage_zone varchar(50),
  origin varchar(1000),
  scrum_team varchar(500),
  responsible varchar(200),
  use_case varchar(200),
  domain varchar(50),
  monitoring_team varchar(50),
  complexity varchar(50),
  is_critical varchar(10),
  create_date datetime,
  update_date datetime,
  PRIMARY KEY(job_name)
);

DROP TABLE IF EXISTS job_error;
CREATE TABLE job_error
(
  report_date varchar(20),
  uuaa varchar(20),
  job_name varchar(50),
  folder varchar(50),
  application varchar(50),
  sub_application varchar(50),
  order_id varchar(20),
  order_date varchar(20),
  start_time varchar(50),
  end_time varchar(50),
  json_name varchar(100),
  dependency varchar(100),
  source_origin varchar(50),
  error_type varchar(1000),
  error_reason varchar(1000),
  action varchar(200),
  project_name varchar(100),
  scrum_master varchar(100),
  create_date datetime,
  update_date datetime
)