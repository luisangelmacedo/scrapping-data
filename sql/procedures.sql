DELIMITER //
DROP PROCEDURE IF EXISTS sp_job_merge //
CREATE PROCEDURE sp_job_merge ()
BEGIN
	#Borrando parametria de jobs que ya no existen
	DELETE FROM job_params
	 WHERE job_id 
	    IN (SELECT job_id 
	          FROM job
	         WHERE job_name 
	           NOT IN (SELECT job_name
	                     FROM job_tmp));

  #Borrando jobs que ya no existen
  DELETE FROM job
   WHERE job_name 
     NOT IN (SELECT job_name 
               FROM job_tmp);

  #Reemplazando jobs existentes
  REPLACE INTO job
   SELECT j.*,
          SYSDATE(),
          SYSDATE()
     FROM job_tmp j;
  
  #Borrando la tabla temporal
  DROP TABLE job_tmp;
 
END
//

DROP PROCEDURE IF EXISTS sp_job_params_merge //
CREATE PROCEDURE sp_job_params_merge ()
BEGIN	
	#Borrando parametria no existente
	DELETE FROM job_params
	 WHERE (job_id, param_type, param_value)
	   NOT IN (SELECT job_id, param_type, param_value
	             FROM job_params_tmp);
	
	#Reemplazando parametria para actualizar
	REPLACE INTO job_params
	 SELECT DISTINCT
	        jpt.*,
	        SYSDATE(),
	        SYSDATE()
	  FROM job_params_tmp jpt;
    
	#Borrando tabla temporal
	DROP TABLE job_params_tmp;
END
//

DROP PROCEDURE IF EXISTS sp_folder_inventory_merge //
CREATE PROCEDURE sp_folder_inventory_merge ()
BEGIN
	INSERT INTO folder_inventory
	SELECT reservation_date, 
	       folder,
	       uuaa,
	       frecuency,
	       scrum_team,
	       responsible,
	       status,
	       SYSDATE(),
	       null
	  FROM folder_inventory_tmp t
	    ON duplicate key 
	UPDATE status = t.status,
	       update_date = SYSDATE();
	  DROP TABLE folder_inventory_tmp;
END
//

DROP PROCEDURE IF EXISTS sp_job_temporary_merge //
CREATE PROCEDURE sp_job_temporary_merge ()
BEGIN
	insert ignore into job_temporary
	SELECT execution_date,
	       job_name,
				 uuaa,
				 job_frecuency,
				 job_type,
				 job_sub_type,
				 sub_application,
				 dynamic_job_name,
				 json_name,
				 job_size,
				 description,
				 order_type,
				 calendar_type,
				 order_time,
				 order_day,
				 order_max_days,
				 job_incondition,
				 job_outcondition,
				 automation_request,
				 job_params,
				 execution_account,
				 success_email,
				 fail_email,
				 employee_code,
				 scrum_team,
				 scrum_master,
				 responsible,
				 SYSDATE(),
				 null
	  FROM job_temporary_tmp t;
	  DROP TABLE job_temporary_tmp;
end
//

DROP PROCEDURE IF EXISTS sp_job_inventory_merge //
CREATE PROCEDURE sp_job_inventory_merge ()
BEGIN
	INSERT INTO job_inventory
	SELECT job_name,
	       job_type, 
				 json_name, 
				 frecuency, 
				 source, 
				 description, 
				 input_path, 
				 output_path,
	       storage_zone, 
				 origin, 
				 scrum_team, 
				 responsible,
				 use_case, 
				 domain, 
				 monitoring_team, 
				 complexity, 
				 is_critical,
				 SYSDATE(),
				 null
	  FROM job_inventory_tmp t
	    on duplicate key
	UPDATE json_name = t.json_name,
         monitoring_team = t.monitoring_team,
         scrum_team = t.scrum_team,
         responsible = t.responsible,
         is_critical = t.is_critical,
				 update_date = SYSDATE();
	  DROP TABLE job_inventory_tmp;
end
//

DROP PROCEDURE IF EXISTS sp_job_error_merge //
CREATE PROCEDURE sp_job_error_merge ()
BEGIN
	TRUNCATE TABLE job_error;
	INSERT INTO job_error
	SELECT report_date, 
	       uuaa, 
				 job_name, 
				 folder, 
				 application, 
				 sub_application, 
				 order_id, 
				 order_date, 
				 start_time, 
	       end_time, 
				 json_name, 
				 dependency, 
				 source_origin, 
				 error_type, 
				 error_reason, 
				 action, 
				 project_name, 
				 scrum_master,
				 SYSDATE(),
				 SYSDATE()
	  FROM job_error_tmp t;
	  DROP TABLE job_error_tmp;
end
//

DROP PROCEDURE IF EXISTS sp_job_execution_ajf_merge //
CREATE PROCEDURE sp_job_execution_ajf_merge ()
BEGIN
	REPLACE INTO job_execution_ajf
	SELECT IFNULL(job_id_y, -1), 
	       job_name,
	       order_id, 
	       folder, 
	       application, 
	       sub_application,
	       order_date, 
	       start_time, 
	       end_time, 
	       host, 
	       run_as, 
	       execution_status,
	       source_origin,
	       SYSDATE(),
	       SYSDATE()
	  FROM job_execution_ajf_tmp;
    DROP TABLE job_execution_ajf_tmp;
END
//

DROP PROCEDURE IF EXISTS sp_job_execution_cstat_merge //
CREATE PROCEDURE sp_job_execution_cstat_merge ()
BEGIN
	REPLACE INTO job_execution_cstat
	 SELECT IFNULL(job_id_y, -1), 
	        job_name,
	        order_id, 
	        folder, 
	        application, 
	        sub_application,
	        order_date, 
	        start_time, 
	        end_time, 
	        host, 
	        run_as, 
	        execution_status,
	        source_origin,
	        SYSDATE(),
	        SYSDATE()
	   FROM job_execution_cstat_tmp tmp;
END
//

DROP PROCEDURE IF EXISTS sp_job_execution //
CREATE PROCEDURE sp_job_execution ()
BEGIN
	DROP TABLE IF EXISTS job_execution;
	CREATE TABLE job_execution as
	SELECT * FROM job_execution_ajf jea
	 where not exists (SELECT 1 
	                     FROM job_execution_cstat jec 
	                    where jea.order_id = jec.order_id 
	                      and jea.job_name = jec.job_name
	                      and jea.order_date = jec.order_date
	                      and jea.start_time = jec.start_time)
	 UNION ALL
	SELECT * FROM job_execution_cstat jec2;
END
//

DROP PROCEDURE IF EXISTS sp_job_execution_report //
CREATE PROCEDURE sp_job_execution_report ()
BEGIN

	#Creando tabla de jobs con sus json respectivos
	DROP TABLE IF EXISTS job_json;
	CREATE TABLE job_json AS
	SELECT
		j.job_id,
		j.job_name,
		CASE
			WHEN jp.param_value LIKE "%-pe-% -o %" THEN
		       SUBSTR(
	       	  	SUBSTR(
	       	  		jp.param_value,
	       	  		INSTR(jp.param_value, '-pe-')-8,
	       	  		LENGTH(jp.param_value)
	       	  	),
	       	  	INSTR(
		       	  	SUBSTR(
		       	  		jp.param_value,
		       	  		INSTR(jp.param_value, '-pe-')-4,
		       	  		LENGTH(jp.param_value)
		       	  	),
		       	  	"-pe-"
	       	  	),
	       	  	INSTR(
		       	  	SUBSTR(
		       	  		jp.param_value,
		       	  		INSTR(jp.param_value, '-pe-')-4,
		       	  		LENGTH(jp.param_value)
		       	  	),
		       	  	" -o "
		       	)-1
	       	  )
			WHEN jp.param_value LIKE "%-gl-% -o %" THEN 
		       SUBSTR(
	       	  	SUBSTR(
	       	  		jp.param_value,
	       	  		INSTR(jp.param_value, '-gl-')-8,
	       	  		LENGTH(jp.param_value)
	       	  	),
	       	  	INSTR(
		       	  	SUBSTR(
		       	  		jp.param_value,
		       	  		INSTR(jp.param_value, '-gl-')-4,
		       	  		LENGTH(jp.param_value)
		       	  	),
		       	  	"-gl-"
	       	  	),
	       	  	INSTR(
		       	  	SUBSTR(
		       	  		jp.param_value,
		       	  		INSTR(jp.param_value, '-gl-')-4,
		       	  		LENGTH(jp.param_value)
		       	  	),
		       	  	" -o "
		       	)-1
	       	  )
			WHEN jp.param_value LIKE "%-ZZ%" THEN
	       	  jp.param_value
			WHEN j.cmd_line LIKE "%transferId%" THEN
	       	  SUBSTR(
	       	  	SUBSTR(
	       	  		j.cmd_line,
	       	  		INSTR(j.cmd_line, "transferId"),
	       	  		LENGTH(j.cmd_line)
	       	  	),
	       	  	INSTR(
	       	  		SUBSTR(
	       	  			j.cmd_line,
	       	  			INSTR(j.cmd_line, "transferId"),
	       	  			LENGTH(j.cmd_line)
	       	  		),
	       	  		" "
	       	  	)+ 1,
	       	  	INSTR(
	       	  		SUBSTR(
	       	  			j.cmd_line,
	       	  			INSTR(j.cmd_line, "transferId"),
	       	  			LENGTH(j.cmd_line)
	       	  		),
	       	  		"-n"
	       	  	)-
	       	  	INSTR(
	       	  		SUBSTR(
	       	  			j.cmd_line,
	       	  			INSTR(j.cmd_line, "transferId"),
	       	  			LENGTH(j.cmd_line)
	       	  		),
	       	  		" "
	       	  	)-3
	       	  )
		END json_name
	FROM job j
	LEFT JOIN job_params jp 
	  ON j.job_id = jp.job_id
	 AND (jp.param_value LIKE "%-pe-% -o %"
	      OR jp.param_value LIKE "%-gl-% -o %")
	 AND jp.param_type = "Variable";

#Creando tabla de inventario de jobs (Tablero 04) limpia
	DROP TABLE IF EXISTS job_inventory_f;
	CREATE TABLE job_inventory_f AS
	SELECT fi.folder, fi.RESERVATION_DATE, fi.SCRUM_TEAM, fi.RESPONSIBLE 
		FROM folder_inventory fi 
	 WHERE (folder, STR_TO_DATE(REPLACE(REPLACE(reservation_date,"//","/"),"20222","2022"),"%d/%m/%Y")) 
			IN (SELECT folder, MAX(STR_TO_DATE(REPLACE(REPLACE(reservation_date,"//","/"),"20222","2022"),"%d/%m/%Y")) 
						FROM folder_inventory fi2
					 GROUP BY folder);

#Creando tabla final del reporte
	DROP TABLE IF EXISTS job_execution_report;
	CREATE TABLE job_execution_report as
	SELECT substr(je.start_time, 1, 6) period,
	       CASE
				   WHEN ji.job_type IS NOT NULL THEN
					   "Reliability"
					 ELSE
					   CASE
						   WHEN fi.reservation_date IS NOT NULL THEN
							   "Proyectos"
							 ELSE
							   CASE
								   WHEN jt.json_name IS NOT NULL THEN
									   "Temporales"
									 ELSE
									   CASE
										   WHEN je.folder LIKE "%03" OR je.folder LIKE "%04" THEN 
											   "APX"
											 ELSE 
											   "Otros"
										 END
								 END
							END
					END execution_type,
					je.job_name,
					IFNULL(jj.json_name, jt.json_name) json_name,
					je.folder,
					CASE
					  WHEN je.sub_applicatiON LIKE "%HAMMUR%" THEN
						  "Hammurabi"
					  WHEN je.sub_applicatiON LIKE "%DATAX%" THEN
						  "Data X"
						WHEN je.sub_applicatiON LIKE "%TRANSF%" THEN
						  "Data X"
						WHEN je.sub_applicatiON LIKE "%APX%" THEN
						  "APX"
						WHEN je.folder LIKE "%03" OR je.folder LIKE "%04" THEN
						  "APX"
						WHEN je.sub_applicatiON LIKE "%HDFS%" THEN
						  "HDFS"
						WHEN je.sub_applicatiON LIKE "%MASTER%" THEN
						  "Ingesta/Procesamiento"
						WHEN je.sub_applicatiON LIKE "%RAW%" THEN
						  "Ingesta/Procesamiento"
						WHEN je.sub_applicatiON LIKE "INGESTA%" THEN
						  "Ingesta/Procesamiento"
						WHEN je.job_name LIKE "%CP%" THEN
						  "Ingesta/Procesamiento"
						WHEN je.job_name LIKE "%VP0%" THEN
						  "Hammurabi"
						ELSE 
						  "Otros"
					END execution_category,
					je.application,
					je.sub_application,
					j.node_id,
					je.order_date,
					je.start_time,
					je.end_time,
		   CASE 
		   	WHEN je.start_time <> " " AND je.end_time <> " " THEN 
		   	  round((str_to_date(je.end_time,"%Y%m%d%H%i%s")-
		   	   str_to_date(je.start_time,"%Y%m%d%H%i%s"))/60,2)
		   END execution_time,
		   CASE
		     WHEN je.folder LIKE "%DIA%" OR je.folder LIKE "%DAY%" THEN 
		       "Diaria"
		     WHEN je.folder LIKE "%MEN%" OR je.folder LIKE "%MON%" THEN
		       "Mensual"
		     WHEN je.folder LIKE "%TMP%" THEN
		       "Temporal"
		     ELSE 
		       "Otros"
		   END frecuency,
		   IFNULL(IFNULL(ji.scrum_team, jt.scrum_team), fi.scrum_team) scrum_team,
		   IFNULL(IFNULL(ji.responsible, jt.scrum_master), fi.responsible) scrum_master,
		   je.execution_status,
		   CASE 
		     WHEN je2.error_type = "" THEN 
		       NULL
		     ELSE 
		       je2.error_type
		   END error_type,
		   je2.error_reason,
		   CASE
			   WHEN jb.register_date IS NULL THEN "No"
			   ELSE "Si"
		   END is_blacklisted,
		   je.source_origin
	  FROM job_execution je
	  LEFT JOIN job j
	    ON je.job_id = j.job_id
	  LEFT JOIN job_inventory ji 
	    ON je.job_name = ji.job_name
	  LEFT JOIN job_error je2  
	    ON je.job_name = je2.job_name 
	   AND je.order_id = je2.order_id 
	   AND je.order_date = je2.order_date 
	  LEFT JOIN job_blacklist jb 
	    ON je.job_name = jb.job_name
	  LEFT JOIN job_json jj
	    ON je.job_id = jj.job_id
	  LEFT JOIN job_inventory_f fi
	    ON je.folder = fi.folder
	  LEFT JOIN job_temporary jt 
	    ON je.job_name = jt.job_name 
	   AND substr(je.start_time,1,8) = jt.execution_date
	 WHERE je.execution_status IN ("NOTOK", "Ended Not OK")
	   AND SUBSTR(je.start_time,1,6) IN (
			DATE_FORMAT(CURDATE(), "%Y%m"),
			DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH), "%Y%m"),
			DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH), "%Y%m"),
			DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH), "%Y%m")
		 );
END
//

DROP PROCEDURE IF EXISTS sp_blacklist_candidates //
CREATE PROCEDURE sp_blacklist_candidates()
BEGIN
	DROP TABLE IF EXISTS job_blacklist_candidates;
	CREATE TABLE job_blacklist_candidates AS
	SELECT DISTINCT jer.* 
	  FROM job_execution_report jer
	  JOIN blacklist_candidates bc 
	    ON jer.job_name = bc.job_name
	   AND str_to_date(substr(jer.start_time, 1, 8), "%Y%m%d") = bc.start_time
	 WHERE jer.execution_type IN ("Proyectos","Reliability");
END
//