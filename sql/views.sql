DROP VIEW IF EXISTS vw_job_dependency;
CREATE VIEW vw_job_dependency AS
SELECT f.job_id father_job_id, 
       f.job_name father_job_name,
       f.folder father_job_folder, 
       j.job_id child_job_id, 
       j.job_name child_job_name,
       j.folder child_job_folder 
  FROM job j
 INNER JOIN job_params jp
    ON j.job_id = jp.job_id
  LEFT JOIN job f
    ON SUBSTR(jp.param_name, 1, INSTR(jp.param_name, "-")-1) =
       f.job_name
 WHERE jp.param_type = "Incondition";
 
DROP VIEW IF EXISTS vw_job_blacklist;
CREATE VIEW vw_job_blacklist AS
SELECT DISTINCT 
       j.job_name,
       bi.uuaa,
       j.folder,
       jb.register_date,
       CASE
	     WHEN j.folder LIKE "%DIA%" THEN
	       "Diario"
	     WHEN j.folder LIKE "%MEN%" THEN
	       "Mensual"
	   END frecuency,
	   CASE
		  WHEN j.sub_application LIKE "%HAMMUR%" THEN
			  "Hammurabi"
		  WHEN j.sub_application LIKE "%DATAX%" THEN
			  "Data X"
			WHEN j.sub_application LIKE "%TRANSF%" THEN
			  "Data X"
			WHEN j.sub_application LIKE "%APX%" THEN
			  "APX"
			WHEN j.folder LIKE "%03" OR j.folder LIKE "%04" THEN
			  "APX"
			WHEN j.sub_application LIKE "%HDFS%" THEN
			  "HDFS"
			WHEN j.sub_application LIKE "%MASTER%" THEN
			  "Ingesta/Procesamiento"
			WHEN j.sub_application LIKE "%RAW%" THEN
			  "Ingesta/Procesamiento"
			WHEN j.sub_application LIKE "INGESTA%" THEN
			  "Ingesta/Procesamiento"
			WHEN j.job_name LIKE "%CP%" THEN
			  "Ingesta/Procesamiento"
			WHEN j.job_name LIKE "%VP0%" THEN
			  "Hammurabi"
			ELSE 
			  "Otros"
	   END execution_category,
	   bi.project_name,
	   DATEDIFF(CURDATE(),jb.register_date) days_in_blacklist
  FROM job_blacklist jb
 INNER JOIN job j 
    ON jb.job_name = j.job_name
 INNER JOIN blacklist_inventory bi
    ON jb.job_name = bi.job_name;