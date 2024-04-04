# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from E_LBDP.Claim_Lag where d3_submission_id ='3c42f208-50c7-4ffe-8cc3-fb8dab331cd0' and active_status = 'Y'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from E_LBDP.Claim_Lag where d3_submission_id ='3c42f208-50c7-4ffe-8cc3-fb8dab331cd0':1206
# MAGIC
# MAGIC -- delete from E_LBDP.Claim_Lag where d3_submission_id ='74a6e023-b0b6-4f43-b724-edceafafa060':3694
# MAGIC -- delete from E_LBDP.Enrollment where d3_submission_id ='48158db9-2e7c-45cb-b768-49d32d876014'

# COMMAND ----------

from pyspark.sql.functions import col

# claimDF = spark.table("{0}.claim_lag".format(src_db))
claimDF = spark.table("{0}.claim_lag".format('S_LBDP')).filter(col("d3_submission_id") == '3944496a-3cd2-43ff-b9d6-ef7fa780b65a') #
claimDF.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from R_LBDP.claim_lag where d3_submission_id = '3944496a-3cd2-43ff-b9d6-ef7fa780b65a'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select * from E_LBDP.claim_lag
# MAGIC -- where d3_submission_id= 'd20f846b-cf6c-40e7-949f-b3eeab8e8a69'
# MAGIC
# MAGIC select * from E_LBDP.Paid_Claim where d3_submission_id = '25ba5525-dcd9-465e-9383-f0fd7095f2ca' and active_status = 'N'
# MAGIC --Select * from E_LBDP.Enrollment where d3_submission_id = '9e62394a-3100-42f3-bb35-338946bffbbd' and active_status = 'N'
# MAGIC --select * from E_LBDP.Claim_Lag where d3_submission_id = 'f3acd3bf-a6f3-4d0a-aa20-53e253608196' and active_status = 'N'--and end_date = '2024-02-06'
# MAGIC -- delete from E_LBDP.enrollment
# MAGIC -- where d3_submission_id= 'b82e78cf-4f0c-41e0-a190-3a990eea20ad'
# MAGIC -- -- '16547fcf-0f5f-4fa0-8261-481cbbdf3e39'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select Count(*) from E_LBDP.Claim_Lag
# MAGIC where d3_submission_id ='79f413d4-fd8a-4b23-9547-091589f7714e'
# MAGIC -- select count(*) from E_LBDP.Paid_Claim
# MAGIC -- where  d3_submission_id = '5709b794-25e1-4aea-b3a1-c7565cc3e466'
# MAGIC -- ('9ee7519f-53ab-4e70-84d7-b1db9ff4b986',
# MAGIC -- 'e1942b91-0950-46d1-b741-844ac11b0521'
# MAGIC -- )

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- TRUNCATE  TABLE  R_LBDP.Enrollment  
# MAGIC DELETE FROM E_LBDP.Claim_Lag WHERE d3_submission_id ='79f413d4-fd8a-4b23-9547-091589f7714e'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select COUNT(*) from R_LBDP.Enrollment ;
# MAGIC select COUNT(*) from R_LBDP.Enrollment ;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT(*) from E_LBDP.Enrollment where d3_submission_id = '48158db9-2e7c-45cb-b768-49d32d876014';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select d3_submission_id,active_status,COUNT(*) from E_LBDP.Claim_Lag where d3_submission_id = '79f413d4-fd8a-4b23-9547-091589f7714e'
# MAGIC GROUP BY d3_submission_id,active_status

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.paid_claim_444094898d0745f882e6674e780a2f1c

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from s_lbdp.bad_record_files

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from E_LBDP.Paid_Claim 
# MAGIC where 
# MAGIC d3_submission_id='44409489-8d07-45f8-82e6-674e780a2f1c'
# MAGIC --d3_submission_id='a29acad5-1ea7-4630-95ab-70caee1b2727'
# MAGIC --d3_submission_id='210a1001-ba25-4a68-ab0d-2220ac305452'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from E_LBDP.Enrollment
# MAGIC where d3_submission_id ='48158db9-2e7c-45cb-b768-49d32d876014'

# COMMAND ----------

#  Read delta table and datatype change of columns
from pyspark.sql.functions import col

spark.read.table("S_LBDP.Enrollment")\
    .withColumn("report_date",col("report_date").cast("date"))\
    .write.format("delta").mode("overwrite").option("overwriteSchema",True).saveAsTable("S_LBDP.Enrollment")
spark.read.table("S_LBDP.Enrollment")\
    .withColumn("report_month",col("report_month").cast("date"))\
    .write.format("delta").mode("overwrite").option("overwriteSchema",True).saveAsTable("S_LBDP.Enrollment")

# COMMAND ----------

#  Read delta table and datatype change of columns
from pyspark.sql.functions import col

spark.read.table("s_lbdp.bad_record_files")\
    .withColumn("d3_submission_id", col("d3_submission_id").cast("string"))\
    .write.format("delta").mode("overwrite").option("overwriteSchema",True).saveAsTable("s_lbdp.bad_record_files")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE S_LBDP.Bad_Record_Files ADD COLUMNS (d3_submission_id string);

# COMMAND ----------

spark.read.table("S_LBDP.Enrollment").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE S_LBDP.Enrollment ALTER COLUMN (report_date string)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALter Delta Table
# MAGIC -- ALTER TABLE S_LBDP.Bad_Record_Files ADD COLUMNS (column_info string);
# MAGIC select * from  S_LBDP.Bad_Record_Files 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from S_LBDP.Bad_Record_Files where date = '2023-11-29'
# MAGIC delete from  S_LBDP.Bad_Record_Files where date = '2023-12-08'

# COMMAND ----------

dbutils.fs.ls('/mnt/dls01-lbdp-lockton/data')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- describe history E_LBDP.Enrollment
# MAGIC RESTORE TABLE E_LBDP.Enrollment TO VERSION AS OF 4
# MAGIC --RESTORE TABLE E_DB_LBDP.paid_claim TO VERSION AS OF 4
# MAGIC --select * from E_DB_LBDP.paid_claim version as of 4
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC desc  S_LBDP.Enrollment
# MAGIC -- where d3_submission_id = 'f40d2af7-3326-46b0-9488-cbf6cf70d674

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/lockton_lbdp_capg_adls/lbdp-lockton/data/test/datamap1.csv")
display(df.createOrReplaceTempView("sql_view"))
IMfinalDataDF = spark.sql(
 f"""
 select _c0 as CarrierName, concat(concat(_c1,',',_c2,','),_c3) as PrimaryMappingFields,_c4 as BaseMappingFields,'20231030'as ETLCreatedDate,'20231030' as ETLUpdatedDate from sql_view
where _c0 <> 'Delta Dental of CA'
 """)
 display(IMfinalDataDF)

# COMMAND ----------

IMfinalDataDF = spark.sql(
 

# COMMAND ----------

display(df.createOrReplaceTempView("sql_view"))

# COMMAND ----------

IMfinalDataDF = spark.sql(
 f"""
 select _c0 as CarrierName, concat(concat(_c1,',',_c2,','),_c3) as PrimaryMappingFields,_c4 as BaseMappingFields,'20231030'as ETLCreatedDate,'20231030' as ETLUpdatedDate from sql_view
where _c0 <> 'Delta Dental of CA'
 """)

# COMMAND ----------

display(IMfinalDataDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select _c0,_c1+','+_c2 from sql_view
# MAGIC select _c0, concat(concat(_c1,',',_c2,','),_c3),_c4 from sql_view
# MAGIC where _c0 <> 'Delta Dental of CA'

# COMMAND ----------

INSERT INTO [S_FC].[Carrier_Account_Structure_Map]
(CarrierName,PrimaryMappingFields,BaseMappingFields)
select 'Aetna','Customer_Structure_1,Customer_Structure_3, Customer_Structure_5','Customer_Structure_3'
UNION
select 'Cigna','Customer_Structure_1,Customer_Structure_2,Customer_Structure_3','Customer_Structure_1'
UNION
select 'VSP','Customer_Structure_1,Customer_Structure_2,Customer_Structure_3','Customer_Structure_1'
UNION
select 'Delta Dental of CA','Customer_Structure_1,Customer_Structure_2','Customer_Structure_1'
UNION
select 'Delta Dental of IL','Customer_Structure_1,Customer_Structure_2,Customer_Structure_3,Customer_Structure_4','Customer_Structure_1'
UNION
select 'Delta Dental of VA','Customer_Structure_1,Customer_Structure_2,Customer_Structure_3','Customer_Structure_1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from E_LBDP.claim_lag -- 3720,4741,3816
# MAGIC
# MAGIC -- where d3_submission_id = 'fc0cd9ef-7fcf-4b49-a421-55c04f915ec3' -- 49
# MAGIC -- where d3_submission_id = 'a1d1bfec-069a-41db-8e18-debc7ff63d8a' -- 26064
# MAGIC --  where d3_submission_id = '6203df68-d381-4ef9-82e9-920684e3e0b3'-- 26064

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE from E_LBDP.paid_claim where d3_submission_id = '6203df68-d381-4ef9-82e9-920684e3e0b3' -- 52128
# MAGIC -- DELETE from E_LBDP.paid_claim where d3_submission_id = 'a1d1bfec-069a-41db-8e18-debc7ff63d8a' -- 26064
# MAGIC select distinct(d3_submission_id) from E_LBDP.paid_claim
# MAGIC
# MAGIC -- select * from R_LBDP.paid_claim,2e186c15-26d8-4d76-9f12-23400752a64a

# COMMAND ----------

# MAGIC %sql
# MAGIC -- claim_lag
# MAGIC -- enrollment
# MAGIC -- paid_claim
# MAGIC -- R_LBDP : Raw DB
# MAGIC -- S_LBDP : SIlver DB
# MAGIC E_LBDP : Gold DB

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from E_LBDP.enrollment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE E_LBDP.claim_lag
# MAGIC -- DROP TABLE R_LBDP.claim_lag
# MAGIC -- DROP TABLE R_LBDP.enrollment
# MAGIC -- DROP TABLE S_LBDP.paid_claim
# MAGIC -- DROP TABLE S_LBDP.claim_lag
# MAGIC -- DROP TABLE S_LBDP.enrollment
# MAGIC -- TRUNCATE TABLE  E_LBDP.paid_claim
# MAGIC -- TRUNCATE TABLE  E_LBDP.claim_lag
# MAGIC TRUNCATE TABLE  E_LBDP.enrollment

# COMMAND ----------

# MAGIC %sql
# MAGIC select * form  R_LBDP.claim_lag;

# COMMAND ----------



