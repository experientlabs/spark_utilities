val df = getMonthlySalesDF
df.count()
df.printSchema()
val new_df = df.withColumn("rep_nm", trim(col("rep_nm")))

val dept_gcs_DF = new_df.filter(col("department").equalto("gcs")).drop(col("department"))
val dept_pcs_DF = new_df.filter(col("department").equalto("pcs")).drop(col("department"))

createTable(sortByColumnNames(dept_gcs_DF), "orc", "my_schema_name.my_gcs_table_name")
createTable(sortByColumnNames(dept_pcs_DF), "orc", "my_schema_name.my_pcs_table_name")

val dept_gcs_df = spark.table("my_schema_name.my_gcs_table_name")
val dept_pcs_df = spark.table("my_schema_name.my_pcs_table_name")

def bqWriter(df:Dataframe, table:String): Unit = {
  var bucket = "my_gcs_bucket_name"
  df.write.format("com.google.cloud.spark.bigquery")
    .option("parentProject", "ems-u390093403990809somedemotext")
    .option("temporaryGcsbucket", bucket)
    .option("project","ems-u390093403990809somedemotext")
    .mode("overwrite").save(table)
}

bqWriter(dept_gcs_DF, "bq_Schema_name.bq_gcs_table_name")
bqWriter(dept_pcs_DF, "bq_Schema_name.bq_pcs_table_name")