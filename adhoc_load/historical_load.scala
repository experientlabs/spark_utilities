import org.apache.log4j.Logger
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, StringType, BooleanType, TimestampType}


def reorderAndRepartitionByPartitionKey(df: Dataframe, partitionKey: List[String]): Dataframe = {
    val partitionKeyCol = partitionKey.map(x => col(x))
    val columns = df.schema.toList.filterNot(a => partitionKey.contains(a) ++ partitionKey
    df.select(columns.map(col): _*).repartition(partKeyCol: _*)
}

def sortByColumnNames(df: Dataframe): Dataframe = {
    df.select(df.schema.fieldNames.sorted.map(col): _*)
}


val partitionKey = List["category_group", "partition_date"]
val partKey = partitionKey.map(x => x)

val schema = "my_schema_name"
val table = "my_table_name"


#---------------- Backup Table ------------------------------
val backupDF = spark.read.orc(s"gs://my_schema_name/my_table_name")
val reOrderedOutputDFbk = reorderAndRepartitionByPartitionKey(sortByColumnNames(backupDF), partitionKey)

reOrderedOutputDFbk.write.mode(SaveMode.Overwrite).partitionBy(partKey: _*).format("orc").saveAsTable("my_schema_name.my_table_name_26082023")

#------------------------------------------------------------
val df = spark.read.orc(s"gs://${schema}/${table}_26082023")

def updatedColumn(df: Dataframe) = {
df
.withColumn("last_update_ts", when(col("last_update_ts").isNull, lit(current_timestamp())))
.withColumn("col1_null_removal", when(col("col1_null_removal").isNull, lit(" ")))
.withColumn("col2_null_removal", when(col("col2_null_removal").isNull, lit(" ")))
.withColumn("col3_null_removal", when(col("col3_null_removal").isNull, lit(" ")))
.withColumn("col4_null_removal", when(col("col4_null_removal").isNull, lit(" ")))
}

val df1=updatedColumn(df)

val finalDF = reorderAndRepartitionByPartitionKey(sortByColumnNames(df1), partitionKey)
spark.sql("drop table my_schema_name.my_table_name")
finalDF.write.mode(saveMode.Overwrite).partitionBy(partKey: _*).format("orc").saveAsTable("my_schema_name.my_table_name")