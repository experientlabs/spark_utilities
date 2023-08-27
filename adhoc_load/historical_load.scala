import org.apache.log4j.Logger
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, StringType, BooleanType, TimestampType}


def reOrderAndPartitionBy(df: Dataframe, partitionKey: List[String]): Dataframe = {
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
val reOrderedOutputDFbk = reOrderAndPartitionBy(sortByColumnNames(backupDF), partitionKey)

reOrderedOutputDFbk.write.mode(SaveMode.Overwrite).partitionBy(partKey: _*).format("orc").saveAsTable("my_schema_name.my_table_name_26082023")
