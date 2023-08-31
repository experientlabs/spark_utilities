import org.apache.log4j.Logger
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, StringType, BooleanType, TimestampType}
import org.apache.spark.sql.catalog.Catalog
import java.time.LocalDate
import java.sql.{Date, Timestamp}


def writeDataToEls(df:Dataframe, elasticOptions: Map[String, String], index:String): Unit = {
  df.write.format("org.elasticsearch.spark.sql").mode(SaveMode.Overwrite).options(elasticOptions).save(index)
}

def bqWriter(df:Dataframe, table:String): Unit = {
  var bucket = Constants.DEVBUCKET
  val isProd = Constants.PROD.equalsIgnoreCase(configUtil.getenv())
  if(isProd){
    bucket = Constants.PRODBUCKET
  }
  df.write.format("com.google.cloud.spark.bigquery")
    .option("parentProject", Constants.BQPROJECT)
    .option("temporaryGcsbucket", bucket)
    .option("project", Constants.BQPROJECT)
    .option("partitionField", partition)
    .option("cluster", clusterfield)
    .mode(Constants.OVERWRITE).save(table)
}

def getCatalog(): Catalog = {
  spark.catalog
}


def createOrUpdatePartitionedTable(df: Dataframe,
                                   tableToRefresh: String,
                                   parititionKey: List[String],
                                   format: String): Unit = {
  // Columns need to be reordered as paritionKey needs to be the last column in the DF
  val partKey = partitionKey.map(x => x)
  val reOrderedOutputDF = reorderAndRepartitionByPartitionKey(sortByColumnNames(df), partitionKey)
  if (!getCatalog().tableExists(tableToRefresh))
    {
      // create the table if not exist
      reOrderedOutputDF.write.partitionBy(partKye: _*).format(format).saveAsTable(tableToRefresh)
    } else {
    // insert into table if exist
    reOrderedOutputDF.write.mode(saveMode.Overwrite).format(format).insertInto(tableToRefresh)
  }

}


def createTable(df:Dataframe, format:String, outputTable:String) : Unit = {
  if (outputTable.contains("field_name_order_matters")){
    df.write.mode(saveMode.Overwrite).format(format).saveAsTable(outputTable)
  } else {
    sortByColumnNames(df).write.mode(SaveMode.Overwrite).format(format).saveAsTable(outputTable)
  }
}


def getIcedData(df: Dataframe): Dataframe = {
  val icedDF = df.filter(col("partition_date").geq(getDateArray(1)))
    .filter(col("partition_date").leq(getDateArray(0)))
}