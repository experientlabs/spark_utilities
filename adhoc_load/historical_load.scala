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
