package it.unibo.big

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object SparkToShapefile {
  import Utils._
  def main(args: Array[String]): Unit = {

    // Register GeoSpark SQL types and functions
    GeoSparkSQLRegistrator.registerAll(sparkSession)

    // Assuming df is your Spark DataFrame with geometry column named "geometry"
    val caseInputDataset = Utils.readCaseInputData()
    val df = caseInputDataset("traps")

    // Convert the Spark DataFrame to a Spatial RDD
    val spatialRDD = Adapter.toSpatialRdd(df, "geometry")

    // Save the Spatial RDD as a Shapefile
    spatialRDD.saveAsShapefile("output_path")

    sparkSession.stop()
  }
}