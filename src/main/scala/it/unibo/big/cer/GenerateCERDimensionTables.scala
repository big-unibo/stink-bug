package it.unibo.big.cer

import geotrellis.vector.io.readWktOrWkb
import it.unibo.big.DimensionsTableUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, udf}

object GenerateCERDimensionTables {
  /**
   * Configuration for the CER dimension tables
   * @param columns_mapping columns mapping from the input dataframe to the dimension table
   * @param geometryName name of the geometry column
   * @param identifier name of the identifier column
   * @param table_name name of the table
   */
  private case class CERConfiguration(columns_mapping: Map[String, String], geometryName: String, identifier: String, table_name: String)

  /**
   * @param sparkSession the spark session
   * @param cerInputData a map where for each cer table there is a dataframe
   * @param dimTrapDf the trap dimension dataframe
   * @return a map where for each cer table there is a tuple with the dimension table and the bridge table
   */
  def apply(sparkSession: SparkSession, cerInputData: Map[String, DataFrame], dimTrapDf: DataFrame): Map[String, (DataFrame, DataFrame)] = {
    val configurations = Seq(
      CERConfiguration(
        Map(
          "d_ty_sda" -> "type_name"
        ),
        "geom4326",
        "water_basin_id",
        "water_basin"
      ),
      CERConfiguration(
        Map(
          "praenomen" -> "praenomen",
          "usage" -> "usage",
          "culverted" -> "culverted",
          "d_ty_sda" -> "type_name"
        ),
        "geom4326",
        "water_course_id",
        "water_course"
      ),
      CERConfiguration(
        Map(
          "raggruppam" -> "crop_type"
        ),
        "geom4326",
        "crop_usage_id",
        "crop_usage"
      )
    )
    configurations.map(c => {
      val (dimensionTable, bridgeTable) = createDimensionTables(c, cerInputData, dimTrapDf)
      c.table_name -> (dimensionTable, bridgeTable)
    }).toMap
  }

  /**
   * Create the dimension tables and the bridge tables.
   * Considering the parameters in the configuration, it renames the columns of the input dataframe and
   * creates the bridge table joining the input dataframe with the trap dimension dataframe.
   *
   * @param sparkSession the spark session
   * @param conf the configuration of the cer table
   * @param cerInputData a map where for each cer table there is a dataframe
   * @param dimTrapDf the trap dimension dataframe
   * @return a tuple with the dimension table and the bridge table
   */
  private def createDimensionTables(sparkSession: SparkSession, conf: CERConfiguration, cerInputData: Map[String, DataFrame], dimTrapDf: DataFrame): (DataFrame, DataFrame) = {
    val inputDataFrame = cerInputData(conf.table_name)
    var dimensionTable = inputDataFrame
    val columnsMapping = conf.columns_mapping
    for ((c1, c2) <- columnsMapping) {
      dimensionTable = dimensionTable.withColumnRenamed(c1, c2)
    }

    // User defined function to check if two geometries are near in a 200 meters radius
    val near: UserDefinedFunction = udf((geom1: String, geom2: String) => {
      val g1 = readWktOrWkb(geom1)
      val g2 = readWktOrWkb(geom2)
      g1.distance(g2) < 200
    })

    dimensionTable = dimensionTable.select(columnsMapping.values.map(col).toSeq :_*)
      .distinct().withColumn(conf.identifier, monotonically_increasing_id())
    var bridgeTable = dimTrapDf.join(inputDataFrame, "ms_id")
    //join bridge table in all the columns on columns mapping
    val joinConditions = columnsMapping.keys.map(c => col(c) === col(columnsMapping(c)))
    bridgeTable = bridgeTable.join(inputDataFrame, joinConditions.reduce((a, b) => a && b))
      .where(near(col(conf.geometryName), col("geom")))
      .select(col("gid"), col(conf.identifier))
      .distinct()
    //add link to the trap that not have a value in the bridge
    val (newDimensionTable, newBridgeTable) = DimensionsTableUtils.addNotNearRows(sparkSession, conf.identifier, dimensionTable, bridgeTable, dimTrapDf)

    (newDimensionTable, newBridgeTable)
  }
}
