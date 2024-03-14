package it.unibo.big.environment_registry

object GenerateEnvironmentRegistryDimensionTables {
  import it.unibo.big.DimensionsTableUtils
  import it.unibo.big.Utils.readGeometry
  import org.apache.spark.sql.expressions.UserDefinedFunction
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.expressions.Window

  /**
   * Configuration for the environment registry dimension tables
   * @param columns_mapping columns mapping from the input dataframe to the dimension table
   * @param geometryName name of the geometry column
   * @param identifier name of the identifier column
   * @param table_name name of the table
   */
  private case class EnvironmentRegistryConfiguration(columns_mapping: Map[String, String], geometryName: String, identifier: String, table_name: String)

  /**
   * @param sparkSession the spark session
   * @param environmentRegistryInputData a map where for each environment registry table there is a dataframe
   * @param dimTrapDf the trap dimension dataframe
   * @return a map where for each environment registry table there is a tuple with the dimension table and the bridge table
   */
  def apply(sparkSession: SparkSession, environmentRegistryInputData: Map[String, DataFrame], dimTrapDf: DataFrame): Map[String, (DataFrame, DataFrame)] = {
    val configurations = Seq(
      EnvironmentRegistryConfiguration(
        Map(
          "type_name" -> "type_name"
        ),
        "geom4326",
        "water_basin_id",
        "water_basin"
      ),
      EnvironmentRegistryConfiguration(
        Map(
          "praenomen" -> "praenomen",
          "usage" -> "usage",
          "culverted" -> "culverted"
        ),
        "geom4326",
        "water_course_id",
        "water_course"
      ),
      EnvironmentRegistryConfiguration(
        Map(
          "crop_type" -> "crop_type"
        ),
        "geom4326",
        "crop_usage_id",
        "crop"
      )
    )
    configurations.map(c => {
      val (dimensionTable, bridgeTable) = createDimensionTables(sparkSession, c, environmentRegistryInputData, dimTrapDf)
      c.table_name -> (dimensionTable, bridgeTable)
    }).toMap
  }

  /**
   * Create the dimension tables and the bridge tables.
   * Considering the parameters in the configuration, it renames the columns of the input dataframe and
   * creates the bridge table joining the input dataframe with the trap dimension dataframe.
   *
   * @param sparkSession the spark session
   * @param conf the configuration of the environment registry table
   * @param environmentRegistryInputData a map where for each environment registry table there is a dataframe
   * @param dimTrapDf the trap dimension dataframe
   * @return a tuple with the dimension table and the bridge table
   */
  private def createDimensionTables(sparkSession: SparkSession, conf: EnvironmentRegistryConfiguration, environmentRegistryInputData: Map[String, DataFrame], dimTrapDf: DataFrame): (DataFrame, DataFrame) = {
    val inputDataFrame = environmentRegistryInputData(conf.table_name)
    var dimensionTable = inputDataFrame
    val columnsMapping = conf.columns_mapping
    for ((c1, c2) <- columnsMapping) {
      dimensionTable = dimensionTable.withColumnRenamed(c1, c2)
    }

    // User defined function to check if two geometries are near in a 200 meters radius
    val distance: UserDefinedFunction = udf((geom1: String, latitude: Double, longitude: Double) => {
      val g1 = readGeometry(geom1)
      val g2 = geotrellis.vector.Point.apply(longitude, latitude).withSRID(4326)
      g1.distance(g2)
    })

    val windowSpec = Window.orderBy(columnsMapping.values.toSeq.map(col): _*)
    dimensionTable = dimensionTable.select(columnsMapping.values.map(col).toSeq :_*)
      .distinct().withColumn(conf.identifier, row_number().over(windowSpec))
    var bridgeTable = dimTrapDf.join(inputDataFrame, "ms_id")
      .withColumn("distance", distance(inputDataFrame(conf.geometryName), col("latitude"), col("longitude")))
    //join bridge table in all the columns on columns mapping
    val joinConditions = columnsMapping.keys.map(c => dimensionTable(c) === bridgeTable(columnsMapping(c)))
    bridgeTable = bridgeTable.where(col("distance") <= 200).join(dimensionTable, joinConditions.reduce((a, b) => a && b))
      .select(bridgeTable("gid"), dimensionTable(conf.identifier))
      .distinct()
    //add link to the trap that not have a value in the bridge
    val (newDimensionTable, newBridgeTable) = DimensionsTableUtils.addNotNearRows(sparkSession, conf.identifier, dimensionTable, bridgeTable, dimTrapDf)

    (newDimensionTable, newBridgeTable)
  }
}

