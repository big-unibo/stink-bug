package it.unibo.big.environment_registry

object GenerateEnvironmentRegistryDimensionTables {
  import it.unibo.big.DimensionsTableUtils
  import it.unibo.big.Utils.getGeometryColumn
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

  //Distance threshold in meters, for the join between the trap and the environment registry
  private val DISTANCE_THRESHOLD = 200
  /**
   * Configuration for the environment registry dimension tables
   *
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
   * @param trapsDf the case trap table dataframe
   * @return a tuple with the dimension table and the bridge table
   */
  private def createDimensionTables(sparkSession: SparkSession, conf: EnvironmentRegistryConfiguration, environmentRegistryInputData: Map[String, DataFrame], trapsDf: DataFrame): (DataFrame, DataFrame) = {
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    val inputDataFrame = environmentRegistryInputData(conf.table_name)
    var dimensionTable = inputDataFrame
    val trapsDfNew = getGeometryColumn("geometry", trapsDf.where(col("geometry").isNotNull))//.withColumn("geometry", getGeomGeospark(col("geometry")))

    val columnsMapping = conf.columns_mapping
    for ((c1, c2) <- columnsMapping) {
      dimensionTable = dimensionTable.withColumnRenamed(c1, c2)
    }
    dimensionTable = getGeometryColumn(conf.geometryName, dimensionTable)//.withColumn(conf.geometryName, getGeomGeospark(col(conf.geometryName)))
    var bridgeTable = trapsDfNew.join(dimensionTable, "ms_id")
      .withColumn("distance", expr(s"ST_Distance(${conf.geometryName}, geometry)"))
      .where(col("distance") <= DISTANCE_THRESHOLD)
        //ST_DISTANCE(ST_Transform(d.${conf.geometryName}, 4326), ST_Transform(t.geometry, 4326))
    val windowSpec = Window.orderBy(columnsMapping.values.toSeq.map(col): _*)
    val distrinctDimensionTable = dimensionTable.select(columnsMapping.values.map(col).toSeq :_*)
      .distinct().withColumn(conf.identifier, row_number().over(windowSpec))
    //join bridge table in all the columns on columns mapping
    val joinConditions = columnsMapping.keys.map(c => distrinctDimensionTable(c) === bridgeTable(columnsMapping(c)))
    bridgeTable = bridgeTable.join(distrinctDimensionTable, joinConditions.reduce((a, b) => a && b))
      //TODO .select(bridgeTable("gid"), distrinctDimensionTable(conf.identifier))
      .distinct()
    //add link to the trap that not have a value in the bridge
    val (newDimensionTable, newBridgeTable) = DimensionsTableUtils.addNotNearRows(sparkSession, conf.identifier, dimensionTable, bridgeTable, trapsDfNew)

    (newDimensionTable, newBridgeTable)
  }
}

