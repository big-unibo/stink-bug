package it.unibo.big.normalized_fact

private[normalized_fact] object MeteoUtils {
  import geotrellis.vector.io.readWktOrWkb
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.{DataFrame, SparkSession}

  //Constants and utilities for the normalization of the data
  val averageDayTemperature: String = "t_day_avg"
  val meteoInfos: Seq[String] = List(averageDayTemperature, "t_day_max", "t_day_min",
    "u_day_avg", "u_day_max", "u_day_min",
    "prec_day", "rad_day", "evapo_trans",
    "wind_direction_day", "wind_speed_day_avg", "wind_speed_day_max")
  val temperatureHour: String = "t_hour"
  val usefulHours: String = "useful_hours"
  val gradeDay: String = "degree_day"
  val gradeDay_lowerBound: Double = 12.2

  /**
   *
   * @param df                  the dataframe to add the useful hours and group the data
   * @param trapInstallationDf the dataframe with the trap installation data
   * @param meteoAdditionalInfo the additional info to add to the meteo data
   * @return the dataframe with the useful hours and the grouped data
   */
  def addUsefulHoursColumnAndGroupData(df: DataFrame, trapInstallationDf: DataFrame, meteoAdditionalInfo: String = "", struct: StructType, weatherDf: DataFrame): DataFrame = {

    val joinCondition = col("lat") === col("latW") &&
      col("long") === col("lonW") &&
      col("date") > col("pastDate") && col("date") <= col("m")

    val groupedData = struct.slice(0, 7).map(x => col(x.name))

    var tmpDf = df.join(weatherDf, joinCondition).withColumn(usefulHours,
        when(col("product_type").equalTo(temperatureHour) &&
          col("value").between(15, 35), 1).otherwise(0))
      .withColumn(gradeDay, when(col("product_type").equalTo(averageDayTemperature) &&
        col("value") > gradeDay_lowerBound, col("value") - gradeDay_lowerBound).otherwise(0))
      .groupBy(groupedData :+ col("product_type"): _*)
      .agg(when(col("product_type").equalTo("prec_day"),
        sum(col("value"))).otherwise(avg(col("value"))).as("value"),
        sum(col(usefulHours)).as(usefulHours), sum(col(gradeDay)).as(gradeDay))
      .groupBy(groupedData: _*)
      .pivot("product_type").agg(first("value").as("value"),
        max(col(usefulHours)).as(usefulHours), max(col(gradeDay)).as(gradeDay)) //max perchè prende valori a 0 per altri campi meteo diversi da temperatura oraria e media
      .withColumnRenamed(s"${temperatureHour}_$usefulHours", s"$meteoAdditionalInfo$usefulHours")
      .withColumnRenamed(s"${averageDayTemperature}_$gradeDay", s"$meteoAdditionalInfo$gradeDay")
      .drop(meteoInfos.map(x => s"${x}_$usefulHours") ++ meteoInfos.map(x => s"${x}_$gradeDay") :+ s"${temperatureHour}_$gradeDay" :+ s"${temperatureHour}_value": _*)

    val tmpColumns = tmpDf.columns ++ Seq(s"cum_$usefulHours", s"cum_$gradeDay")
    val startingComulatives = trapInstallationDf.groupBy("gid").agg(sum(gradeDay).as("cum_dd"), sum(usefulHours).as("cum_uh"))

    tmpDf = tmpDf.join(startingComulatives, Seq("gid"), "left")
      //generate cumulative sum of useful hours and grade day
      .withColumn(s"cum_$gradeDay", col("cum_dd") + sum(col(gradeDay)).over(Window.partitionBy("gid").orderBy("date")))
      .withColumn(s"cum_$usefulHours", col("cum_uh") + sum(col(usefulHours)).over(Window.partitionBy("gid").orderBy("date")))
      .select(tmpColumns.head, tmpColumns.tail: _*) //select only columns of the original dataframe

    for (c <- meteoInfos) {
      tmpDf = tmpDf.withColumnRenamed(s"${c}_value", s"$meteoAdditionalInfo$c")
    }
    tmpDf
  }

  /**
   * Get the installation weather dataframe, with meteo data before installation of the trap
   * @param sparkSession the spark session
   * @param instDf the installation dataframe
   * @param weatherDf the weather dataframe
   * @return the installation dataframe enriched with weather data
   */
  def getInstallationWeatherDataframe(sparkSession: SparkSession, instDf: DataFrame, weatherDf: DataFrame): DataFrame = {
    val harvesineUDF = sparkSession.udf.register("harvesine",
      (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => Distances.haversine(lat1, lon1, lat2, lon2))
    def getLatitude = udf((geom: String) => {
      val point = readWktOrWkb(geom).asInstanceOf[geotrellis.vector.Point]
      point.y
    })
    def getLongitude = udf((geom: String) => {
      val point = readWktOrWkb(geom).asInstanceOf[geotrellis.vector.Point]
      point.x
    })
    val fullInstallation = instDf
      .withColumn("latitude", getLatitude(col("geometry")))
      .withColumn("longitude", getLongitude(col("geometry")))
      .withColumn("installationDate", to_date(col("timestamp_completed")))
      .withColumn("startDate", to_date(lit(year(col("installationDate"))) + "/01/01", "yyyy/MM/dd"))
      .withColumn("installationDateString", date_format(col("timestamp_completed"), "DD-MM-YYYY"))
      .crossJoin(weatherDf)
      .withColumnRenamed("lat", "latW")
      .withColumnRenamed("long", "lonW")
      .withColumn("distance", harvesineUDF(col("latitude"), col("longitude"), col("lat"), col("long")))
    // Use window function to find minimum distance for each group
    val windowSpec = Window.partitionBy(col("gid"), col("installationDate"))
    val installationWeatherDf = fullInstallation
      .withColumn("minDistance", min(col("distance")).over(windowSpec))
      .where(col("distance") === col("minDistance"))
      //filter the weather data before the installation date in the same year, to calculate cumulative
      .filter(col("date").between(col("startDate"), col("installationDate")))
      //add column for useful hours and grade day
      .withColumn(usefulHours,
        when(col("product_type").equalTo(temperatureHour) &&
          col("value").between(15, 35), 1).otherwise(0))
      .withColumn(gradeDay, when(col("product_type").equalTo(averageDayTemperature) &&
        col("value") > gradeDay_lowerBound, col("value") - gradeDay_lowerBound).otherwise(0))
      .drop("distance")
      .drop("minDistance")
    installationWeatherDf
  }
}
/**
 * Constants and utilities for the distances
 */
object Distances {
  import scala.math.{asin, cos, pow, sin, sqrt}

  private val R = 6372.8 //radius in km
  /**
   * Haversine distance in kilometers.
   * @param lat1 point1 latitude
   * @param lon1 point1 longitude
   * @param lat2 point2 latitude
   * @param lon2 point2 longitude
   * @return distance in km
   */
  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians
    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) *
      cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }
}