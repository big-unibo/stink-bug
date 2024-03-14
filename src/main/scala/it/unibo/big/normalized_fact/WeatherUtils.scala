package it.unibo.big.normalized_fact

private[normalized_fact] object MeteoUtils {
  import it.unibo.big.Utils._
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
  val degreeDay: String = "degree_day"
  val gradeDay_lowerBound: Double = 12.2

  /**
   *
   * @param df                  the dataframe to add the useful hours and group the data
   * @param trapInstallationDf the dataframe with the trap installation data
   * @param meteoAdditionalInfo the additional info to add to the meteo data
   * @return the dataframe with the useful hours and the grouped data
   */
  def addUsefulDegreeDaysAndGroupData(df: DataFrame, trapInstallationDf: DataFrame, meteoAdditionalInfo: String = "", struct: StructType, weatherDf: DataFrame): DataFrame = {

    val joinCondition = col("lat") === col("latW") &&
      col("long") === col("lonW") &&
      col("date") > col("pastDate") && col("date") <= col("m")

    val groupedData = struct.slice(0, 7).map(x => col(x.name))

    var tmpDf = df.join(weatherDf, joinCondition)
      .withColumn(degreeDay, when(col("p_type").equalTo(averageDayTemperature) &&
        col("value") > gradeDay_lowerBound, col("value") - gradeDay_lowerBound).otherwise(0))
      .groupBy(groupedData :+ col("p_type"): _*)
      .agg(when(col("p_type").equalTo("prec_day"),
        sum(col("value"))).otherwise(avg(col("value"))).as("value"), sum(col(degreeDay)).as(degreeDay))
      .groupBy(groupedData: _*)
      .pivot("p_type").agg(first("value").as("value"), max(col(degreeDay)).as(degreeDay)) //max because it takes values of 0 for other weather fields other than hourly and average temperature
      .withColumnRenamed(s"${averageDayTemperature}_$degreeDay", s"$meteoAdditionalInfo$degreeDay")
      .drop(meteoInfos.map(x => s"${x}_$degreeDay") : _*)

    val tmpColumns = tmpDf.columns ++ Seq(s"cum_$degreeDay")
    val startingComulatives = trapInstallationDf.groupBy("gid").agg(sum(degreeDay).as("cum_dd"))

    tmpDf = tmpDf.join(startingComulatives, Seq("gid"), "left")
      //generate cumulative sum of useful hours and grade day
      .withColumn(s"cum_$degreeDay", col("cum_dd") + sum(col(degreeDay)).over(Window.partitionBy("gid").orderBy("t")))
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

    val newInstDf = instDf.withColumn("installationDate", to_date(col("timestamp_completed")))
      .withColumn("year", year(col("installationDate")))
      .withColumn("startDate", to_date(concat(year(col("installationDate")).cast("string"), lit("/01/01")), "yyyy/MM/dd")).repartition(col("year")).cache

    newInstDf.collect

    val groups = newInstDf.groupBy("year").agg(max("installationDate").as("installationDate"), min("startDate").as("startDate"))
    val newWeatherDf = weatherDf.withColumn("date", to_date(col("date")))
      .withColumnRenamed("lat", "latW")
      .withColumnRenamed("long", "lonW")
      .withColumn("year", year(col("date")))
      .repartition(col("year"))

    //filter weather dataframe to get only the data before the installation date
    val filteredWeatherDf = newWeatherDf.join(groups, Seq("year"))
      .filter(col("date").between(col("startDate"), col("installationDate")))
      .select("date", "latW", "lonW", "p_type", "value", "year").cache()

    //collect the weather data for caching
    filteredWeatherDf.collect

    val fullInstallation = newInstDf
      .withColumn("latitude", getLatitude(col("geometry")))
      .withColumn("longitude", getLongitude(col("geometry")))
      .join(filteredWeatherDf, "year")
      .withColumn("distance", harvesineUDF(col("latitude"), col("longitude"), col("latW"), col("lonW")))
    // Use window function to find minimum distance for each group
    val windowSpec = Window.partitionBy(col("gid"), col("installationDate"))
    val installationWeatherDf = fullInstallation
      .withColumn("minDistance", min(col("distance")).over(windowSpec))
      .where(col("distance") === col("minDistance"))
      //filter the weather data before the installation date in the same year, to calculate cumulative
      .filter(col("date").between(col("startDate"), col("installationDate")))
      //add column for useful hours and grade day
      .withColumn(degreeDay, when(col("p_type").equalTo(averageDayTemperature) &&
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
  import scala.math._

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