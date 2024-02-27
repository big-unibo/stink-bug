package it.unibo.big

object GenerateDFM extends App {
  import org.slf4j.{Logger, LoggerFactory}
  import com.typesafe.config.{Config, ConfigFactory}
  import com.typesafe.config.ConfigValue
  import org.apache.commons.codec.binary.Base64
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.SparkSession


  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import java.util.Date

  private val sparkSession = SparkSession.builder().master("local[*]").appName("BMSB DFM creation").getOrCreate()
  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
  private val config: Config = ConfigFactory.load ()

  apply()

  def apply(): Unit = {
    //read case input data and create a temp view for each file
    val caseInputData = config.getConfig("dataset.CASE").entrySet().toArray
      .map(_.asInstanceOf[java.util.Map.Entry[String, ConfigValue]]).map(
        t => {
          val df = sparkSession.read.option("header", "true").csv(t.getValue.render().replaceAll("\"", ""))
          df.createTempView(t.getKey)
          t.getKey -> df
        })
    var dfNormalized: DataFrame = null
    var dfDaily: DataFrame = null
    //TODO fix query
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
    val monitoredTrapsDf = sparkSession.sql(QueryUtils.monitoredTraps).cache
    monitoredTrapsDf.show()
    val notMonitoredTrapsDf = sparkSession.sql(QueryUtils.notMonitoredTraps).cache
    notMonitoredTrapsDf.show()
    val workingTrapsDf = sparkSession.sql(QueryUtils.workingTraps).cache
    workingTrapsDf.show()

    // --------------------------------------------------
/*    //read the weather data
    val weatherDf = sparkSession.read.option("header", "true").csv(config.getString("dataset.weather")).cache

    def getInstallationInfos(gid: Int, weatherDf: DataFrame): Option[(Date, (Double, Double))] = {
      // find installation date
      val instQuery =
        s"""select to_char(togo.timestamp_completed, 'DD-MM-YYYY') as date, ST_Y(geo.geom) as lat_t, ST_X(geo.geom) as long_t
           |from geo_object geo, task_on_geo_object togo
           |where ((togo.task_id = 5 and geo.ms_id in (9, 12, 13)) or (togo.task_id = 2 and geo.ms_id = 6) )
           |and togo.gid = geo.gid
           |and togo.gid = $gid
           |and geo.geom is not null
           |""".
          stripMargin
      val result = postgres.queryTable(instQuery).first()
      Some(format.parse(result(0).toString), getTrapMeteoData((result(1).toString.toDouble, result(2).toString.toDouble), weatherDf))
    }

    def getMonitoringValue(v: Option[Double], diff: Long, daily: Boolean = false): Any = {
      println(s"The value monitored is $v with $diff diff days")
      v.map(v => if (daily) v else v * diff).orNull
    }

    def getMonitoringGlobalValue(value: Row, i: Int, diff: Long): Option[Double] = {
      println(s"The value monitored is ${value(i)} with $diff diff days")
      if (value(i) != null) Some(value.getLong(i).toDouble / diff) else None
    }

    if (!fullDf.isEmpty) {
      val fullData = if(MS_ID.isDefined) fullDf.collect.filter(_(5) == MS_ID.get) else fullDf.collect()
      var normalizedRecords = Seq[Row]()
      var dailyRecords = Seq[Row]()
      var value: Row = null
      var pastGid: Option[Int] = None
      var pastDate: Option[Date] = None
      var weatherLatLon: Option[(Double, Double)] = None

      var i = 0
      while (i < fullData.length) {
        var values = Seq[Row]()
        do {
          value = fullData(i)
          i += 1
          values :+= value
        } while (value(1) == null && i < fullData.length && (if (i < fullData.length) value(4) == fullData(i)(4) else true) && value(6).asInstanceOf[Boolean])

        val gid = value(4).toString.toInt
        print(s"Selected values ${values.mkString("\n")} for trap $gid")

        //update records balancing with diff days

        val getInstallationDate = if (pastGid.isDefined) !(pastGid.get == gid && pastDate.isDefined) else true

        pastDate = if (getInstallationDate) {
          val installationData = getInstallationInfos(gid, weatherDf)
          weatherLatLon = installationData.map(_._2)
          installationData.map(_._1)
        } else pastDate

        //The timestamp completed is ok
        val actualMonitoringDate = value(1)
        val workingTrap = value(6).asInstanceOf[Boolean]
        val dataAreNotBroken = actualMonitoringDate != null && workingTrap

        if (dataAreNotBroken) {
          var actualPastDate = pastDate.get

          val actualDate = format.parse(actualMonitoringDate.toString)
          val totalDatesDiff = getPositiveDateDiff(pastDate, actualDate)
          val adulti = getMonitoringGlobalValue(value, 8, totalDatesDiff)
          val g2 = getMonitoringGlobalValue(value, 9, totalDatesDiff)
          val g3 = getMonitoringGlobalValue(value, 10, totalDatesDiff)
          //get daily data
          val adultiDaily = getMonitoringValue(adulti, totalDatesDiff, daily = true)
          val g2Daily = getMonitoringValue(g2, totalDatesDiff, daily = true)
          val g3Daily = getMonitoringValue(g3, totalDatesDiff, daily = true)

          dailyRecords ++= values.flatMap(v => {
            val taskDate = format.parse(v(0).toString)
            //default the monitoring is 3 days after task day
            val monitoringDate = if (v(1) == null) DateUtils.addDays(taskDate) else {
              DateUtils.getMonitoringDate(taskDate, format.parse(v(1).toString))
            }
            var rows = Seq[Row]()

            while (actualPastDate != monitoringDate) {
              val md = DateUtils.addDays(actualPastDate, 1)
              val r = Row.fromSeq(Seq(new sql.Date(taskDate.getTime), new sql.Date(md.getTime), gid,
                adultiDaily, g2Daily, g3Daily, totalDatesDiff,
                new sql.Date(actualPastDate.getTime), weatherLatLon.get._1, weatherLatLon.get._2
              ))
              actualPastDate = md
              rows :+= r
            }
            rows
          })

          normalizedRecords ++= values.map(v => {
            val taskDate = format.parse(v(0).toString)
            //default the monitoring is 3 days after task day
            val monitoringDate = if (v(1) == null) DateUtils.addDays(taskDate) else {
              DateUtils.getMonitoringDate(taskDate, format.parse(v(1).toString))
            }
            val actualDaysDiff = getPositiveDateDiff(pastDate, monitoringDate)

            val r = Row.fromSeq(Seq(new sql.Date(taskDate.getTime),
              new sql.Date(monitoringDate.getTime), gid,
              getMonitoringValue(adulti, actualDaysDiff),
              getMonitoringValue(g2, actualDaysDiff),
              getMonitoringValue(g3, actualDaysDiff), actualDaysDiff,
              //ad one to past date to start after it
              new sql.Date(pastDate.get.getTime), weatherLatLon.get._1, weatherLatLon.get._2
            ))
            pastDate = Some(monitoringDate)
            r
          })
        } else {
          pastDate = if (actualMonitoringDate != null) Some(format.parse(actualMonitoringDate.toString)) else if (!workingTrap) Some(DateUtils.addDays(format.parse(value(0).toString))) else None
        }

        pastGid = Some(value(4).toString.toInt)
      }

      dfNormalized = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(normalizedRecords), NormalizedDataVersion.struct)
      dfDaily = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(dailyRecords), DailyDataVersion.struct)

      FileUtils.saveFile(dfNormalized, NormalizedDataVersion.temporaryFileName(MS_ID))
      FileUtils.saveFile(dfDaily, DailyDataVersion.temporaryFileName(MS_ID))
    }

    def addUsefulHoursColumnAndGroupData(df: DataFrame, dataVersion: DataVersion, meteoAdditionalInfo: String = ""): DataFrame = {

      val joinCondition = col("lat") === col("latW") &&
        col("long") === col("lonW") &&
        col("date") > col("pastDate") && col("date") <= col("m")

      val groupedData = dataVersion.struct.slice(0, 7).map(x => col(x.name))

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

      for (c <- meteoInfos) {
        tmpDf = tmpDf.withColumnRenamed(s"${c}_value", s"$meteoAdditionalInfo$c")
      }
      tmpDf
    }

    val normalizedFinalDf = addUsefulHoursColumnAndGroupData(dfNormalized, NormalizedDataVersion).cache
    FileUtils.saveFile(normalizedFinalDf, NormalizedDataVersion.finalFileName(MS_ID))
    var dailyFinalDf = addUsefulHoursColumnAndGroupData(dfDaily, DailyDataVersion, "daily_")

    dailyFinalDf = dailyFinalDf.join(
      normalizedFinalDf, dailyFinalDf("gid") === normalizedFinalDf("gid") &&
        dailyFinalDf("t") === normalizedFinalDf("t")
    )

    for (c <- NormalizedDataVersion.struct.fields.slice(0, 7)) {
      dailyFinalDf = dailyFinalDf.drop(normalizedFinalDf(c.name))
    }

    for (c <- normalizedColumns) {
      dailyFinalDf = dailyFinalDf.withColumn(c, col(c) / col("Giorni monitoraggio"))
    }
    FileUtils.saveFile(dailyFinalDf, DailyDataVersion.finalFileName(MS_ID))
  }

  /**
   *
   * @param d1 the previous date of the difference
   * @param d2 the date netx to past date
   * @return the difference of days between d1 and d2, if negative throws IllegalArgumentException
   */
  private def getPositiveDateDiff(d1: Option[Date], d2: Date): Long = {
    val diff = DateUtils.getDaysDiff(d1.get, d2)
    if (diff < 0) {
      throw new IllegalArgumentException()
    }
    diff*/
  }

  private def getTrapMeteoData(trapLatLon: (Double, Double), weatherDf: DataFrame): (Double, Double) = {
    /*val harvesineUDF = sparkSession.udf.register("harvesine",
      (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => Distances.haversine(lat1, lon1, lat2, lon2))

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val latLon = weatherDf.select(col("lat"), col("long")).distinct()
      .withColumn("distance", harvesineUDF(
        lit(trapLatLon._1), lit(trapLatLon._2),
        col("lat"), col("long")
      )).withColumn(
        "mindistance",
        min(col("distance")).over(Window.orderBy("distance"))
      ).filter(col("distance") === col("mindistance")).first()
    (latLon(0).toString.toDouble, latLon(1).toString.toDouble)*/
    ???
  }
}
