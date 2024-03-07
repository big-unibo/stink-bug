package it.unibo.big.normalized_fact

object GenerateNormalizedFactWithMeteo {
  import it.unibo.big.normalized_fact.MeteoUtils.addUsefulDegreeDaysAndGroupData
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import org.slf4j.{Logger, LoggerFactory}

  import java.sql
  import java.text.SimpleDateFormat
  import java.util.Date


  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Generate the normalized fact with the meteo data
   * @param sparkSession the spark session
   * @param caseInputData a map where for each case table there is a dataframe
   * @param weatherDf the dataframe with the weather data
   * @return the normalized fact with the meteo data
   */
  def apply(sparkSession: SparkSession, caseInputData: Map[String, DataFrame], weatherDf: DataFrame): DataFrame = {
    var dfNormalized: DataFrame = null
    val format = new SimpleDateFormat("dd-MM-yyyy")

    val (fullDf, inst) = getNormalizedCaputresDataframe(sparkSession, caseInputData)
    // get the installation date of each trap and the meteo data
    val installationWeatherDf = MeteoUtils.getInstallationWeatherDataframe(sparkSession, inst, weatherDf).cache()

    /**
     *
     * @param gid the trap identifier
     * @return the installation date and the lat and lon of the weather cell near the trap
     */
    def getInstallationInfos(gid: Int): Option[(Date, (Double, Double))] = {
      val result = installationWeatherDf.filter(col("gid") === gid)
        .select(col("installationDateString"), col("latW"), col("longW")).first()
      Some(format.parse(result(0).toString), (result(1).toString.toDouble, result(2).toString.toDouble))
    }

    def getMonitoringValue(v: Option[Double], diff: Long, daily: Boolean = false): Any = {
      LOGGER.debug(s"The value monitored is $v with $diff diff days")
      v.map(v => if (daily) v else v * diff).orNull
    }

    def getMonitoringGlobalValue(value: Row, i: Int, diff: Long): Option[Double] = {
      LOGGER.debug(s"The value monitored is ${value(i)} with $diff diff days")
      if (value(i) != null) Some(value.getLong(i).toDouble / diff) else None
    }

    val struct = StructType(
      List(StructField("t", DateType),
        StructField("m", DateType),
        StructField("gid", IntegerType),
        StructField("Adults captured", DoubleType),
        StructField("Small instars captured", DoubleType),
        StructField("Large instars captured", DoubleType),
        StructField("Days of monitoring", LongType),
        StructField("pastDate", DateType),
        StructField("latW", DoubleType),
        StructField("lonW", DoubleType)
      ))

    if (!fullDf.isEmpty) {
      val fullData = fullDf.collect()
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
        LOGGER.debug(s"Selected values ${values.mkString("\n")} for trap $gid")

        //update records balancing with diff days

        val getInstallationDate = if (pastGid.isDefined) !(pastGid.get == gid && pastDate.isDefined) else true

        pastDate = if (getInstallationDate) {
          val installationData = getInstallationInfos(gid)
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
          val totalDatesDiff = DateUtils.getPositiveDateDiff(pastDate, actualDate)
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
            val actualDaysDiff = DateUtils.getPositiveDateDiff(pastDate, monitoringDate)

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

      dfNormalized = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(normalizedRecords), struct)
    }
    val normalizedFinalDf = addUsefulDegreeDaysAndGroupData(dfNormalized, installationWeatherDf, weatherDf = weatherDf, struct = struct)
    normalizedFinalDf
  }

  /**
   *
   * @param sparkSession the spark session
   * @param caseInputData a map where for each case table there is a dataframe
   * @return a dataframe where for each possible monitoring task there is the fact if the trap is monitored or not and is working or not
   *         another dataframe with traps installation date
   */
  private[normalized_fact] def getNormalizedCaputresDataframe(sparkSession: SparkSession, caseInputData: Map[String, DataFrame]): (DataFrame, DataFrame) = {
    //get the traps that have been monitored
    val monitoredTrapsDf = caseInputData("task_on_geo_object").as("togo")
      .join(caseInputData("given_answer").as("ga"), "togo_id")
      .join(caseInputData("answer").as("a"), "answer_id")
      .join(caseInputData("question").as("q"), "question_id")
      .join(caseInputData("traps").as("geo"), "gid")
      .filter((col("togo.task_id") === 6 && col("geo.ms_id").isin(9, 12)) || (col("togo.task_id") === 3 && col("geo.ms_id") === 6))
      .filter(col("timestamp_completed").isNotNull)
      .filter(col("a.question_id").isin("BMSB.PASS.Q4", "BMSB.PASS.Q7", "BMSB.PASS.Q8",
        "BMSB.PASS.Q10", "BMSB.PASS.Q11", "BMSB.PASS.Q14", "BMSB.PASS.Q15",
        "BMSB.PASS.Q16", "BMSB.PASS.Q17", "BMSB.PASSNEW.Q3", "BMSB.PASSNEW.Q4", "BMSB.PASSNEW.Q5"))
      .groupBy(col("togo.timestamp_assignment"), col("togo.timestamp_completed"), col("togo.togo_id"), col("geo.name"), col("togo.gid"), col("geo.ms_id"))
      .agg(sum(when(col("a.question_id").isin("BMSB.PASS.Q10", "BMSB.PASS.Q11", "BMSB.PASS.Q4", "BMSB.PASSNEW.Q3"), col("ga.text").cast("integer")).otherwise(lit(0))).as("Adults captured"),
        sum(when(col("a.question_id").isin("BMSB.PASS.Q14", "BMSB.PASS.Q15", "BMSB.PASS.Q7", "BMSB.PASSNEW.Q4"), col("ga.text").cast("integer")).otherwise(lit(0))).as("Small instars captured"),
        sum(when(col("a.question_id").isin("BMSB.PASS.Q16", "BMSB.PASS.Q17", "BMSB.PASS.Q8", "BMSB.PASSNEW.Q5"), col("ga.text").cast("integer")).otherwise(lit(0))).as("Large instars captured"))
      .withColumn("is_monitored", lit(true))
      .cache

    //get the traps that have been selected as working
    val workingTrapsDf = caseInputData("traps").as("geo").join(caseInputData("task_on_geo_object").as("togo"), "gid")
      .join(caseInputData("given_answer").as("ans"), "togo_id")
      .filter((col("ans.answer_id") === "BMSB.PASSNEW.Q2.A2" && col("geo.ms_id").isin(9, 12)) || (col("ans.answer_id").isin("BMSB.PASS.Q1.A3", "BMSB.PASS.Q1.A4", "BMSB.PASS.Q1.A5") && col("geo.ms_id") === 6))
      .select(col("togo.timestamp_assignment"), col("togo.timestamp_completed"), col("togo.togo_id"), col("geo.name"), col("geo.gid"), col("geo.ms_id"), lit(false).as("is_working"))

    //get monitored dates for each trap
    val trapMonitoring: Map[Any, Array[Any]] = monitoredTrapsDf.collect().map(r => {
      r.get(r.fieldIndex("gid")) -> r.get(r.fieldIndex("timestamp_assignment"))
    }).groupBy(_._1).map {
      case (x, y) => x -> y.map(_._2)
    }

    //get all the dates in which the traps have been assigned to a task
    val togo_dates = caseInputData("traps").as("geo").join(caseInputData("task_on_geo_object").as("togo"), "gid")
      .filter(col("togo.task_id").isin(3, 6))
      .select(col("togo.timestamp_assignment"), col("geo.ms_id"))
      .distinct()
    //get the installation date of each trap
    val inst = caseInputData("traps").as("geo").join(caseInputData("task_on_geo_object").as("togo"), "gid")
      .filter((col("togo.task_id") === 5 && col("geo.ms_id").isin(9, 12)) || (col("togo.task_id") === 2 && col("geo.ms_id") === 6))
      .filter(col("geo.geometry").isNotNull)
      .select(col("togo.timestamp_completed"), col("geo.name"), col("geo.gid"), col("geo.ms_id"))
    //get the not monitored tasks for each trap
    val notMonitoredTrapsDf = togo_dates.as("togo_dates").join(inst.as("inst"), "ms_id")
      .where(col("timestamp_assignment") > col("inst.timestamp_completed"))
      .select(col("timestamp_assignment"), lit(null).as("timestamp_completed"), lit(null).as("togo_id"),
        col("inst.name"), col("inst.gid"), col("ms_id"),
        lit(null).as("Adults captured"), lit(null).as("Small instars captured"), lit(null).as("Large instars captured"),
        lit(false).as("is_monitored"))
      //add a filter that remove the timestamp assignment for the traps that are already monitored, i.e. if the pair gid and timestamp_assignment is present in the monitoredTrapsDf
      .filter(r => !trapMonitoring.getOrElse(r.get(r.fieldIndex("gid")), Array[Any]()).contains(r.get(r.fieldIndex("timestamp_assignment"))))

    //link all the partial results to create the final dataframe
    val fullDf = workingTrapsDf.as("working") //apply full-outer join with the union of the two dataframes
      .join(monitoredTrapsDf.union(notMonitoredTrapsDf).as("monitoring"), Seq("gid", "timestamp_assignment"), "full_outer")
      .select(
        date_format(col("timestamp_assignment"), "dd-MM-yyyy").as("taskDate"),
        date_format(coalesce(col("working.timestamp_completed"), col("monitoring.timestamp_completed")), "dd-MM-yyyy").as("rilevationDate"),
        coalesce(col("working.name"), col("monitoring.name")).as("name"),
        coalesce(col("working.togo_id"), col("monitoring.togo_id")).as("togo_id"),
        col("gid"),
        coalesce(col("working.ms_id"), col("monitoring.ms_id")).as("ms_id"),
        coalesce(col("is_working"), lit(true)).as("is_working"),
        coalesce(col("is_monitored"), lit(false)).as("is_monitored"),
        col("monitoring.Adults captured"), col("monitoring.Small instars captured"), col("monitoring.Large instars captured")
      ).cache
    (fullDf, inst)
  }
}
