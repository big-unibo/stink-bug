package it.unibo.big.normalized_fact

import it.unibo.big.normalized_fact.GenerateNormalizedFactWithMeteo.getNormalizedCaputresDataframe
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GenerateTrapsValidity {

  /**
   * Generate the traps validity dataframe, referring to the three cases:
   * - if a trap has more than 5 consecutive unworkings or 10 consecutive unmonitorings
   * - if a trap has more than 3 consecutive unmonitorings
   * - otherwise, it is interesting
   *
   * @param sparkSession  the spark session
   * @param caseInputData the case input data
   * @return a dataframe where for each gid (trap identifier) there is the validity (valid, invalid, noisy)
   */
  def apply(sparkSession: SparkSession, caseInputData: Map[String, DataFrame]): DataFrame = {
    val preprocessData = getNormalizedCaputresDataframe(sparkSession, caseInputData)._1
    val struct = StructType(
      StructField("gid", IntegerType) ::
        StructField("validity", StringType) :: Nil
    )
    if (!preprocessData.isEmpty) {
      val fullData = preprocessData.collect
      var value: Row = null
      var pastGid: Option[Int] = None
      var numberOfUnworkings = 0
      var numberOfConsecutiveUnMonitoring = 0
      var consecutiveUnMonitoringsMap = Map[Int, (Int, Int)]()

      var i = 0
      var gid = 0
      while (i < fullData.length) {
        var values = Seq[Row]()
        do {
          value = fullData(i)
          i += 1
          values :+= value
        } while (!value(7).asInstanceOf[Boolean] && i < fullData.length && (if (i < fullData.length) value(3) == fullData(i)(3) else true) && value(6).asInstanceOf[Boolean])

        gid = value(3).toString.toInt

        val newTrap = if (pastGid.isDefined) !(pastGid.get == gid) else true
        if (newTrap && pastGid.isDefined) {
          consecutiveUnMonitoringsMap += (pastGid.get -> (numberOfConsecutiveUnMonitoring, numberOfUnworkings))
          numberOfConsecutiveUnMonitoring = 0
          numberOfUnworkings = 0
        }

        if (value(6).asInstanceOf[Boolean]) { //if working
          numberOfConsecutiveUnMonitoring = Math.max(numberOfConsecutiveUnMonitoring, values.count(x => !x(7).asInstanceOf[Boolean]))
        } else {
          numberOfUnworkings += 1
          values.foreach(v => {
            if (!v(7).asInstanceOf[Boolean]) {
              numberOfUnworkings += 1
            }
          })
        }
        pastGid = Some(value(3).toString.toInt)
      }
      consecutiveUnMonitoringsMap += (gid -> (numberOfConsecutiveUnMonitoring, numberOfUnworkings))

      val trapsValidityDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(consecutiveUnMonitoringsMap.toSeq.map {
        case (x, (consecutiveUnMonitorings, broken)) if broken > 5 || consecutiveUnMonitorings >= 10 => Row(x, "invalid")
        case (x, (consecutiveUnMonitorings, _)) if consecutiveUnMonitorings >= 3 => Row(x, "noisy")
        case (x, _) => Row(x, "valid")
      }), struct)

      trapsValidityDf
    } else {
      //return empty dataframe
      sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq.empty[Row]), struct)
    }
  }
}
