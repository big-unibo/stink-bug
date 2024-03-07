package it.unibo.big.casedimension

object GenerateTrapDimension {
  import it.unibo.big.Utils.readGeometry
  import org.apache.spark.sql.functions.{col, when}
  import org.apache.spark.sql.{DataFrame, SparkSession}

  /**
   * Generate the trap dimension table
   * @param sparkSession the spark session
   * @param caseInputData a map where for each case table there is a dataframe
   * @return the trap dimension table dataframe
   */
  def apply(sparkSession: SparkSession, caseInputData: Map[String, DataFrame]): DataFrame = {
    var trapsWithValuedSVP = caseInputData("traps").as("geo").join(caseInputData("task_on_geo_object").as("togo"), "gid")
      .join(caseInputData("given_answer").as("ga"), "togo_id")
      .join(caseInputData("answer").as("a"), "answer_id")
      .where(col("question_id") === "BMSB.INSTNEW.Q16")
      .where(col("ms_id").isin(9, 12))
      .select(col("gid"), col("district"), col("geometry"), col("name"), col("ms_id"), col("date_start"), col("date_end"), col("ga.text").cast("integer").as("svp (manual)"))
    //look with traps without svp value and link to the nearest traps in 100 meters radius if present
    val traps = caseInputData("traps").cache
    val trapsRows = traps.collect().map(r => {
      r(0).asInstanceOf[Int] -> (readGeometry(r.get(r.fieldIndex("geometry")).toString).geom, r)
    }).toMap
    var trapsWithSVP = trapsWithValuedSVP.collect().map(r => {
      val gid =  r(0).asInstanceOf[Int]
      gid -> (trapsRows(gid)._1, r.get(r.fieldIndex("svp (manual")).asInstanceOf[Int])
    }).toMap
    for ((gid, (geom, r)) <- trapsRows) {
      if(!trapsWithSVP.contains(gid)) {
        val closest = trapsWithSVP.map {
          case (gid2, (geom2, svp)) => (gid2, geom.distance(geom2), svp)
        }.minBy(_._2)
        if(closest._2 <= 100) {
          //add new row to the dataframe trapsWithValuedSVP
          trapsWithSVP += gid -> (geom, closest._3)
          trapsWithValuedSVP = trapsWithValuedSVP.union(sparkSession.createDataFrame(Seq((gid, geom, r.get(r.fieldIndex("name")), r.get(r.fieldIndex("ms_id")), r.get(r.fieldIndex("date_start")), r.get(r.fieldIndex("date_end")), closest._3)))
            .toDF("gid", "geometry", "name", "ms_id", "date_start", "date_end", "svp (manual)"))
        }
      }
    }
    //add the rest of the trap
    val rows = trapsRows.filterKeys(!trapsWithSVP.contains(_)).toSeq.map {
      case (gid, (geom, r)) =>
        (gid, geom, r.get(r.fieldIndex("name")), r.get(r.fieldIndex("ms_id")), r.get(r.fieldIndex("date_start")), r.get(r.fieldIndex("date_end")), null)
    }
    val trapsDimDf = trapsWithValuedSVP.union(sparkSession.createDataFrame(rows).toDF("gid", "geometry", "name", "ms_id", "date_start", "date_end", "svp (manual)"))
    calculateArea(trapsDimDf)
  }

  /**
   * Calculate the area of the trap
   * @param df the trap dataframe
   * @return the trap dataframe with the area column
   */
  private def calculateArea(df: DataFrame): DataFrame = {
    val gidFCRARN = Seq(460, 787, 786, 785, 784, 777, 776, 775, 452, 268, 267, 266, 265, 264, 263, 256, 255, 254, 196, 42, 41, 40, 39, 32, 31, 30)
    val gidOTHER = Seq(184, 887)
    val nameBOFE = Seq("MO05", "MO06")
    val admProvBOFE = Seq("BO", "FE")
    val admProvFCRARN = Seq("FC", "RA", "RN")
    val admProvMORE = Seq("MO", "RE")

    df.withColumn("area",
        when(col("gid").isin(gidFCRARN: _*), "FC-RA-RN")
          .when(col("gid").isin(gidOTHER: _*), "OTHER")
          .when(col("name").isin(nameBOFE: _*), "BO-FE")
          .when(col("district").isin(admProvBOFE: _*), "BO-FE")
          .when(col("district").isin(admProvFCRARN: _*), "FC-RA-RN")
          .when(col("district").isin(admProvMORE: _*), "MO-RE")
          .otherwise("OTHER")
      )
  }
}
