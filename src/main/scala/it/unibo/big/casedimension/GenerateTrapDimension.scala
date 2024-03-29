package it.unibo.big.casedimension

object GenerateTrapDimension {
  import it.unibo.big.Utils._
  import org.apache.spark.sql.functions.{col, when}
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import com.vividsolutions.jts.geom.Geometry

  /**
   * Generate the trap dimension table
   * @param sparkSession the spark session
   * @param caseInputData a map where for each case table there is a dataframe
   * @return the trap dimension table dataframe
   */
  def apply(sparkSession: SparkSession, caseInputData: Map[String, DataFrame]): DataFrame = {
    val trapsDf = caseInputData("traps").withColumn("latitude", getLatitude(col("geometry")))
      .withColumn("longitude", getLongitude(col("geometry")))
    var trapsWithValuedSVP = trapsDf.as("geo").join(caseInputData("task_on_geo_object").as("togo"), "gid")
      .join(caseInputData("given_answer").as("ga"), "togo_id")
      .join(caseInputData("answer").as("a"), "answer_id")
      .where(col("question_id") === "BMSB.INSTNEW.Q16")
      .where(col("ms_id").isin(9, 12))
      .select(col("gid"), col("district"), col("geometry"), col("name"), col("ms_id"), col("monitoring_started"), col("monitoring_ended"),
        col("ga.text").cast("integer").as("svp (manual)"), col("latitude"), col("longitude"))

    val columns = Seq("gid", "district", "latitude", "longitude", "name", "ms_id", "monitoring_started", "monitoring_ended", "svp (manual)")

    //look with traps without svp value and link to the nearest traps in 100 meters radius if present
    val traps = getGeometryColumn("geom", trapsDf.withColumn("geom", col("geometry"))).cache
    val trapsRows = traps.collect().map(r => {
      r(0).toString.toInt -> (r.getAs[Geometry](r.fieldIndex("geom")), r, r.getAs[Double](r.fieldIndex("latitude")), r.getAs[Double](r.fieldIndex("longitude")))
    }).toMap
    var trapsWithSVP = trapsWithValuedSVP.collect().map(r => {
      val gid =  r(0).toString.toInt
      gid -> (trapsRows(gid)._1, r.get(r.fieldIndex("svp (manual)")).asInstanceOf[Int])
    }).toMap
    trapsWithValuedSVP = trapsWithValuedSVP.drop("geometry").select(columns.map(col) :_*)
    for ((gid, (geom, r, lat, lon)) <- trapsRows) {
      if(!trapsWithSVP.contains(gid)) {
        val closest = trapsWithSVP.map {
          case (gid2, (geom2, svp)) => (gid2, geom.distance(geom2), svp)
        }.minBy(_._2)
        if(closest._2 <= 100) {
          //add new row to the dataframe trapsWithValuedSVP
          trapsWithSVP += gid -> (geom, closest._3)
          trapsWithValuedSVP = trapsWithValuedSVP.union(sparkSession.createDataFrame(Seq((gid, r.getString(r.fieldIndex("district")), lat, lon, r.getString(r.fieldIndex("name")), r.getString(r.fieldIndex("ms_id")).toInt, r.getString(r.fieldIndex("monitoring_started")), r.getString(r.fieldIndex("monitoring_ended")), closest._3)))
            .toDF(columns :_*))
        }
      }
    }
    //add the rest of the trap
    val rows = trapsRows.filterKeys(!trapsWithSVP.contains(_)).toSeq.map {
      case (gid, (_, r, lat, lon)) =>
        (gid, r.getString(r.fieldIndex("district")), lat, lon, r.getString(r.fieldIndex("name")), r.getString(r.fieldIndex("ms_id")).toInt, r.getString(r.fieldIndex("monitoring_started")), r.getString(r.fieldIndex("monitoring_ended")), null)
    }
    val trapsDimDf = trapsWithValuedSVP.union(sparkSession.createDataFrame(rows).toDF(columns :_*))
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
