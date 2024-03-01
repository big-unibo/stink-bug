package it.unibo.big.casedimension

object GenerateCaseDimensionTable {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, SparkSession}

  /**
   * Generate the case dimension table and bridge table
   * @param sparkSession the spark session
   * @param caseInputData a map where for each case table there is a dataframe
   * @return the case dimension table and bridge table dataframes
   */
  def apply(sparkSession: SparkSession, caseInputData: Map[String, DataFrame]): (DataFrame, DataFrame) = {
    val dimCaseDf : DataFrame = caseInputData("task_on_geo_object").as("togo").join(caseInputData("traps").as("go"), "gid")
      .join(caseInputData("geo_answer").as("ga"), "gid")
      .join(caseInputData("answer").as("a"), "answer_id")
      .where((col("ms_id").isin(9, 12) && col("task_id") === 5) || (col("ms_id") === 6 && col("task_id") === 2))
      .where(col("question_id").isin(
        "BMSB.INST.Q2", "BMSB.INSTNEW.Q3", "BMSB.INST.Q4", "BMSB.INSTNEW.Q5",
        "BMSB.INST.Q6", "BMSB.INSTNEW.Q7", "BMSB.INST.Q8", "BMSB.INSTNEW.Q9",
        "BMSB.INST.Q10", "BMSB.INSTNEW.Q11", "BMSB.INST.Q12", "BMSB.INSTNEW.Q13",
        "BMSB.INST.Q14", "BMSB.INSTNEW.Q15"
      )).select(col("answer_id"),
        when(col("question_id").isin("BMSB.INST.Q2", "BMSB.INSTNEW.Q3"), lit("Hedges and borders"))
          .when(col("question_id").isin("BMSB.INST.Q4", "BMSB.INSTNEW.Q5"), lit("Gardens and groves"))
          .when(col("question_id").isin("BMSB.INST.Q6", "BMSB.INSTNEW.Q7"), lit("Agricultural buildings"))
          .when(col("question_id").isin("BMSB.INST.Q8", "BMSB.INSTNEW.Q9"), lit("Residential buildings"))
          .when(col("question_id").isin("BMSB.INST.Q10", "BMSB.INSTNEW.Q11"), lit("Herbaceous crops"))
          .when(col("question_id").isin("BMSB.INST.Q12", "BMSB.INSTNEW.Q13"), lit("Tree fruit crops"))
          .when(col("question_id").isin("BMSB.INST.Q14", "BMSB.INSTNEW.Q15"), lit("River banks and channels"))
          .otherwise(col("question_id")).alias("answer_category"),
        when(col("answer_id").isin("BMSB.INST.Q2.A9", "BMSB.INSTNEW.Q3.A9"), lit("Altra siepe o bordura"))
          .when(col("answer_id").isin("BMSB.INST.Q4.A6", "BMSB.INSTNEW.Q5.A6"), lit("Altro giardino o boschetto"))
          .when(col("answer_id").isin("BMSB.INST.Q6.A1", "BMSB.INST.Q6.A2", "BMSB.INST.Q6.A3", "BMSB.INST.Q6.A4", "BMSB.INST.Q6.A5",
            "BMSB.INSTNEW.Q7.A1", "BMSB.INSTNEW.Q7.A2", "BMSB.INSTNEW.Q7.A3", "BMSB.INSTNEW.Q7.A4", "BMSB.INSTNEW.Q7.A5"), lit("Fabbricato ad uso agricolo"))
          .when(col("answer_id").isin("BMSB.INST.Q8.A1", "BMSB.INST.Q8.A2", "BMSB.INST.Q8.A3", "BMSB.INST.Q8.A4", "BMSB.INST.Q8.A5",
            "BMSB.INSTNEW.Q9.A1", "BMSB.INSTNEW.Q9.A2", "BMSB.INSTNEW.Q9.A3", "BMSB.INSTNEW.Q9.A4", "BMSB.INSTNEW.Q9.A5"), lit("Fabbricato ad uso abitativo"))
          .when(col("answer_id").isin("BMSB.INST.Q10.A8", "BMSB.INSTNEW.Q11.A8"), lit("Altra coltura estensiva"))
          .when(col("answer_id").isin("BMSB.INST.Q12.A13", "BMSB.INSTNEW.Q13.A13"), lit("Altra coltura arborea"))
          .when(col("answer_id").isin("BMSB.INST.Q14.A5", "BMSB.INSTNEW.Q15.A5"), lit("Altro argine o canale"))
          .otherwise(col("a.text")).alias("answer_text")
      ).distinct()
      .withColumn("cid", monotonically_increasing_id())
    val bridgeTrapCaseDf = caseInputData("task_on_geo_object").as("togo").join(caseInputData("given_answer").as("ga"), "togo_id")
      .join(caseInputData("answer").as("a"), "answer_id")
      .where(col("question_id").isin(
        "BMSB.INST.Q2", "BMSB.INSTNEW.Q3", "BMSB.INST.Q4", "BMSB.INSTNEW.Q5",
        "BMSB.INST.Q6", "BMSB.INSTNEW.Q7", "BMSB.INST.Q8", "BMSB.INSTNEW.Q9",
        "BMSB.INST.Q10", "BMSB.INSTNEW.Q11", "BMSB.INST.Q12", "BMSB.INSTNEW.Q13",
        "BMSB.INST.Q14", "BMSB.INSTNEW.Q15"
      )).select(col("gid"), col("answer_id"), col("text")).join(dimCaseDf.as("t"), "answer_id")
      .where((col("text").isNotNull && col("text") === col("element_name")) || col("text").isNull)
      .select(col("gid"), col("cid"))
    (dimCaseDf, bridgeTrapCaseDf)
  }
}
