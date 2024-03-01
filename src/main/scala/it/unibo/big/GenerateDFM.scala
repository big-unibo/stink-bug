package it.unibo.big

import it.unibo.big.normalized_fact.GenerateNormalizedFactWithMeteo

object GenerateDFM extends App {

  import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.slf4j.{Logger, LoggerFactory}

  private val sparkSession = SparkSession.builder().master("local[*]").appName("BMSB DFM creation").getOrCreate()
  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
  private val config: Config = ConfigFactory.load()

  apply()

  def apply(): Unit = {
    //read case input data and create a temp view for each file
    val caseInputData: Map[String, DataFrame] = config.getConfig("dataset.CASE").entrySet().toArray
      .map(_.asInstanceOf[java.util.Map.Entry[String, ConfigValue]]).map(
        t => {
          val df = sparkSession.read.option("header", "true").csv(t.getValue.render().replaceAll("\"", ""))
          t.getKey -> df
        }).toMap
    val normalizedFinalDf: DataFrame = GenerateNormalizedFactWithMeteo(sparkSession, config, caseInputData)
    FileUtils.saveFile(normalizedFinalDf, config.getString("dataset.DFM.fact_passive_monitoring"))
  }
}
