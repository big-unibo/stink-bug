package it.unibo.big

object Utils {
  import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
  import org.apache.spark.sql.{DataFrame, SparkSession}

  val sparkSession = SparkSession.builder().master("local[*]").appName("BMSB DFM creation").getOrCreate()
  val config: Config = ConfigFactory.load()

  def readCaseInputData(): Map[String, DataFrame] = {
    config.getConfig("dataset.CASE").entrySet().toArray
    .map(_.asInstanceOf[java.util.Map.Entry[String, ConfigValue]]).map(
      t => {
        val df = sparkSession.read.option("header", "true").csv(t.getValue.render().replaceAll("\"", ""))
        t.getKey -> df
      }).toMap
  }
}
