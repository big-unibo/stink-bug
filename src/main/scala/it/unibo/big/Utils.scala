package it.unibo.big

object Utils {
  import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
  import geotrellis.vector._
  import geotrellis.vector.io.readWktOrWkb
  import org.apache.spark.sql.{DataFrame, SparkSession}

  val sparkSession = SparkSession.builder().master("local[*]")
    .appName("BMSB DFM creation")
    .getOrCreate()
  val config: Config = ConfigFactory.load()

  def readCaseInputData(): Map[String, DataFrame] = {
    config.getConfig("dataset.CASE").entrySet().toArray
    .map(_.asInstanceOf[java.util.Map.Entry[String, ConfigValue]]).map(
      t => {
        val df = sparkSession.read.option("header", "true").csv(t.getValue.render().replaceAll("\"", ""))
        t.getKey -> df
      }).toMap
  }

  /**
   * Read geometry from input string
   * @param inputString input string of geometry with SRID=4326;POINT(11.8455944 44.374285) format
   * @return the geometry
   */
  def readGeometry(inputString: String): Projected[Geometry] = {
    try {
      val splittedString = inputString.split(";")
      val srid = splittedString.head.split("SRID=")(1).toInt
      val geomString = splittedString(1)
      // Convert WKT/WKB string to GeoTrellis geometry
      readWktOrWkb(geomString).withSRID(srid)
    } catch {
      case _: Exception =>
        throw new Exception(s"Error reading geometry from input string: $inputString")
    }

  }
}
