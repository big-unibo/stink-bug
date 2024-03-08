package it.unibo.big

object Utils {
  import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
  import geotrellis.vector._
  import geotrellis.vector.io.readWktOrWkb
  import org.apache.spark.sql.{DataFrame, SparkSession}

  //set parameters for download from https
  private val tslVersion = "TLSv1.3"
  System.setProperty("javax.net.debug", "ssl")
  System.setProperty("jdk.tls.client.protocols", tslVersion)
  System.setProperty("https.protocols", tslVersion)
  // Set the cipher suites for the client
  // System.setProperty("jdk.tls.client.cipherSuites", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")
  // Set the cipher suites for the server (if applicable)
  //System.setProperty("jdk.tls.server.cipherSuites", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")

  val sparkSession = SparkSession.builder().master("local[*]")
    .appName("Stink bug CUBE creation")
    //.config("spark.executor.extraJavaOptions", s"-Dhttps.protocols=$tslVersion")
    //.config("spark.driver.extraJavaOptions", s"-Dhttps.protocols=$tslVersion")
    .getOrCreate()
  val config: Config = ConfigFactory.load()

  def readInputData(datasetName: String): Map[String, DataFrame] = {
    config.getConfig(s"dataset.$datasetName").entrySet().toArray
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

  import org.jsoup.Jsoup
  import scala.collection.JavaConverters._

  import org.jsoup.nodes.Document
  /**
   * Get file and folder names from a directory listing
   * @param link the link of the directory
   * @return an array of file and folder names
   */
  def getFileAndFolderNames(link: String): Array[String] = {
    import sys.process._
    // Execute curl command to get directory listing
    val result = s"curl -s $link".!!
    // Parse HTML using Jsoup
    val doc: Document = Jsoup.parse(result)

    // Extract filenames from anchor tags
    val filenames = doc.select("a[href]").asScala.drop(5).map { element =>
      val href = element.attr("href")
      // Extract filename from the href
      val filename = href.split("/").lastOption.getOrElse("")
      filename
    }.toList

    // Filter out filenames that are not files (e.g., directories)
    filenames.filterNot(_.isEmpty).toArray
  }
}

object ImagesMap {
  import Utils._

  /**
   * Generate a map of images for each folder
   * @param link the link of the folder containing the images
   * @return a map where each key is a folder name and the value is an array of image paths
   */
  def generateImagesMap(link: String): Map[String, Array[String]] = {
    val folderNames = getFileAndFolderNames(link)
    folderNames.map(folderName => folderName -> getFileAndFolderNames(link + folderName + "/NDVI/").map(img => s"$link$folderName/NDVI/$img")).toMap
  }
}
