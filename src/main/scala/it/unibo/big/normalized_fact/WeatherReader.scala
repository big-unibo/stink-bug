package it.unibo.big.normalized_fact

object WeatherReader extends App {
  import it.unibo.big.Utils.config
  import org.apache.commons.io.FileUtils
  import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
  import org.datasyslab.geosparksql.utils.Adapter

  import java.io.File
  import java.net.URL

  def readWeatherShapefile(path: String) : Unit = {
    import it.unibo.big.Utils._
    val tmpFolder = "tmp/weather"
    val fileNames = getFileAndFolderNames(path)
    //download all files from path and save in the temporary folder (if not already present)
    fileNames.foreach(x => {
      val tmpFile = new File(s"$tmpFolder/$x")
      if(!tmpFile.exists()) {
        FileUtils.copyURLToFile(new URL(path + x), tmpFile)
      }
    })

    val spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, tmpFolder)
    val df = Adapter.toDf(spatialRDD, sparkSession)
    df.printSchema()
    df.show()
  }
  //TODO check error hadoop, remove geom column (?)
  readWeatherShapefile(config.getString("dataset.weather"))
}
