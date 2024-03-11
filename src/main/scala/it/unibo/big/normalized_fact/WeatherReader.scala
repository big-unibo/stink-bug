package it.unibo.big.normalized_fact

object WeatherReader {
  import org.apache.commons.io.FileUtils
  import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
  import org.datasyslab.geosparksql.utils.Adapter
  import org.apache.spark.sql.DataFrame

  import java.io.File
  import java.net.URL

  def readWeatherShapefile(path: String) : DataFrame = {
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

    //read the shapefile and convert to dataframe
    val spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, tmpFolder)
    Adapter.toDf(spatialRDD, sparkSession)
  }
}
