package it.unibo.big.normalized_fact

import it.unibo.big.Utils.{config, sparkSession}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter
import org.slf4j.{Logger, LoggerFactory}
import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils

object WeatherReader extends App {
  def readWeatherShapefile(path: String) : Unit = {
    import it.unibo.big.Utils._
    val tmp_folder = "tmp/"
    val fileNames = getFileAndFolderNames(path)
    //download all files from path and save in the temporary folder (if not already present)
    fileNames.foreach(x => {
      val tmpFile = new File(s"$tmp_folder$x")
      if(!tmpFile.exists()) {
        FileUtils.copyURLToFile(new URL(path + x), tmpFile)
      }
    })
    //a .shp file is needed otherwise an exception is thrown
    val shapefileTmpPath = fileNames.filter(_.endsWith(".shp")).map(tmp_folder + _)
      .headOption.getOrElse(throw new Exception(s"No shapefile found in $path"))

    // Read the shapefile into a Spark DataFrame
    val shapefileRDD = ShapefileReader.readToPointRDD(sparkSession.sparkContext, shapefileTmpPath)
    val df = Adapter.toDf(shapefileRDD, sparkSession)
    df.printSchema()
    df.show()
  }
  //TODO check work
  readWeatherShapefile(config.getString("dataset.weather"))
}
