package it.unibo.big.calculated_svp

/**
 * Utils for the SVP statistics
 */
object SVPUtils {

  import geotrellis.raster.io.geotiff.GeoTiff
  import geotrellis.raster.{CellGrid, DoubleCellType, MultibandTile, Tile, isData}
  import geotrellis.spark.io.hadoop._
  import geotrellis.vector.Geometry
  import org.apache.hadoop.fs.Path
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import geotrellis.vector.io.readWktOrWkb
  import org.slf4j.{Logger, LoggerFactory}

  private val MULTIBAND_INDEXES = Seq("RGB")
  private[calculated_svp] val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Returns the info for each trap of the analyzed, with the specified function
   * @param dataframe the dataframe to get the info
   *                  the dataframe must have a column named 0 that is an identifier and is integer
   * @param fun the function to apply to the dataframe
   * @tparam T the type of the result of the function
   */
  private def getTrapsInfo[T](dataframe: DataFrame, fun: Row => T): Map[Int, T] = {
    val dfList = dataframe.collect ().toSeq
    var fieldsMap = Map[Int, T]()
    dfList.foreach {
      x => fieldsMap += x.getInt ( 0 ) -> fun(x)
    }
    fieldsMap
  }

  /***
   *
   * @param sparkSession the spark session
   * @param index the index to get
   * @param title the title of the image
   * @param date the date of the image
   * @return the tile and the source of the image
   */
  def getIndexImageInfo ( sparkSession: SparkSession, index: String, title: String, date: String ): (CellGrid, GeoTiff[_ >: MultibandTile with Tile <: CellGrid]) = {

    val file_name = s"<DIRECTORY>/$date/$index/${title}_$index.tif"

    if(MULTIBAND_INDEXES.contains(index)) {
      val indexSource = HadoopGeoTiffReader
        .readMultiband ( new Path ( file_name ) )( sparkSession.sparkContext )

      (indexSource.tile, indexSource)
    } else {
      val indexSource = HadoopGeoTiffReader
        .readSingleband ( new Path ( file_name ) )( sparkSession.sparkContext )

      (indexSource.tile.convert ( DoubleCellType ), indexSource)
    }
  }

  /**
   *
   * @param trapRadius radius of trap used for calculate the area
   * @param indexSource source of image
   * @param t tile of image
   * @param geom geometry to crop and mask
   * @param fun a function that given a value returns a boolean for update a data counter
   * @return the cropped result and the covered area of the geometry in the exact cropped area
   */
  def cropGeometryAndGetCoverage(trapRadius: Int, indexSource: GeoTiff[_ >: MultibandTile with Tile <: CellGrid], t: Tile, geom: Geometry, fun: Double => Boolean ): (Tile, Double) = {
    val radiusArea = getRadiusAreaFromRadius(trapRadius)
    //count the amount of data cells
    val (res, dataCount) = cropAndGetValues(indexSource, t, geom, fun)
    val coveredArea = (dataCount * 100D) / radiusArea

    LOGGER.debug(s"Data count = $dataCount, radius = $radiusArea")
    (res, coveredArea)
  }

  /**
   *
   * @param indexSource source of image
   * @param t tile of image
   * @param geom geometry to crop and mask

   * @return the cropped image and the counter value
   */
  private def cropAndGetValues(indexSource: GeoTiff[_ >: MultibandTile with Tile <: CellGrid], t: Tile, geom: Geometry, fun: Double => Boolean): (Tile, Int) = {
    val res = t.crop(indexSource.extent, geom.envelope).mask(geom.envelope, geom)

    var funCount = 0
    res.foreachDouble { x =>
      if (isData(x)) {
        funCount += (if(fun(x)) 1 else 0)
      }
    }

    (res, funCount)
  }

  /**
   *
   * @param trapRadius the selected radius for the trap buffer
   * @return the area (a costant) of the selected radius
   */
  def getRadiusAreaFromRadius(trapRadius: Int): Int = {
    require(trapRadius == 200 || trapRadius == 1000)
    if (trapRadius == 200) 1313 else 31533
  }

  /**
   *
   * @param trapRadius the used radius
   * @param croppedDf a function that starting from the trap radius and the input dataframes returns
   *                  a new dataframe where:
   *                  - the first column is an integer identifier for the trap
   *                  - the second column is a geometry that is intersection between the buffer constructed using
   *                    the trap radius and the data about the cultures
   *                  - the third is the trap buffer built around the trap with a radius of trapRadius
   * @param inputDataframes the dataframes to use for the query
   * @return a map with traps ids,
   *         the geometry of the data in the radius where data about cultures is cropped and
   *         the whole buffer of the trap that is used for the coverage, with a radius of trapRadius
   */
  def q(trapRadius: Int, inputDataframes: Map[String, DataFrame], croppedDf: (Int, Map[String, DataFrame]) => DataFrame): Map[Int, Option[(Geometry, Geometry)]] = {
    //join the dataset with external table in order to
    // have the geometry that is the difference between the trap and the cultures in the trapRadius area
    getTrapsInfo(croppedDf(trapRadius, inputDataframes), x => if (x.isNullAt(1)) None else Some(readWktOrWkb(x.getString(1)), readWktOrWkb(x.getString(2))))
  }
}