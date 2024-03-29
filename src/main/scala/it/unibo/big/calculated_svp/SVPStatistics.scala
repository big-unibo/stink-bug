package it.unibo.big.calculated_svp

object SVPStatistics extends App {

  import geotrellis.raster.Tile
  import geotrellis.raster.io.geotiff.SinglebandGeoTiff
  import it.unibo.big.calculated_svp.SVPUtils._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}

  import java.io.FileNotFoundException

  /**
   *
   * @param sparkSession the used spark session
   * @param inputData    map of case and environment registry input data
   * @param trapRadius   the radius of the trap to consider in the calculation of the automatic SVP (200 or 1000)
   * @param mapImages    a map where each key is a date and the value is a list of image paths for that date,
   *                     where extract the vegetation index
   * @return the dataframe with the result of the calculation of automatic SVP */
  def calculateAutomaticSVP(sparkSession: SparkSession, inputData: Map[String, DataFrame], trapRadius: Int, mapImages: Map[String, Array[String]]): DataFrame = {
    require(trapRadius == 200 || trapRadius == 1000)
    val trapsMap = q(trapRadius, inputData)
    //used vegetation index for threshold calculation
    val vegetationIndexesThresholds = Map("NDVI" -> 0.7)

    //For each gid and date returns a map of index and values and percentage of coverage
    var trapsDates = Map[(Int, String), Map[String, (Double, Double)]]()
    mapImages.foreach { case (date, images) => //add all gid date pairs, with zero coverage
      trapsMap.keys.foreach(gid => trapsDates += (gid, date) -> vegetationIndexesThresholds.keys.toSeq.map(i => i -> (0D, 0D)).toMap)

      for (path <- images) {
        try {
          for ((index, threshold) <- vegetationIndexesThresholds) {
            val (t, s) = getIndexImageInfo(sparkSession, path)
            val convertedTile = t.asInstanceOf[Tile]
            val indexSource = s.asInstanceOf[SinglebandGeoTiff]

            //use only traps in extent
            trapsMap.filter(_._2.isDefined).filter(x => indexSource.extent.contains(x._2.get._1)).foreach { case (gid, Some((differenceGeom, bufferGeom))) => val (resPoly, indexResult) = cropGeometryAndGetCoverage(trapRadius, indexSource, convertedTile, differenceGeom, _ >= threshold)
              val (_, coveredArea) = cropGeometryAndGetCoverage(trapRadius, indexSource, convertedTile, bufferGeom, _ => true)

              LOGGER.debug(
                s"""Trap $gid is in area with $indexResult % (radius $trapRadius)
                   |total values ${resPoly.size}
                   |Area covered $coveredArea %
                   |path = $path index = $index
                   |""".stripMargin)

              if (!indexResult.isNaN) {
                val (lastIndexValue, lastCoverage) = trapsDates((gid, date))(index)
                if (lastCoverage < coveredArea || (lastCoverage == coveredArea && indexResult > lastIndexValue)) {
                  trapsDates += (gid, date) -> (trapsDates((gid, date)) + (index -> (indexResult, coveredArea)))
                }
              }
            }
          }
        } catch {
          case _: FileNotFoundException => LOGGER.error(s"File not found $path")
          case e: Exception => LOGGER.error(e.getMessage)
        }
      }
    }
    val trapsIndexDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(trapsDates.toSeq.map { case ((gid, date), mapIndex) => Row(Seq(gid, date) ++ vegetationIndexesThresholds.keys.toSeq.flatMap(mapIndex(_).productIterator): _*)
    }), StructType(Seq(StructField("gid", IntegerType), StructField("date", StringType)) ++ vegetationIndexesThresholds.keys.toSeq.flatMap(ix => Seq(StructField(ix, DoubleType), StructField(s"${ix}_coverage", DoubleType)))))
    trapsIndexDf.select("gid", "NDVI").groupBy("gid").avg("NDVI").withColumnRenamed("avg(NDVI)", "svp (auto)")
  }

}