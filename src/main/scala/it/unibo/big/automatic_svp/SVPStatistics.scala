package it.unibo.big.automatic_svp

object SVPStatistics extends App {
  import geotrellis.raster.Tile
  import geotrellis.raster.io.geotiff.SinglebandGeoTiff
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import it.unibo.big.FileUtils
  import it.unibo.big.automatic_svp.SVPUtils._
  import org.apache.spark.sql.{DataFrame, SparkSession}

  import java.io.FileNotFoundException

  /**
   *
   * @param sparkSession the used spark session
   * @param caseInputData map of case input data
   * @param trapRadius the radius of the trap to consider in the calculation of the automatic SVP
   * @param mapImages a map where each key is a date and the value is a list of image paths for that date,
   *                  where extract the vegetation index
   * @param croppedDf a function that starting from the trap radius and the input dataframes returns
   *               a new dataframe where:
   *               - the first column is an integer identifier for the trap
   *               - the second column is a geometry that is intersection between the buffer constructed using
   *                 the trap radius and the data about the cultures
   *               - the third is the trap buffer built around the trap with a radius of trapRadius
   * @param vegetationIndexesThresholds a map where each key is a vegetation index and
   *                                    the value is the threshold, to consider a spontaneous vegetation
   * @return the dataframe with the result of the calculation
   */
  def apply(sparkSession: SparkSession, caseInputData: Map[String, DataFrame],
            trapRadius: Int, mapImages : Map[String, Array[String]], croppedDf: (Int, Map[String, DataFrame]) => DataFrame,
            vegetationIndexesThresholds: Map[String, Double] = Map("NDVI" -> 0.7, "MTCI" -> 1.4)): DataFrame = {
    require(trapRadius == 200 || trapRadius == 1000)
    val trapsMap = q(trapRadius, caseInputData, croppedDf)

    //For each gid and date returns a map of index and values and percentage of coverage
    var trapsDates = Map[(Int, String), Map[String, (Double, Double)]]()
    mapImages.foreach {
      case (date, images) =>
        //add all gid date pairs, with zero coverage
        trapsMap.keys.foreach(gid => trapsDates += (gid, date) -> vegetationIndexesThresholds.keys.toSeq.map(i => i -> (0D, 0D)).toMap)

        for (title <- images) {
          try {
            for ((index, threshold) <- vegetationIndexesThresholds) {
              val (t, s) = getIndexImageInfo(sparkSession, index, title, date)
              val convertedTile = t.asInstanceOf[Tile]
              val indexSource = s.asInstanceOf[SinglebandGeoTiff]

              //use only traps in extent
              trapsMap.filter(_._2.isDefined).filter(x => indexSource.extent.contains(x._2.get._1)).foreach {
                case (gid, Some((differenceGeom, bufferGeom))) =>
                  val (resPoly, indexResult) = cropGeometryAndGetCoverage(trapRadius, indexSource, convertedTile, differenceGeom, _ >= threshold)
                  val (_, coveredArea) = cropGeometryAndGetCoverage(trapRadius, indexSource, convertedTile, bufferGeom, _ => true)

                  println(
                    s"""Trap $gid is in area with $indexResult % (radius $trapRadius)
                       |total values ${resPoly.size}
                       |Area covered $coveredArea %
                       |title = $title index = $index
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
            case _: FileNotFoundException => println(s"File not found $title")
            case e: Exception => println(e.printStackTrace())
          }
        }
    }
    val trapsIndexDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(trapsDates.toSeq.map {
      case ((gid, date), mapIndex) => Row(Seq(gid, date) ++ vegetationIndexesThresholds.keys.toSeq.flatMap(mapIndex(_).productIterator): _*)
    }), StructType(
      Seq(StructField("gid", IntegerType), StructField("date", StringType)) ++
        vegetationIndexesThresholds.keys.toSeq.flatMap(ix => Seq(StructField(ix, DoubleType),
          StructField(s"${ix}_coverage", DoubleType)))
    ))
    FileUtils.saveFile(trapsIndexDf, fileName = s"auto_svp_radius_${trapRadius}.csv")
    trapsIndexDf
  }
}