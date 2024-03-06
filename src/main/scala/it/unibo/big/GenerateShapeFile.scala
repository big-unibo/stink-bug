package it.unibo.big

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point, PrecisionModel}
import org.apache.spark.sql.functions.{col, udf}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
object SparkToShapefile extends App {
  import Utils._

  GeoSparkSQLRegistrator.registerAll(sparkSession)

  val caseInputDataset = Utils.readCaseInputData()
  private def getGeom = udf((geom: String) => {
    try {
      val geometry = readGeometry(geom)
      val p = geometry.geom.asInstanceOf[geotrellis.vector.Point]
      new Point(new Coordinate(p.x, p.y), new PrecisionModel(), geometry.srid).asInstanceOf[Geometry]
    } catch {
      case _: Exception => null
    }
  })
  val trapDf = caseInputDataset("traps")
      .where(col("geometry").isNotNull).withColumn("geometry", getGeom(col("geometry")))
  trapDf.printSchema()
  //java.lang.String cannot be cast to
  val spatialRDD = Adapter.toSpatialRdd(trapDf, "geometry")
  val shapefileDf = Adapter.toDf(spatialRDD,sparkSession)
  shapefileDf.printSchema()
  shapefileDf.show()
  // Save the Spatial RDD as a shapefile TODO not works
  shapefileDf.write.format("shapefile").option("path", "output.shp").save()
  //spatialRDD.saveAsGeoJSON("output.geojson")
  // Save the Spatial RDD as a Shapefile*/
  sparkSession.stop()
}