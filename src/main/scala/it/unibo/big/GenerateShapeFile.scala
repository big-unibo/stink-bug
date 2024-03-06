package it.unibo.big

object SparkToShapefile extends App {
  import Utils._
  import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryCollection, Point, PrecisionModel}
  import geotrellis.vector
  import org.apache.spark.sql.functions.{col, udf}
  import org.datasyslab.geospark.spatialRDD.SpatialRDD
  import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
  import org.geotools.data.shapefile.shp.{ShapeType, ShapefileWriter}

  import java.io.File
  import java.nio.channels.FileChannel
  import java.nio.file.StandardOpenOption
  import java.util

  writeShapeFile()

  private def writeShapeFile(): Unit = {
    GeoSparkSQLRegistrator.registerAll(sparkSession)

    val caseInputDataset = Utils.readCaseInputData()

    def getGeom = udf((geom: String) => {
      try {
        val geometry = readGeometry(geom)
        val p = geometry.geom.asInstanceOf[vector.Point]
        new Point(new Coordinate(p.x, p.y), new PrecisionModel(), geometry.srid).asInstanceOf[Geometry]
      } catch {
        case _: Exception => null
      }
    })

    val trapDf = caseInputDataset("traps")
      .where(col("geometry").isNotNull).withColumn("geometry", getGeom(col("geometry")))

    val spatialRDD: SpatialRDD[Geometry] = Adapter.toSpatialRdd(trapDf, "geometry")

    val shapefileWriter = new ShapefileWriter(
      FileChannel.open(new File("output.shp").toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE),
      FileChannel.open(new File("output.shx").toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    )

    val records: util.List[Geometry] = spatialRDD.rawSpatialRDD.collect()
    var geometries = Seq[Geometry]()
    //iter
    val iter = records.iterator()
    while (iter.hasNext) {
      val row: Geometry = iter.next()
      geometries :+= row
    }
    val geometryCollection = new GeometryCollection(geometries.toArray, new PrecisionModel(), 4326)
    shapefileWriter.write(geometryCollection, ShapeType.POINT)
    shapefileWriter.close()

    sparkSession.stop()
  }
}