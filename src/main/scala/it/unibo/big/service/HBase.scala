package it.unibo.big.service

object HBase {
  import it.unibo.big.FileUtilsYaml
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.Path
  import org.apache.hadoop.hbase.HBaseConfiguration
  import org.apache.hadoop.hbase.client.Result
  import org.apache.hadoop.hbase.io.ImmutableBytesWritable
  import org.apache.hadoop.hbase.util.Bytes
  import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.functions.{col, to_date}
  import org.apache.spark.sql.types.{FloatType, IntegerType}
  import org.apache.spark.sql.{DataFrame, SparkSession}

  import java.io.{BufferedReader, InputStreamReader}

  private val ymlJson =
    FileUtilsYaml.loadYamlIntoJsonNode(
      new BufferedReader(
        new InputStreamReader(getClass.getResourceAsStream("/credentials.yml"))
      )
    )
  private val hbaseSitePath = new Path(ymlJson.path("hbase").path("hbase_site_path").asText())
  /**
   *
   * @param fileName the name of the sequence file exported from hbase and saved on hdfs
   *                 with the command hbase org.apache.hadoop.hbase.mapreduce.Export <table> <hbase-path>
   * @return the dataframe of the file name converted using the function
   */
  def readWeatherSequenceFile(sparkSession: SparkSession, fileName: String): DataFrame = {

    val conf = createConfiguration(Map("io.serializations" -> "org.apache.hadoop.io.serializer.WritableSerialization,org.apache.hadoop.hbase.mapreduce.ResultSerialization"))
    // Load an RDD of (ImmutableBytesWritable, Result) tuples from the file
    val hBaseRDD = sparkSession.sparkContext.newAPIHadoopFile(
      fileName,
      classOf[SequenceFileInputFormat[ImmutableBytesWritable, Result]], // Storage format of the data to be read
      classOf[ImmutableBytesWritable], // `Class` of the key associated with the `fClass` parameter
      classOf[Result], conf = conf) // `Class` of the value associated with the `fClass` parameter

    fromRDDToWeatherProcessedDataFrame(sparkSession, hBaseRDD)
  }

  private def createConfiguration(optionMap: Map[String, String]): Configuration = {
    val configuration = HBaseConfiguration.create()
    configuration.addResource(hbaseSitePath) // Set hbase-site.xml
    for(option <- optionMap)
      configuration.set(option._1, option._2)
    configuration
  }

  /**
   * Transform RDD resulting from HBase scan of weather_processed table into a DataFrame
   * @param hBaseRDD weather_processed RDD
   * @return weather_processed table as a DataFrame
   */
  def fromRDDToWeatherProcessedDataFrame(sparkSession: SparkSession, hBaseRDD: RDD[(ImmutableBytesWritable, Result)]): DataFrame = {
    import sparkSession.implicits._
    hBaseRDD.map(row => {
        (Bytes.toString(row._1.get()),
          Bytes.toString(row._2.getValue(Bytes.toBytes("d"), Bytes.toBytes("product_type"))),
          Bytes.toString(row._2.getValue(Bytes.toBytes("d"), Bytes.toBytes("lat"))),
          Bytes.toString(row._2.getValue(Bytes.toBytes("d"), Bytes.toBytes("long"))),
          Bytes.toString(row._2.getValue(Bytes.toBytes("d"), Bytes.toBytes("date"))),
          Bytes.toString(row._2.getValue(Bytes.toBytes("d"), Bytes.toBytes("hour"))),
          Bytes.toString(row._2.getValue(Bytes.toBytes("d"), Bytes.toBytes("value"))),
          Bytes.toString(row._2.getValue(Bytes.toBytes("d"), Bytes.toBytes("type"))))
      }).toDF()
      .withColumnRenamed("_1", "rowkey")
      .withColumnRenamed("_2", "product_type")
      .withColumnRenamed("_3", "lat")
      .withColumnRenamed("_4", "long")
      .withColumnRenamed("_5", "date")
      .withColumnRenamed("_6", "hour")
      .withColumnRenamed("_7", "value")
      .withColumnRenamed("_8", "type")
      .withColumn("date", to_date(col("date"), "yyyy/MM/dd"))
      .withColumn("hour", col("hour").cast(IntegerType))
      .withColumn("lat", col("lat").cast(FloatType))
      .withColumn("long", col("long").cast(FloatType))
      .withColumn("value", col("value").cast(FloatType))
  }
}
