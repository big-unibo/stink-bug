package it.unibo.big

object GenerateDFM extends App {

  import Utils._
  import it.unibo.big.calculated_svp.SVPStatistics
  import it.unibo.big.casedimension.{GenerateCaseDimensionTable, GenerateTrapDimension}
  import it.unibo.big.cer.GenerateCERDimensionTables
  import it.unibo.big.normalized_fact.{GenerateNormalizedFactWithMeteo, GenerateTrapsValidity}
  import it.unibo.big.service.HBase.readWeatherSequenceFile
  import it.unibo.big.service.Postgres
  import org.apache.spark.sql.DataFrame
  import org.slf4j.{Logger, LoggerFactory}

  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  apply()

  def apply(): Unit = {
    //read case input data and create a temp view for each file
    val caseInputData: Map[String, DataFrame] = Utils.readCaseInputData()

    val postgres = new Postgres(sparkSession, "cimice")
    val cerInputData = Map("water_basin" -> postgres.queryTable("select d_ty_sda, geom4326 from acque_interne"),
      "water_course" -> postgres.queryTable("select prenome as praenomen, uso as usage, tombinato as culverted, geom4326 from retebonifica"),
      "crop" -> postgres.queryTable("select raggruppam, geom4326 from uso_suolo"))

    // Read from PostgreSQL table into DataFrame
    val mapImagesSVP : Map[String, Array[String]] = ???
    val croppedDf : (Int, Map[String, DataFrame]) => DataFrame = ???
    val croppedGroundTruthDf : (Int, Map[String, DataFrame]) => DataFrame = ???
    val weatherDf : DataFrame = readWeatherSequenceFile(sparkSession, s"/abds/hbase/weather_processed")
    val calculateSVP = false //TODO set to true

    //generate the normalized fact table
    val normalizedFinalDf: DataFrame = GenerateNormalizedFactWithMeteo(sparkSession, caseInputData, weatherDf)
    //generate the traps validity table
    val trapsValidityDf : DataFrame = GenerateTrapsValidity(sparkSession, caseInputData)
    //generate the trap dimension table
    var trapsDimensionTable : DataFrame = GenerateTrapDimension(sparkSession, caseInputData)
    trapsDimensionTable = trapsDimensionTable.join(trapsValidityDf, Seq("gid"))
    if(calculateSVP) {
      //calculate the automatic SVP
      val automaticSVP = SVPStatistics.calculateAutomaticSVP(sparkSession, caseInputData ++ cerInputData, trapRadius = 200, mapImagesSVP, croppedDf)
      //calculate the ground truth SVP
      val groundTruthSVP = SVPStatistics.getGroundTruthSVP(sparkSession, trapRadius = 200, caseInputData ++ cerInputData, croppedGroundTruthDf)

      //join the automatic svp with the trap dimension
      trapsDimensionTable = trapsDimensionTable.join(automaticSVP, Seq("gid"), "left")
        .join(groundTruthSVP, Seq("gid"), "left")
    }

    //generate the case dimension and bridge tables
    val (caseDimensionDf, caseBridgeDf) = GenerateCaseDimensionTable(sparkSession, caseInputData, trapsDimensionTable)
    //generate the CER dimension and bridge tables
    val CERDimensions = GenerateCERDimensionTables(sparkSession, cerInputData, trapsDimensionTable)

    for((name, (dim, bridge)) <- CERDimensions) {
      FileUtils.saveFile(dim, config.getString(s"dataset.DFM.dim_$name"))
      FileUtils.saveFile(bridge, config.getString(s"dataset.DFM.bridge_trap_$name"))
    }
    FileUtils.saveFile(trapsDimensionTable, config.getString("dataset.DFM.dim_trap"))
    FileUtils.saveFile(normalizedFinalDf, config.getString("dataset.DFM.fact_passive_monitoring"))
    FileUtils.saveFile(caseDimensionDf, config.getString("dataset.DFM.dim_case"))
    FileUtils.saveFile(caseBridgeDf, config.getString("dataset.DFM.bridge_trap_case"))
    FileUtils.saveFile(caseInputData("monitoring_session"), config.getString("dataset.DFM.monitoring_session"))
  }
}
