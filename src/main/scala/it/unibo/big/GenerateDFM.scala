package it.unibo.big

object GenerateDFM extends App {


  import it.unibo.big.calculated_svp.SVPStatistics
  import it.unibo.big.casedimension.{GenerateCaseDimensionTable, GenerateTrapDimension}
  import it.unibo.big.cer.GenerateCERDimensionTables
  import it.unibo.big.normalized_fact.{GenerateNormalizedFactWithMeteo, GenerateTrapsValidity}
  import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.slf4j.{Logger, LoggerFactory}

  private val sparkSession = SparkSession.builder().master("local[*]").appName("BMSB DFM creation").getOrCreate()
  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
  private val config: Config = ConfigFactory.load()

  apply()

  def apply(): Unit = {
    //read case input data and create a temp view for each file
    val caseInputData: Map[String, DataFrame] = config.getConfig("dataset.CASE").entrySet().toArray
      .map(_.asInstanceOf[java.util.Map.Entry[String, ConfigValue]]).map(
        t => {
          val df = sparkSession.read.option("header", "true").csv(t.getValue.render().replaceAll("\"", ""))
          t.getKey -> df
        }).toMap
    val cerInputData = ???
    val mapImagesSVP : Map[String, Array[String]] = ???
    val croppedDf : (Int, Map[String, DataFrame]) => DataFrame = ???
    val croppedGroundTruthDf : (Int, Map[String, DataFrame]) => DataFrame = ???
    val weatherDf : DataFrame = ???

    //generate the normalized fact table
    val normalizedFinalDf: DataFrame = GenerateNormalizedFactWithMeteo(sparkSession, caseInputData, weatherDf)
    //generate the traps validity table
    val trapsValidityDf : DataFrame = GenerateTrapsValidity(sparkSession, caseInputData)
    //generate the trap dimension table
    var trapsDimensionTable : DataFrame = GenerateTrapDimension(sparkSession, caseInputData)
    trapsDimensionTable = trapsDimensionTable.join(trapsValidityDf, Seq("gid"))
    //calculate the automatic SVP
    val automaticSVP = SVPStatistics.calculateAutomaticSVP(sparkSession, caseInputData ++ cerInputData, trapRadius = 200, mapImagesSVP, croppedDf)
    //calculate the ground truth SVP
    val groundTruthSVP = SVPStatistics.getGroundTruthSVP(sparkSession, trapRadius = 200, caseInputData ++ cerInputData, croppedGroundTruthDf)

    //join the automatic svp with the trap dimension
    trapsDimensionTable = trapsDimensionTable.join(automaticSVP, Seq("gid"), "left")
      .join(groundTruthSVP, Seq("gid"), "left")

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
