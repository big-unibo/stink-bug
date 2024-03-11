package it.unibo.big

object GenerateCUBE extends App {

  import Utils._
  import it.unibo.big.calculated_svp.SVPStatistics
  import it.unibo.big.casedimension.{GenerateCaseDimensionTable, GenerateTrapDimension}
  import it.unibo.big.environment_registry.GenerateEnvironmentRegistryDimensionTables
  import it.unibo.big.normalized_fact.{WeatherReader, GenerateNormalizedFactWithMeteo, GenerateTrapsValidity}
  import org.apache.spark.sql.DataFrame
  import org.slf4j.{Logger, LoggerFactory}
  import it.unibo.big.ImagesMap.generateImagesMap

  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  apply()

  def apply(): Unit = {
    //read case input data and create a temp view for each file
    val caseInputData: Map[String, DataFrame] = Utils.readInputData("CASE")

    val environmentRegistryInputData = Utils.readInputData("environment_registry")

    val link = config.getString("dataset.satellite_images")

    // download the images from link
    val mapImagesSVP : Map[String, Array[String]] = generateImagesMap(link)
    val weatherDf : DataFrame = WeatherReader.readWeatherShapefile(config.getString("dataset.weather"))
    //generate the normalized fact table
    val normalizedFinalDf: DataFrame = GenerateNormalizedFactWithMeteo(sparkSession, caseInputData, weatherDf)
    //generate the traps validity table
    val trapsValidityDf : DataFrame = GenerateTrapsValidity(sparkSession, caseInputData)
    //generate the trap dimension table
    var trapsDimensionTable : DataFrame = GenerateTrapDimension(sparkSession, caseInputData)
    trapsDimensionTable = trapsDimensionTable.join(trapsValidityDf, Seq("gid"))
    //calculate the automatic SVP
    val automaticSVP = SVPStatistics.calculateAutomaticSVP(sparkSession, caseInputData ++ environmentRegistryInputData, trapRadius = 200, mapImagesSVP)

    //join the automatic svp with the trap dimension
    trapsDimensionTable = trapsDimensionTable.join(automaticSVP, Seq("gid"), "left")

    //generate the case dimension and bridge tables
    val (caseDimensionDf, caseBridgeDf) = GenerateCaseDimensionTable(sparkSession, caseInputData, trapsDimensionTable)
    //generate the environment registry dimension and bridge tables
    val environmentRegistryDimensions = GenerateEnvironmentRegistryDimensionTables(sparkSession, environmentRegistryInputData, trapsDimensionTable)

    for((name, (dim, bridge)) <- environmentRegistryDimensions) {
      FileUtils.saveFile(dim, config.getString(s"dataset.CUBE.dim_$name"))
      FileUtils.saveFile(bridge, config.getString(s"dataset.CUBE.bridge_trap_$name"))
    }
    FileUtils.saveFile(trapsDimensionTable, config.getString("dataset.CUBE.dim_trap"))
    FileUtils.saveFile(normalizedFinalDf, config.getString("dataset.CUBE.fact_passive_monitoring"))
    FileUtils.saveFile(caseDimensionDf, config.getString("dataset.CUBE.dim_case"))
    FileUtils.saveFile(caseBridgeDf, config.getString("dataset.CUBE.bridge_trap_case"))
    FileUtils.saveFile(caseInputData("monitoring_session"), config.getString("dataset.CUBE.monitoring_session"))
  }
}
