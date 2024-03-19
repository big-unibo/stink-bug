package it.unibo.big

object GenerateCube extends App {

  import Utils._
  import it.unibo.big.calculated_svp.SVPStatistics
  import it.unibo.big.casedimension.{GenerateCaseDimensionTable, GenerateTrapDimension}
  import it.unibo.big.environment_registry.GenerateEnvironmentRegistryDimensionTables
  import it.unibo.big.normalized_fact.{WeatherReader, GenerateNormalizedFactWithMeteo, GenerateTrapsValidity}
  import it.unibo.big.service.Postgres
  import org.apache.spark.sql.DataFrame
  import org.slf4j.{Logger, LoggerFactory}
  import it.unibo.big.ImagesMap.generateImagesMap

  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  apply()

  def apply(): Unit = {
    //read case input data and create a temp view for each file
    val caseInputData: Map[String, DataFrame] = Utils.readInputData("CASE")

    val postgres = new Postgres(sparkSession, "cimice")
    val environmentRegistryInputData = Map("water_basin" -> postgres.queryTable("select d_ty_sda as type_name, '\"' || ST_AsEWKT(geom4326) || '\"' as geom4326, ms_id from acqueinterne"),
      "water_course" -> postgres.queryTable("select prenome as praenomen, uso as usage, tombinato as culverted, '\"' || ST_AsEWKT(geom4326) || '\"' as geom4326, ms_id from retebonifica"),
      "crop" -> postgres.queryTable("select raggruppam as crop_type, '\"' || ST_AsEWKT(geom_4326) || '\"' as geom4326, ms_id from uso_suolo"))

   //TODO change val environmentRegistryInputData = Utils.readInputData("environment_registry")

    val link = config.getString("dataset.satellite")

    // download the images from link
    val mapImagesSVP : Map[String, Array[String]] = generateImagesMap(link)
    val weatherDf : DataFrame = WeatherReader.readWeatherShapefile(config.getString("dataset.weather"))

    FileUtils.saveFile(caseInputData("monitoring_session"), config.getString("dataset.cube.monitoring_session"))
    //generate the normalized fact table
    val normalizedFinalDf: DataFrame = GenerateNormalizedFactWithMeteo(sparkSession, caseInputData, weatherDf)
    FileUtils.saveFile(normalizedFinalDf, config.getString("dataset.cube.fact_passive_monitoring"))
    //generate the traps validity table
    val trapsValidityDf : DataFrame = GenerateTrapsValidity(sparkSession, caseInputData)
    //generate the trap dimension table
    var trapsDimensionTable : DataFrame = GenerateTrapDimension(sparkSession, caseInputData)
    trapsDimensionTable = trapsDimensionTable.join(trapsValidityDf, Seq("gid"))
    //calculate the automatic SVP
    val automaticSVP = SVPStatistics.calculateAutomaticSVP(sparkSession, caseInputData ++ environmentRegistryInputData, trapRadius = 200, mapImagesSVP)

    //join the automatic svp with the trap dimension
    trapsDimensionTable = trapsDimensionTable.join(automaticSVP, Seq("gid"), "left")
    FileUtils.saveFile(trapsDimensionTable, config.getString("dataset.cube.dim_trap"))
    //generate the case dimension and bridge tables
    val (caseDimensionDf, caseBridgeDf) = GenerateCaseDimensionTable(sparkSession, caseInputData, trapsDimensionTable)
    FileUtils.saveFile(caseDimensionDf, config.getString("dataset.cube.dim_case"))
    FileUtils.saveFile(caseBridgeDf, config.getString("dataset.cube.bridge_trap_case"))
    //generate the environment registry dimension and bridge tables
    val environmentRegistryDimensions = GenerateEnvironmentRegistryDimensionTables(sparkSession, environmentRegistryInputData, caseInputData("traps"))

    for((name, (dim, bridge)) <- environmentRegistryDimensions) {
      FileUtils.saveFile(dim, config.getString(s"dataset.cube.dim_$name"))
      FileUtils.saveFile(bridge, config.getString(s"dataset.cube.bridge_trap_$name"))
    }
  }
}
