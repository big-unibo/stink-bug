package it.unibo.big.service

import org.apache.spark.sql.{DataFrame, SparkSession}
import it.unibo.big.FileUtilsYaml
import java.io.{BufferedReader, InputStreamReader}

class Postgres(private val sparkSession: SparkSession, databaseName: String) {

  private val ymlJson =
    FileUtilsYaml.loadYamlIntoJsonNode(
      new BufferedReader(
        new InputStreamReader(getClass.getResourceAsStream("/credentials.yml"))
      )
    )
  private val url: String = "jdbc:postgresql://" +
    ymlJson.path("postgres").path("host").asText() +
    ":" + ymlJson.path("postgres").path("port").asText() +
    "/" + databaseName
  private val user: String = ymlJson.path("postgres").path("user").asText()
  private val password: String = ymlJson.path("postgres").path("password").asText()
  // Tell spark which driver to use and load the class
  private val driver = "org.postgresql.Driver"
  Class.forName(driver)

  /**
    * Query table and get as a DataFrame
    */
  def queryTable(query: String): DataFrame = {
    sparkSession
      .sqlContext
      .read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("pas" +
        "sword", password)
      .option("query", query)
      .load()
  }

}