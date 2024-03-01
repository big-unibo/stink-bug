package it.unibo.big

import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.math.{asin, cos, pow, sin, sqrt}

object FileUtils {
  /**
   * Default options for the save
   */
  private val optionsMapDefault = Map("header" -> "true", "decimal" -> ",", "sep" -> ";")

  /**
   * Save a DataFrame to a file
   * @param df DataFrame to save
   * @param fileName name of the file
   * @param repartition if true, repartition the DataFrame to 1 partition
   * @param optionsMap options for the save
   */
  def saveFile(df: DataFrame, fileName: String, repartition: Boolean = true, optionsMap: Map[String, String] = optionsMapDefault): Unit = {
    (if (repartition) df.repartition(1) else df).write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .options(optionsMap)
      .save(fileName)
  }
}