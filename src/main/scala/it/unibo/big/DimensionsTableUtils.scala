package it.unibo.big

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DimensionsTableUtils {

  /**
   *
   * @param sparkSession the used spark session
   * @param idColumn the name of the identifier column
   * @param dimensionTable the dimension table dataframe
   * @param bridgeTable the bridge table dataframe
   * @param dimensionTraps the traps dimension dataframe
   * @return the dimension table and the bridge table with the new rows added
   *         where the traps not have a value in the bridge table are linked
   *         to the new row that is "Nothing"s in the dimension table
   */
  def addNotNearRows(sparkSession: SparkSession, idColumn: String, dimensionTable: DataFrame, bridgeTable: DataFrame, dimensionTraps: DataFrame): (DataFrame, DataFrame) = {
    //insert a new row in the dimension table where each value is Nothing, except the first column which is an identifier and is 0
    val newRow = dimensionTable.columns.map(c => if(c == idColumn) 0 else "Nothing")
    val newRowDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(Row.fromSeq(newRow))), dimensionTable.schema)
    val newDimensionTable = dimensionTable.union(newRowDf)
    //link the new row in the bridge table for the trap that not have a value in the bridge
    val newBridgeTable = dimensionTraps.select("gid")
      .except(bridgeTable.select("gid"))
      .withColumn(idColumn, lit(0)).union(bridgeTable)
    (newDimensionTable, newBridgeTable)
  }
}
