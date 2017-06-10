package spark.datarepo

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by peter on 07-06-2017.
  */
object schema {

  val callId = StructField("id", StringType)
  val oCallId = StructField("name", StringType)
  val callTime = StructField("address", StringType)
  val duration = StructField("country", StringType)

  val struct = StructType(Array(callId, oCallId, callTime, duration))

  def getDataSqlRow(): Array[Row] = {

    return Array(Row("1L","peter","lertoften","dk"),
      Row("2L","peter","lertooften","dk"),
      Row("3L","anders","solsiden","dk"),
      Row("4L","andeers","solsiden","dk"),
      Row("5L","mads","solvænget","us"),
      Row("6L","mads","solvaenget","us"),
      Row("7L","kaarl","solsikken","se"),
      Row("8L","karl","soolsikken",""),
      Row("9L","karl","solsiken","se"),
      Row("10L","søren","lertoften, roskilde","dk"),
      Row("11L","soren","lertoften, roskilde","dk"),
      Row("12L","fhfhdhdh","sddsdsfgsgsdf","ooooo")
    )

  }
}
