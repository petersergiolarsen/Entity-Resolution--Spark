package schema

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Created by peter on 07-06-2017.
  */
object LSHModel {

  val id1 = StructField("id1", StringType)
  val id2 = StructField("id2", StringType)
  val jaccard = StructField("jaccardcoefficient", DoubleType)

  val struct = StructType(Array(id1,id2,jaccard))



}
