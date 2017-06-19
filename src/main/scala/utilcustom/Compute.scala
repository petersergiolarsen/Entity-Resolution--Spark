package utilcustom

import org.apache.spark.sql.Row

/**
  * Created by peter on 06-06-2017.
  */
object Compute extends Serializable {

  def compute(data:List[(Int,(List[Long],String))]) : Array[Row] = {

    var fRes:Array[Row] = new Array[Row](data.size)

    for (i <- data.indices){

      var key1:String = data.apply(i)._2._2
      var elm1 = data.apply(i)._2._1

      for (j <- i+1 until data.size)
      {

        var key2:String = data.apply(j)._2._2
        var elm2 = data.apply(j)._2._1

        if (!elm1.equals(elm2))
        {

          var coeff:Double =Jaccard.jaccard(elm1,elm2)

          fRes.update(i,Row(key1,key2,coeff))
        }
      }
    }
    fRes
  }


}
