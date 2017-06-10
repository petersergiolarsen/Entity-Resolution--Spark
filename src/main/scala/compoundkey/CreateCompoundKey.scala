package compoundkey

import org.apache.spark.rdd.RDD

/**
  * Created by peter on 6/4/17.
  */
class CreateCompoundKey[T] extends Serializable{


  def setup(data:T): String =
  {
    return concatenate(data)
  }


  def concatenate(data: T): String = {

    return data.toString.replace(" ","").trim

  }


}
