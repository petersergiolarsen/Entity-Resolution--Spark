package shingles

import scala.collection.mutable

/**
  * Created by peter on 6/4/17.
  */
class CreateShingles extends Serializable{

  def create(data:String,k:Int): mutable.HashSet[String] = {

    val set:mutable.HashSet[String] = new mutable.HashSet[String]()

    val newData = data.toLowerCase()

    for(i <- 0 until data.length-k+1)
    {
     val sub:String = data.substring(i,i+k)
      set.+=(sub)
    }

    return set

  }

}
