package datahasher

import org.apache.commons.lang3.RandomUtils

/**
  * Created by peter on 6/5/17.
  */
object DataHasher extends Serializable{

  final val HSTART:Long = 0xBB40E64DA205B064L
  final val HMULT:Long = 7664345821815920749L


  def hash(data:String,seedOne:Long,seedTwo:Long,byteTable:Array[Long]) : Long = {

    var h = seedOne
    val hmult:Long = seedTwo
    val ht:Array[Long] = byteTable
    val len:Int = data.length

    for (i <- 0 until len){
      val ch:Char = data.charAt(i)
      h = (h*hmult)^ht.apply(ch & 0xff)
      h = (h*hmult)^ht.apply((ch>>>8)&0xff)
    }

    return h.abs & 2147482949

  }


}
