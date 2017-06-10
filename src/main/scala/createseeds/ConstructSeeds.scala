package createseeds

import scala.collection.mutable
import scala.util.Random

/**
  * Created by peter on 6/5/17.
  */
class ConstructSeeds(nFunctions:Int) {



  def createMinHashSeeds() : Array[(Long,Long)] = {

    val seed:Array[(Long,Long)] = new Array[(Long, Long)](nFunctions)
    var set:mutable.HashSet[Int] = new mutable.HashSet[Int]()
    var rnd:Random = new Random()

    for (i <- seed.indices)
      {
        var s = (rnd.nextLong(),rnd.nextLong())
        if (set.add(s.hashCode())){

          seed.update(i,s)

        }
        else
        {
          i.-(1)
        }
      }

    return seed
  }

  def createLookUpTable() : Array[Long] = {

    val lookUpTable = new Array[Long](256)
    var h:Long = 0x544B2FBACAAF1684L

    for (i <- lookUpTable.indices)
    {
      for (j<- 0 until 31){
        h = (h>>>7)^h
        h = (h << 11)^h
        h = (h >>> 10)^h
      }

      lookUpTable.update(i,h)
    }

    return lookUpTable

  }

}
