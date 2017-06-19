package utilcustom

/**
  * Created by peter on 06-06-2017.
  */
object Jaccard extends Serializable{


  def jaccard(a1 : List[Long], b1 : List[Long]) : Double = {

    val aa1=a1.toSet
    val bb1=b1.toSet

    val intersect = aa1.intersect(bb1).size.doubleValue
    val union = aa1.union(bb1).size.doubleValue

    return intersect/union
  }
}
