package spark

import compoundkey.CreateCompoundKey
import createseeds.ConstructSeeds
import datahasher.DataHasher
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, BeforeClass, Test}
import shingles.CreateShingles
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import spark.datarepo.schema
import util.{Compute, Jaccard}

import scala.util.hashing.MurmurHash3

/**
  * Created by peter on 6/4/17.
  */
class testGraphXAndEntityRes {

  var sc:SparkContext=_
  var conf: SparkConf = _

  @Before
  def setup(): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutil")

    conf = new SparkConf().setAppName("graphER").setMaster("local")
    sc=new SparkContext(conf)

  }

  @Test
  def testCreateCompoundKey(): Unit = {

    val key:CreateCompoundKey[DataObj] = new CreateCompoundKey[DataObj]
    val shingles:CreateShingles=new CreateShingles()

    val data = sc.parallelize(getData())

    val compoundKey = data.map(x => key.setup(x._2))

    compoundKey.collect().foreach(x=> println(x))

  }

  @Test
  def testShingles(): Unit = {

    val data = getDataForshingles()

    val shingles:CreateShingles=new CreateShingles()

    val res =shingles.create(data,3)

    val l = data.length

    assert(res!=null)

  }


  @Test
  def constructER(): Unit = {

    // https://github.com/mrsqueeze/spark-hash/blob/master/src/main/scala/com/invincea/spark/hash/LSH.scala

    val minClusterSize=2

    val nBands:Int = 4

    val nFunctions:Int = 400

    val shingleSize:Int = 4

    val key:CreateCompoundKey[DataObj] = new CreateCompoundKey[DataObj]

    val shingling:CreateShingles=new CreateShingles()

    val constructSeeds:ConstructSeeds = new ConstructSeeds(nFunctions)

    val data = sc.parallelize(getData())

    val seeds = constructSeeds.createMinHashSeeds()

    val lookUpTable = constructSeeds.createLookUpTable()

    val brSeed =sc.broadcast(seeds)

    val brLookUp=sc.broadcast(lookUpTable)

    val compoundKey = data.map(x => (x._1.toString,key.setup(x._2)))

    val shingles = compoundKey.map(y=>(y._1,shingling.create(y._2,shingleSize)))

    val minHash = shingles.map(x => (x._1,brSeed.value.map(k => x._2.map(elm => DataHasher.hash(elm, k._1, k._2, brLookUp.value)).min).sortBy(s=>s).toList))

    val banded = minHash.flatMap(x=> x._2.grouped(nBands).toList.map(x=> x.hashCode()%nBands).zipWithIndex.map(h =>
      (h._2, (x._2.grouped(nBands).toList.zipWithIndex.takeWhile(elm => elm._2 == h._1), x._1)))).filter(r=> r._2._1.nonEmpty)

    val grouped=banded.groupByKey().filter(elm => elm._2.size>=minClusterSize).map(g=> g._2.map(f=> (g._1, (f._1.head._1, f._2))))


    val res = grouped.flatMap(x=> Compute.compute(x.toList))

    var finalres = res.collect()

    println("")

  }

  @Test
  def convertToParquet(): Unit =
  {
    val data = schema.getDataSqlRow()
    val sql:SQLContext = SQLContext.getOrCreate(sc)
    val sparkData =sc.parallelize(data)
    val df =sql.createDataFrame(sparkData,schema.struct)

    df.write.mode(SaveMode.Overwrite).parquet("C:/Users/g49629/IdeaProjects/ER/src/test/resources/testdata")

  }




  @Test
  def testRowRem(): Unit = {

    val r:Row = Row(1,2,3,4,5,6,8,7)
    val s = r.toSeq.filter(v=> v!=1)

    println(s)

  }


  @Test
  def testArray(): Unit = {

    val a:Array[Int] = new Array[Int](10)
    a.update(0,1)
    a.update(1,4)
    a.update(2,5)
    a.update(3,6)
    a.update(4,66)
    a.update(5,1)
    a.update(6,1)
    a.update(7,1)
    a.update(8,1)
    a.update(9,1)

    val g =a.grouped(2).toList
    println("")
  }

  @Test
  def testJaccard(): Unit = {

    var s1:List[Long] = List(12,12,23,23,23,1)


    var s2:List[Long] = List(12,12,23,23,23,12)

    var rs=Jaccard.jaccard(s1,s2)

    println(rs)

  }

  @Test
  def testDataHasher(): Unit = {

    val p = "peter"
    var constructSeeds:ConstructSeeds = new ConstructSeeds(nFunctions = 100)
    val seeds = constructSeeds.createMinHashSeeds()
    val lookUpTable = constructSeeds.createLookUpTable()

    val res =seeds.map((x) => DataHasher.hash(p,x._1,x._2,lookUpTable)).min

    println(res)

  }

  @Test
  def testSeed(): Unit = {

    val seed:ConstructSeeds = new ConstructSeeds(40)

    val seeds =seed.createMinHashSeeds()
    val lookUptable = seed.createLookUpTable()

    assert(seeds.length==40)
    assert(lookUptable.length==256)

  }




  @Test
  def testSwap(): Unit = {

    val tup:Tuple2[Long,Long] = new Tuple2[Long,Long](1l,10l)

    val res=tup.swap

    assert(res._1==10L)
    assert(res._2==1L)
  }


  def getData(): Array[Tuple2[Long,DataObj]] = {

    return Array((11L, new DataObj(1L,"peter","lertoften","dk",1234.90)),
      (22L,new DataObj(2L,"peter","lertooften","dk",3234.90)),
      (33L,new DataObj(3L,"anders","solsiden","dk",5234.90)),
      (44L,new DataObj(4L,"andeers","solsiden","dk",8234.90)),
      (55L,new DataObj(5L,"mads","solvænget","us",8234.90)),
      (66L,new DataObj(6L,"mads","solvaenget","us",164.90)),
      (77L,new DataObj(7L,"kaarl","solsikken","se",7234.90)),
      (88L,new DataObj(8L,"karl","soolsikken","",12224.90)),
      (99L,new DataObj(9L,"karl","solsiken","se",23232.90)),
      (1010L,new DataObj(10L,"søren","lertoften, roskilde","dk",1234.90)),
      (1111L,new DataObj(11L,"soren","lertoften, roskilde","dk",1234.90)),
      (1212L,new DataObj(12L,"fhfhdhdh","sddsdsfgsgsdf","ooooo",1234.90))
    )

  }

  def getDataForshingles(): String = {

    return "petersergiolarsenlertoften2640roskilde01023456solsiden21allerød3450"

  }

}


class DataObj(id:Long,name:String,adress:String,country:String,moneyTransferred:Double) extends Serializable
{

  val Id: Long =id
  val Name: String = name
  val Adress: String =adress
  val Country: String = country
  val MoneyTransferred: Double =moneyTransferred

  override def toString: String = {

    return name+adress+country

  }


}










