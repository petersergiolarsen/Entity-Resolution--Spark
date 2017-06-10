package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.junit.{After, Before, Test}

/**
  * Created by peter on 5/29/17.
  */
class testSparkElastic{

  var sc: SparkContext = _



  @Before
  def setup(): Unit =
  {

    val conf:SparkConf = new SparkConf().setAppName("test").setMaster("local")
    conf.set("es.nodes","localhost:9200")
    conf.set("es.resource","bank/account")
    conf.set("spark.speculation","false")
    sc = new SparkContext(conf)

  }

  @After
  def close(): Unit ={

    sc.stop()


  }

  @Test
  def testOne(): Unit =
  {
    val data = sc.parallelize(getDataAsObj())

    data.collect().foreach(x=> println(x.id+" : "+x.name +" : " + x.lastName +" : " +x.account +" : " +x.address))

  }


  @Test
  def writeToElastic(): Unit =
  {

    val data = sc.parallelize(getDataAsObj())
    EsSpark.saveToEs(data,"bank/account")

  }


  def getData() : Seq[Pair[Int,String]] = {

    return Seq(Pair(1,"peter"),Pair(2,"marting"))

  }

  def getDataAsObj() : Seq[BankObj] = {

    Seq(BankObj(122,"Peter","Larsen","Nordea","Lertoften"),
      BankObj(123,"Martin","An","Danske Banke","Solsiden"))
  }

}


case class BankObj(id:Int,name:String,lastName:String,account:String, address:String)

