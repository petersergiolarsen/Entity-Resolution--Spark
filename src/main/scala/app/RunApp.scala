package app

import java.util.Properties

import compoundkey.CreateCompoundKey
import createseeds.ConstructSeeds
import datahasher.DataHasher
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import schema.LSHModel
import shingles.CreateShingles
import utilcustom.Compute

/**
  * Created by peter on 06-06-2017.
  */
class RunApp {

  var cx:SparkContext=_

  def run(spConf: SparkConf, pathToData: String,
          pathToSaveResult: String, tmbTableName: String,
          selectFields: String, minClusterSize: Int,
          nBands: Int, nHashFunctions: Int,
          shingleSize: Int, debug: Boolean=true): Int = {

    try
    {
      cx = new SparkContext(spConf)
      cx.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

      val sql: SQLContext = SQLContext.getOrCreate(cx)
      sql.setConf("spark.sql.parquet.compression.codec", "snappy")
      sql.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      sql.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      val df = readData(sql, pathToData)
      df.registerTempTable(tmbTableName)

      if (debug){

        df.show()

      }


      val dfSubset = sql.sql(selectFields).rdd

      val transformed = dfSubset.map(x=> (x.apply(0).asInstanceOf[String],x.toSeq.filter(g=>  g!=x.apply(0))))

      val resRDD = runLSH(cx,transformed,minClusterSize,nBands,nHashFunctions,shingleSize).filter(r=> r!=null)

      val dfRes = sql.createDataFrame(resRDD,LSHModel.struct)

      if (debug)
        {
          dfRes.show()
        }
      else{


        dfRes.write.mode(SaveMode.Overwrite).parquet(pathToSaveResult)

      }



      return 1

    }
    catch {

      case e: Exception => e.printStackTrace()

       return -1
    }
    finally {

      cx.stop()

    }
  }

   def readData(sQLContext: SQLContext,path: String): DataFrame = {

      val df =sQLContext.read.parquet(path)
      return df
   }

  def runLSH(sc:SparkContext,data:RDD[(String,Seq[Any])],minClusterSize: Int,
             nBands: Int, nHashFunctions: Int,
             shingleSize: Int): RDD[Row] ={


    val key:CreateCompoundKey[Seq[Any]] = new CreateCompoundKey[Seq[Any]]

    val shingling:CreateShingles=new CreateShingles()

    val constructSeeds:ConstructSeeds = new ConstructSeeds(nHashFunctions)

    val seeds = constructSeeds.createMinHashSeeds()

    val lookUpTable = constructSeeds.createLookUpTable()

    val brSeed =sc.broadcast(seeds)

    val brLookUp=sc.broadcast(lookUpTable)

    val compoundKey = data.map(x => (x._1,key.setup(x._2)))

    val shingles = compoundKey.map(y=>(y._1,shingling.create(y._2,shingleSize)))

    val minHash = shingles.map(x => (x._1,brSeed.value.map(k => x._2.map(elm => DataHasher.hash(elm, k._1, k._2, brLookUp.value)).min).sortBy(s=>s).toList))

    val banded = minHash.flatMap(x=> x._2.grouped(nBands).toList.map(x=> x.hashCode()%nBands).zipWithIndex.map(h =>
      (h._2, (x._2.grouped(nBands).toList.zipWithIndex.takeWhile(elm => elm._2 == h._1), x._1)))).filter(r=> r._2._1.nonEmpty)

    val grouped=banded.groupByKey().filter(elm => elm._2.size>=minClusterSize).map(g=> g._2.map(f=> (g._1, (f._1.head._1, f._2))))

    val res = grouped.flatMap(x=> Compute.compute(x.toList))

    res

  }





}

object RunApp {

  var localOrCluster: Int = _
  var master: String = _
  var appName: String = _
  var nameNode: String = _
  var pathToLoadDataFrom: String = _
  var pathToSaveDataTo: String = _
  var tmbTableName: String = _
  var selectFields: String = _

  var debug: Boolean = _

  var minClusterSize:Int=_
  var nBands:Int=_
  var nHashFunctions:Int=_
  var shingleSize:Int=_


  def main(args: Array[String]): Unit = {

    val pathProps: String = args(0)
    val configFileName: String = args(1)

    val hadoopConf: Configuration = getHadoopConf()

    val props: Properties = readPropteries(hadoopConf, pathProps + "/" + configFileName)
    setupPropertyVariables(props)

    if (localOrCluster == -1) {
      master = "local"
    } else {

      master = props.getProperty("spark.master")
    }

    val spConf: SparkConf = getConf(appName, master)
    val run = new RunApp()

    if (run.run(spConf, nameNode + pathToLoadDataFrom, nameNode + pathToSaveDataTo,tmbTableName,selectFields,minClusterSize,nBands,nHashFunctions,shingleSize,debug) == 1) {
      System.exit(1)
    }
    else {
      System.exit(-1)
    }

  }

  def getConf(appName: String, master: String): SparkConf = {

    val conf: SparkConf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    conf.set("spark.kryo.registrationRequired", "true")
    //    conf.registerKryoClasses(registerKryoClasses())

  }

//  def registerKryoClasses(): Array[Class[_]] = {
//
//    Array(classOf[_])
//  }

  def readPropteries(hadoopConf: Configuration, path: String): Properties = {
    val properties: Properties = new Properties()
    val p: Path = new Path(path)
    val fs: FileSystem = FileSystem.get(hadoopConf)
    properties.load(fs.open(p))
    return properties

  }

  def getHadoopConf(): Configuration = {

    val conf = new Configuration()
    conf.set("hadoop.security.authentication", "kerberos")
    return conf
  }

  def setupPropertyVariables(properties: Properties): Unit = {

    nameNode = properties.getProperty("nameNode")
    appName = properties.getProperty("spark.app.name")
    pathToLoadDataFrom = properties.getProperty("pathToLoadDataFrom")
    pathToSaveDataTo = properties.getProperty("pathToSaveDataTo")
    localOrCluster = properties.getProperty("localOrCluster").toInt
    tmbTableName = properties.getProperty("tmbTableName")
    selectFields = properties.getProperty("selectFields")

    minClusterSize = properties.getProperty("minClusterSize").toInt
    nBands = properties.getProperty("nBands").toInt
    nHashFunctions = properties.getProperty("nHashFunctions").toInt
    shingleSize = properties.getProperty("shingleSize").toInt


    debug = properties.getProperty("debug").toBoolean


  }
}