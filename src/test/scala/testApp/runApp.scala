package testApp

import app.RunApp
import com.google.common.io.Resources
import org.junit.{Before, Test}

/**
  * Created by G49629 on 07-06-2017.
  */
class runApp {

  val pathToProps = Resources.getResource("config").getPath
  val fileName = "configurationparquet.properties"

  @Before
  def setup(): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutil")

  }

  @Test
  def test(): Unit = {

    val args:Array[String] = new Array[String](2)

    args(0) = pathToProps
    args(1) = fileName

    RunApp.main(args)



  }


}
