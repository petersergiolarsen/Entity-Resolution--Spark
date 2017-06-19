package plotting



import org.junit.Test
import plotly.{Bar, Plotly, Scatter, element, layout}
import plotly.element._
import plotly.layout.Layout



/**
  * Created by peter on 6/19/17.
  */
class testplotting {



  @Test
  def test1(): Unit = {

    val trace1 = Scatter(
      Seq(1, 2, 3, 4),
      Seq(10, 15, 13, 17)
    )

    val trace2 = Scatter(
      Seq(1, 2, 3, 4),
      Seq(16, 5, 11, 9)
    )



    val data = Seq(trace1, trace2)

    Plotly.plot("peter",data,Layout.apply("first plot"))










  }

}
