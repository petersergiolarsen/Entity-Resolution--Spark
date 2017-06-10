package spark


import java.util.Date

import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.immutable.HashSet

/**
  * Created by peter on 5/20/17.
  */
class TestScalaFunc extends AssertionsForJUnit{



  @Test
  def test1(): Unit =
  {
    val t = TimerAno
    val a:Array[String] = new Array[String](10)
    a(0) = "dd"
    t.main(a)
  }

  @Test
  def test2(): Unit ={

    val s = new Funny(2020,10,22)

    val d = new Date()



    val res =s.<(d)

    println(s.toString())


  }

  @Test
  def test3(): Unit =
  {
    val p = new Point(10,20)
    val res = p.move(10,20)
    assert(res._1==20)
    assert(res._2==40)

  }

  @Test
  def testunion(): Unit = {

    val s = List(1,2,3,4,5,6).toSet
    val d = List(1,2,3,4,10,11,23).toSet

    val res1 = s.intersect(d)
    val res = s.union(d)
    println(res.toString())

  }

  object TimerAno{

    def oncepersec(callback: () => Unit): Unit =
    {
      val i:Int = 10
      var k: Int = 0
      while (k<i) {
        callback();
        Thread sleep  1000
        k+=1;
      }
    }

    def main(args: Array[String]): Unit = {
      oncepersec(() => println("time flies"))
    }

  }

  class Funny(year:Int, month:Int, day:Int) extends Ord
  {

    def y=year
    def m = month
    def d = day

    override def toString(): String= y + "-" + m + "-" + d

    override def <(that: Any): Boolean = {

      if (!that.isInstanceOf[Date])
        error("not instance of date")

      val o = that.asInstanceOf[Date]
      (y<o.getYear) ||
        (y==o.getYear && (m<o.getMonth || (m==o.getMonth && d < o.getDay)))
    }

  }


  trait Ord{

    def < (that: Any): Boolean
    def <=(that: Any): Boolean = (this < that) || (this == that)
    def > (that: Any): Boolean = !(this <= that)
    def >=(that: Any): Boolean = !(this < that)

  }




  class Point(x:Int,y:Int)
  {

    var px = x
    var py = y

    def move(dx:Int,dy:Int): (Int,Int) ={

      px = px+dx
      py = py+dy

      return Pair(x = px,y = py)
    }

    val multiplied = (i:Int) => i*12

  }


}
