import java.text.SimpleDateFormat
import java.util.Date

import org.specs2._
import org.tazeen727.yhacessor._

import scala.math._

/**
  * Created by tazeen727 on 2016/02/20.
  */
object YahooFinanceSpec extends Specification { def is = s2"""
    fetch Yahoo! finance data
      return empty seq when invalid term               $e0
      get stock price of one day                       $e1
      get stock split info                             $e2
      get stock prices of 3 days                       $e3
      get stock prices of multi-pages                  $e4
      get stock prices of 2 weeks                      $e5
      get stock prices of 2 months with split          $e6
  """

  val df = new SimpleDateFormat("yyyy-MM-dd")

  def e0 = {
    // condition
    val code = "4503"
    val from = df.parse("2016-02-09")
    val to   = df.parse("2016-02-08")
    val term = Daily

    val data = YahooFinance.pullPriceSeries(code, from, to, term)

    beRight(data)

    data.right.get must have size 0
  }

  def e1 = {
    // condition
    val code = "4503"
    val from = df.parse("2016-02-08")
    val to   = df.parse("2016-02-08")
    val term = Daily

    val data = YahooFinance.pullPriceSeries(code, from, to, term)

    beRight(data)

    val result = data.right.get

    result must have size 1

    result.head must beAnInstanceOf[YfPriceData]

    val result1 = result.head.asInstanceOf[YfPriceData]

    result1.date     must_== df.parse("2016-02-08")
    result1.opening  must_== BigDecimal("1635.5")
    result1.high     must_== BigDecimal("1668.5")
    result1.low      must_== BigDecimal("1619")
    result1.closing  must_== BigDecimal("1662.5")
    result1.volume   must_== BigDecimal("6091900")
    result1.adjusted must_== BigDecimal("1662.5")
  }

  def e2 = {
    // condition
    val code = "1433"
    val from = df.parse("2016-01-27")
    val to   = df.parse("2016-01-27")
    val term = Daily

    val data = YahooFinance.pullPriceSeries(code, from, to, term)

    beRight(data)
    data.right.get.head must beAnInstanceOf[YfSplitData]
    val result = data.right.get.head.asInstanceOf[YfSplitData]

    result.date       must_== df.parse("2016-02-08")
    result.splitRatio must_== BigDecimal("2")
  }

  def e3 = {
    // condition
    val code = "4503"
    val from = df.parse("2016-02-08")
    val to   = df.parse("2016-02-10")
    val term = Daily

    val data = YahooFinance.pullPriceSeries(code, from, to, term)

    beRight(data)

    data.right.get must have size 3

    val result = data.right.get
    result must allOf {
      beAnInstanceOf[YfPriceData]
    }

    result.map(_.asInstanceOf[YfPriceData].date) must allOf {
      between(df.parse("2016-02-08"), df.parse("2016-02-10"))
    }
  }

  def e4 = {
    // condition
    val code = "4503"
    val from = df.parse("2016-01-13")
    val to   = df.parse("2016-02-11")
    val term = Daily

    val data = YahooFinance.pullPriceSeries(code, from, to, term)

    beRight(data)

    data.right.get must have size 21

    val result = data.right.get
    val result1 = result(20).asInstanceOf[YfPriceData]

    result1.date    must_== df.parse("2016-01-13")
    result1.closing must_== BigDecimal("1649")
  }

  def e5 = {
    // condition
    val code = "4503"
    val from = df.parse("2016-01-13")
    val to   = df.parse("2016-02-11")
    val term = Weekly

    val data = YahooFinance.pullPriceSeries(code, from, to, term)

    beRight(data)

    data.right.get must have size 4

    val result = data.right.get.asInstanceOf[Seq[YfPriceData]]

    result(0).date must_== df.parse("2016-02-08")
    result(1).date must_== df.parse("2016-02-01")
    result(2).date must_== df.parse("2016-01-25")
    result(3).date must_== df.parse("2016-01-18")
  }

  def e6 = {
    // condition
    val code = "1433"
    val from = df.parse("2015-12-01")
    val to   = df.parse("2016-02-01")
    val term = Monthly

    val data = YahooFinance.pullPriceSeries(code, from, to, term)

    beRight(data)

    data.right.get must have size 4

    val result = data.right.get

    result(0).date must_== df.parse("2016-02-01")
    result(1).date must_== df.parse("2016-01-27")
    result(1).asInstanceOf[YfSplitData].splitRatio must_== BigDecimal("2")
    result(2).date must_== df.parse("2016-01-01")
    result(3).date must_== df.parse("2015-12-01")
  }



}
