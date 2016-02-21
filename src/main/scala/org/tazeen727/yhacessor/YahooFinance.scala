package org.tazeen727.yhacessor

import java.net.URL
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date, TimeZone}

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.Exception._

/**
  * Created by tazeen727 on 2016/02/17.
  */
object YahooFinance {

  def pullPriceSeries(stockCode: String,
                      from: Date,
                      to: Date,
                      term: TermType,
                      timeoutMillis: Int = 100000): Either[Throwable, Seq[YfData]] = {

    def scrapePages(doc: Document, buff: mutable.Buffer[YfData]): Either[Throwable, Seq[YfData]] = {
      parseDataTable(doc, term).right.map { seq =>
        buff ++= seq

        doc.select("ul.ymuiPagingBottom.clearFix > a")
        .filter(_.text == "次へ")
        .map(_.attr("href")) match {
          case Seq(nextHref) =>
            val nextUrl = new URL(nextHref)
            val maybeNextDoc = allCatch either Jsoup.parse(nextUrl, timeoutMillis)
            maybeNextDoc.right flatMap { doc =>
              scrapePages(doc, buff)
            }
          case _ => Right(buff.toSeq)
        }
      }.joinRight
    }

    // make url
    val url = makeUrlQuery(stockCode, from, to, term)

    val maybeDoc = allCatch either Jsoup.parse(url, timeoutMillis)
    maybeDoc.right.map { doc =>
      val buff = mutable.ListBuffer[YfData]()
      scrapePages(doc, buff)
    }.joinRight
  }

  def makeUrlQuery(stockCode: String, from: Date, to: Date, term: TermType, page: Int = 1): URL = {
    // example:
    // http://info.finance.yahoo.co.jp/history/?code=9740&sy=2016&sm=1&sd=18&ey=2016&em=2&ed=17&tm=d&p=1

    val buf = new StringBuilder("http://info.finance.yahoo.co.jp/history/")

    // stock code
    buf.append(s"?code=${stockCode}")

    // from date
    val sCal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"))
    sCal.setTime(from)
    buf.append(s"&sy=${sCal.get(Calendar.YEAR)}")
    buf.append(s"&sm=${sCal.get(Calendar.MONTH) + 1}")
    buf.append(s"&sd=${sCal.get(Calendar.DAY_OF_MONTH)}")

    // to date
    val eCal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"))
    eCal.setTime(to)
    buf.append(s"&ey=${eCal.get(Calendar.YEAR)}")
    buf.append(s"&em=${eCal.get(Calendar.MONTH) + 1}")
    buf.append(s"&ed=${eCal.get(Calendar.DAY_OF_MONTH)}")

    // term type (daily/weekly/monthly)
    val tm = term match {
      case Daily   => "d"
      case Weekly  => "w"
      case Monthly => "m"
    }
    buf.append(s"&tm=${tm}")

    // page
    buf.append(s"&p=${page}")

    new URL(buf.toString())
  }

  private val dateFormatByDate  = new SimpleDateFormat("yyyy年M月d日")
  private val dateFormatByMonth = new SimpleDateFormat("yyyy年M月")
  private val decimalFormat     = new DecimalFormat("#,###.#")
  decimalFormat.setParseBigDecimal(true)
  private val stockSplitPattern = """^分割: (\d+(?:\.\d+)?)株 -> (\d+(?:\.\d+)?)株$""".r

  def parseDataTable(doc: Document, term: TermType): Either[Throwable, Seq[YfData]] =
    allCatch either doc.select("table.boardFin tr").drop(1).map { row =>
      val cells = row.select("td")
      if (cells.size == 7) {
        // prices
        val date = if (term == Monthly) {
          dateFormatByMonth.parse(cells(0).text)
        } else {
          dateFormatByDate.parse(cells(0).text)
        }
        val opening  = BigDecimal(decimalFormat.parse(cells(1).text).asInstanceOf[java.math.BigDecimal])
        val high     = BigDecimal(decimalFormat.parse(cells(2).text).asInstanceOf[java.math.BigDecimal])
        val low      = BigDecimal(decimalFormat.parse(cells(3).text).asInstanceOf[java.math.BigDecimal])
        val closing  = BigDecimal(decimalFormat.parse(cells(4).text).asInstanceOf[java.math.BigDecimal])
        val volume   = BigDecimal(decimalFormat.parse(cells(5).text).asInstanceOf[java.math.BigDecimal])
        val adjusted = BigDecimal(decimalFormat.parse(cells(6).text).asInstanceOf[java.math.BigDecimal])
        YfPriceData(date, opening, high, low, closing, volume, adjusted)

      } else if (cells(1).attr("colspan") == "6") {
        // stock split
        val date = dateFormatByDate.parse(cells(0).text)
        val (before, after) = cells(1).text.trim match {
          case stockSplitPattern(b, a) => (BigDecimal(b), BigDecimal(a))
          case _ => throw new RuntimeException("Unknown row format: " + row.html)
        }
        YfSplitData(date, (after / before).setScale(6))

      } else throw new RuntimeException("Unknown row format: " + row.html)
    }

}

// @formatter:off
abstract sealed class TermType
case object Daily extends TermType
case object Weekly extends TermType
case object Monthly extends TermType
// @formatter:on

sealed abstract class YfData(val date: Date)

case class YfPriceData(override val date: Date,
                       opening: BigDecimal,
                       high: BigDecimal,
                       low: BigDecimal,
                       closing: BigDecimal,
                       volume: BigDecimal,
                       adjusted: BigDecimal) extends YfData(date)

case class YfSplitData(override val date: Date,
                       splitRatio: BigDecimal) extends YfData(date)
