package com.john.test

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit}

import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by Dong on 2016/9/18.
  *
  * @return
  */
object SingleSerialARIMA {

  def main(args: Array[String]): Unit = {
    val jsonData = Source.fromFile("src/main/resources/data/R_ARIMA_DataSet2.csv").mkString
    println(jsonData)
    val result = forecast(jsonData,2)
    print(result)
  }

  def makeJSON(a: Any): String = a match {
    case m: Map[String, Double] => m.map {
      case (time, content) => "\"" + time + "\":" + content
    }.mkString("{", ",", "}")
    case l: ArrayBuffer[Any] => l.map(makeJSON).mkString("[", ",", "]")
    case s: String => "\"" + s + "\""
  }

  def forecast(jsonData: String, nFutures: Int) : String = {
    var historyList: List[Map[String,Double]] = null
    val futureData = new ArrayBuffer[Map[String,Double]]()
    val tree = JSON.parseFull(jsonData)
    tree match {
      case Some(list: List[Map[String,Double]]) => historyList = list
      case None =>
      case other =>
    }
    if (historyList == null) return ""
    val historyData = historyList.map { item =>
      item.head._2
    }
    val datetimeformater = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
    val lastTime = LocalDateTime.parse(historyList(historyList.size-1).head._1, datetimeformater)
    val lastButOneTime = LocalDateTime.parse(historyList(historyList.size-2).head._1, datetimeformater)
    val duration = lastButOneTime.until(lastTime,ChronoUnit.SECONDS)
    val ts = Vectors.dense(historyData.toArray)
    val arimalModel = ARIMA.autoFit(ts)
    val forecast = arimalModel.forecast(ts,nFutures)
    for (i <- 0 until nFutures) {
      futureData += Map(lastTime.plusSeconds(duration*(i+1)).format(datetimeformater) -> forecast.apply(ts.size + i))
    }
    return makeJSON(futureData)
  }

}
