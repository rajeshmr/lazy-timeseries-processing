package timeseries

import scala.io.Source


object TimeSeries {
  case class Event(timeStamp:Long, priceRatio:Double, eventsCount:Int = 0,
                   rollingSum:Double=0, minPriceRatio:Double=0, maxPriceRatio:Double=0){
    def header = "T\tV\tN\tRS\tMinV\tMaxV"

    override def toString = s"$timeStamp\t$priceRatio\t$eventsCount\t$rollingSum\t$minPriceRatio\t$maxPriceRatio"
  }
  case class Chain(left:Option[Event], right:Option[Event])

  def toEvent(string:String) = {
    val splits = string.split("[ \t]")
    Event(splits(0).toLong, splits(1).toDouble)
  }

  def toChain(events:Stream[Event]) = Chain(None,Option(events.head)) #:: events.zip(events.tail)
    .map{ case (prev, current) => Chain(Option(prev), Option(current))}

  def main(args: Array[String]) {
    val events = Source.fromFile("/Users/raj/workspace/scala-de-challenge/src/main/resources/data_scala.txt").getLines()
      .map(toEvent)
      .toStream

    val chainedEvents = toChain(events)



  }
}
