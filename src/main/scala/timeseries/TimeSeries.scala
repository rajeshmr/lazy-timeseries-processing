package timeseries

import scala.io.Source

object TimeSeries {

  val WINDOW = 60

  case class Event(timeStamp:Long, priceRatio:Double, eventsCount:Int,
                   rollingSum:Double, minPriceRatio:Double, maxPriceRatio:Double){
    var prev:Option[Event] = None
    def header = "T\tV\tN\tRS\tMinV\tMaxV"
    override def toString = s"$timeStamp\t$priceRatio\t$eventsCount\t$rollingSum\t$minPriceRatio\t$maxPriceRatio"
    def ++(that:Event) = {
      val e = Event(this.timeStamp, this.priceRatio,
        this.eventsCount + that.eventsCount,
        this.rollingSum + that.priceRatio,
        Seq(this.minPriceRatio, that.priceRatio).min,
        Seq(this.maxPriceRatio, that.priceRatio).max
      )
      e.prev = that.prev
      e
    }
  }

  def toEvent(string:String) = {
    val splits = string.split("[ \t]")
    val timestamp = splits(0).toLong
    val priceRatio = splits(1).toDouble
    Event(timestamp, priceRatio, 1, priceRatio, priceRatio, priceRatio)
  }

  def toChain(events:Stream[Event]) = events.head #:: events.zip(events.tail).map{
      case (prev, current) => current.prev = Option(prev); current
  }

  def aggregate(current:Event):Event = current.prev match {
    case Some(prev) if current.timeStamp - prev.timeStamp < WINDOW => aggregate(current ++ prev)
    case _ => current
  }

  def main(args: Array[String]) {
    val events = Source.fromFile("/Users/raj/workspace/scala-de-challenge/src/main/resources/data_scala.txt").getLines()
      .map(toEvent)

    val chainedEvents = toChain(events.toStream)
      .map(aggregate)
      .foreach(p => println(p))
  }
}
