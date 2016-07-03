package timeseries

import scala.io.Source

object TimeSeries {

  val WINDOW = 60
  def header = {
    println(s"T${" " * 10}V${" "*7}N${" " * 1}RS${" "*6}MinV${" "*4}MaxV")
    println("-" * 45)
  }

  case class Event(timeStamp:Long, priceRatio:Double, eventsCount:Int,
                   rollingSum:Double, minPriceRatio:Double, maxPriceRatio:Double){
    var previous:Option[Event] = None
    override def toString = f"$timeStamp $priceRatio%.5f $eventsCount $rollingSum%.5f $minPriceRatio%.5f $maxPriceRatio%.5f"
    def ++(that:Event) = {
      val aggregatedEvent = Event(this.timeStamp, this.priceRatio,
        this.eventsCount + that.eventsCount,
        this.rollingSum + that.priceRatio,
        Seq(this.minPriceRatio, that.priceRatio).min,
        Seq(this.maxPriceRatio, that.priceRatio).max
      )
      aggregatedEvent.previous = that.previous
      aggregatedEvent
    }
  }

  def toEvent(string:String) = {
    val splits = string.split("[ \t]")
    val timestamp = splits(0).toLong
    val priceRatio = splits(1).toDouble
    Event(timestamp, priceRatio, 1, priceRatio, priceRatio, priceRatio)
  }

  def toChain(events:Stream[Event]) = events.head #:: events.zip(events.tail).map{
      case (prev, current) => current.previous = Option(prev); current
  }

  def aggregate(current:Event):Event = current.previous match {
    case Some(prev) if current.timeStamp - prev.timeStamp < WINDOW => aggregate(current ++ prev)
    case _ => current
  }

  def main(args: Array[String]) {
    header
    val events = Source.fromFile("/Users/raj/workspace/scala-de-challenge/src/main/resources/data_scala.txt").getLines()
      .map(toEvent)

    toChain(events.toStream)
      .map(aggregate)
      .foreach(p => println(p))
  }
}
