package timeseries

import scala.io.Source
import Implicits._

object Implicits{
  case class Precision(p:Double)
  implicit class DoubleWithAlmostEquals(val d:Double) extends AnyVal {
    def ~=(d2:Double)(implicit p:Precision) = (d - d2).abs < p.p
  }
}

case class Event(timeStamp:Long, priceRatio:Double, eventsCount:Int,
                 rollingSum:Double, minPriceRatio:Double, maxPriceRatio:Double){
  implicit val precision = Precision(0.00001)
  var previous:Option[Event] = None
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
  override def toString = f"$timeStamp $priceRatio%.5f $eventsCount $rollingSum%.5f $minPriceRatio%.5f $maxPriceRatio%.5f"
  override def equals(any:Any) = any match {
    case that:Event => Seq(this.timeStamp==that.timeStamp, this.priceRatio ~= that.priceRatio, this.eventsCount == that.eventsCount,
    this.rollingSum ~= that.rollingSum, this.minPriceRatio ~= that.minPriceRatio, this.maxPriceRatio ~= that.maxPriceRatio).reduce(_ && _)
    case _ => false
  }
}

object Event{
  def apply(string:String):Event = {
    val splits = string.split("[ \t]")
    val timestamp = splits(0).toLong
    val priceRatio = splits(1).toDouble
    Event(timestamp, priceRatio, 1, priceRatio, priceRatio, priceRatio)
  }
}

object TimeSeries {
  val WINDOW = 60
  def printHeader(){
    println(s"T${" " * 10}V${" "*7}N${" " * 1}RS${" "*6}MinV${" "*4}MaxV")
    println("-" * 45)
  }

  def toChain(events:Stream[Event]) = events.head #:: events.zip(events.tail).map{
      case (prev, current) => current.previous = Option(prev); current
  }

  def aggregate(current:Event):Event = current.previous match {
    case Some(prev) if current.timeStamp - prev.timeStamp < WINDOW => aggregate(current ++ prev)
    case _ => current
  }

  def apply(file:String) = {
    val events = Source.fromFile(file).getLines().map(s => Event(s))
    toChain(events.toStream).map(aggregate)
  }

  def main(args: Array[String]) {
    if(args.length < 1){
      println("Usage: timeseries.TimeSeries <input_file>")
      System.exit(0)
    }
    printHeader()
    TimeSeries(args(0)).foreach(p => println(p))
  }
}
