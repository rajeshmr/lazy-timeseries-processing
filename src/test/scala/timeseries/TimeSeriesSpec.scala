package timeseries

import org.scalatest.{Matchers, FlatSpec}
import timeseries.TimeSeries.{Chain, Event}


class TimeSeriesSpec extends FlatSpec with Matchers{

  "TimeSeries" should "convert tab/space separated string to event" in {
    TimeSeries.toEvent("1\t1.0") should be(Event(1, 1.0))
    TimeSeries.toEvent("1 1.0") should be(Event(1, 1.0))
  }

  it should "transform event stream to event chain" in {
    val events = Stream(Event(1,1.0), Event(2,2.0), Event(3,3.0))
    val expected = Stream(Chain(None, Some(Event(1,1.0))),
      Chain(Some(Event(1,1.0)), Some(Event(2,2.0))),
      Chain(Some(Event(2,2.0)), Some(Event(3,3.0)))
    )
    TimeSeries.toChain(events) should be(expected)
  }



}
