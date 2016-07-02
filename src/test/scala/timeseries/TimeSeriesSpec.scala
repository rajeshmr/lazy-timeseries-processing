package timeseries

import org.scalatest.{Matchers, FlatSpec}
import timeseries.TimeSeries
import timeseries.TimeSeries.Event


class TimeSeriesSpec extends FlatSpec with Matchers {

  "TimeSeries" should "convert tab/space separated string to event" in {
    TimeSeries.toEvent("1\t1.0") should be(Event(1, 1.0, 1, 1.0, 1.0, 1.0))
    TimeSeries.toEvent("1 1.0") should be(Event(1, 1.0, 1, 1.0, 1.0, 1.0))
  }

}