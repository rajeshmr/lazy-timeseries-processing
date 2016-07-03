package timeseries

import org.scalatest.{FlatSpec, Matchers}


class TimeSeriesSpec extends FlatSpec with Matchers {

  "TimeSeries" should "convert tab/space separated string to event" in {
    Event("1\t1.0") should be(Event(1, 1.0, 1, 1.0, 1.0, 1.0))
    Event("1 1.0") should be(Event(1, 1.0, 1, 1.0, 1.0, 1.0))
  }

  it should "chain events" in {
    val e1 = Event(1, 1.0, 1, 1.0, 1.0, 1.0)
    val e2 = Event(2, 2.0, 2, 2.0, 2.0, 2.0)
    val e3 = Event(3, 3.0, 3, 3.0, 3.0, 3.0)
    val chained = TimeSeries.toChain(Stream(e1, e2, e3))
    chained.head.previous should be(None)
    chained(1).previous should be (Some(e1))
    chained(2).previous should be (Some(e2))
  }

  it should "aggregate two events using ++ def" in {
    val previous = Event(1, 1.0, 1, 1.0, 1.0, 1.0)
    val current = Event(2, 2.0, 2, 2.0, 2.0, 2.0)
    val expected = Event(2, 2.0, 3, 3.0, 1.0, 2.0)
    expected.previous = Some(previous)
    current ++ previous should be(expected)
  }

  it should "aggregate based on window" in {
    val e1 = Event(10, 1.0, 1, 1.0, 1.0, 1.0)
    val e2 = Event(21, 2.0, 2, 2.0, 2.0, 2.0)
    val e3 = Event(3, 3.0, 3, 3.0, 3.0, 3.0)
  }

  it should "return a stream give a input text file" in {
    val stream = TimeSeries(this.getClass.getResource("/test_input.txt").getPath)
    stream.head should be(Event(1355270609, 1.80215, 1, 1.80215, 1.80215, 1.80215))
    stream(1) should be(Event(1355270621,	1.80185,	2,	3.604,	1.80185,	1.80215))
    stream(2) should be(Event(1355270646,	1.80195,	3,	5.40595,	1.80185,	1.80215))
    stream(3) should be(Event(1355270702,	1.80225,	2,	3.6042,	1.80195,	1.80225))
    stream(4) should be(Event(1355270702,	1.80215,	3,	5.40635,	1.80195,	1.80225))
    stream(5) should be(Event(1355270829,	1.80235,	1,	1.80235,	1.80235,	1.80235))
  }





}