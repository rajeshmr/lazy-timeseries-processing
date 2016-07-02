package practice

object Test {

  def main(args: Array[String]) {
    lazy val fibs: Stream[BigInt] = BigInt(0) #:: BigInt(1) #:: fibs.zip(fibs.tail).map { n => n._1 + n._2 }
    fibs.take(10).foreach(p => println(p))

  }

}
