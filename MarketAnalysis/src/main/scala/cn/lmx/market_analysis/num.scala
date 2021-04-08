package cn.lmx.market_analysis

object num {
  def time(timestamp: Long, offset: Long, windowSize: Long) = {
    timestamp - (timestamp - offset + windowSize) % windowSize
  }

  def tomorrow(times: Long): Long = {
    (times / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
  }

  def main(args: Array[String]): Unit = {
    println(System.currentTimeMillis())
    println(tomorrow(System.currentTimeMillis()))
  }
}
