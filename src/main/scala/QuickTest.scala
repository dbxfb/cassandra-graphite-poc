package com.criteo.sre.storage.sgrastar.singularity

import java.nio.file.Paths
import lucene.MetricsIndex
import scala.io.Source

object QuickTest {
  def main(args: Array[String]) = {
    val indexPath = Paths.get("./lucene-test-idx")

    val idx = if (!indexPath.toFile.exists) {
      val idx = new MetricsIndex("ks", Option(indexPath),  100000)
      println("Index directory does not exist, importing sample data...")

      var i = 0
      val (_, insertTime) = time(() => {
        for (line <- Source.fromFile("/home/dpanth3r/sandbox/metrics/metrics_201503").getLines) {
          idx.insert(Metric fromString line, 0L)
          i += 1
          if (i % 100000 == 0)
            println(s"Pushed $i entries ${System.currentTimeMillis()/1000}")
        }
      })
      idx.commit(true)

      println(s"Inserting sample dataset: ${insertTime/1000}ms")
      idx
    } else {
      new MetricsIndex("ks", Option(indexPath), 100000)
    }

    val request = "h*.*"
    println("Search " + request + ": " + idx.search(request, Long.MinValue, Long.MaxValue))

    val search = time(() => idx.search("he*.*", Long.MinValue, Long.MaxValue), 1000)
    println("Average time per search: " + search + "us")

    Map(
      "basic matching short" ->
        "carbon.agents.6c-3b-e5-be-04-e4-a.memUsage",
      "basic matching long" ->
        "criteo.arbitrageService.counter.sum.businessmodels.scndpricefactorcache.agesum.10.us.us_nyc.hostw075-ny8",
      "basic non-matching short" ->
        "carbon.agents.6c-3b-e5-be-04-e4-a.memUsages",
      "basic non-matching long" ->
        "criteo.arbitrageService.counter.sum.businessmodels.scndpricefactorcache.agesum.10.us.us_nyc.hostw075-ny89",
      "wildcards" ->
        "criteo.AdStatTool.counter.avg.*.*.EU.monitor.start.*.*.*",
      "regexps" ->
        "criteo.recoService.counter.sum.Reco.CAS.{LPV,RecoPredABTest}.{B,BC}.NbReturnedItems.*.us_*.hostw[01]?0-*",
      "re2" ->
        "criteo.arbitrageService.counter.sum.businessmodels.scndpricefactorcache.{agesum,value}.*.*.*.hostw[012]??-ny8"
    )
      .map { case (desc, pattern) =>
        benchmark(desc, () => idx.search(pattern, Long.MinValue, Long.MaxValue))
    }

    println("Index size: " + idx.size() / (1024*1024) + "MiB")

    idx.close()
  }

  def benchmark(desc: String, action: () => Seq[Metric]) = {
    val bench = time(action, 1000)
    val res = action()
    println(s"${desc} => ${res.size} results, ${bench.toFloat/1000f}ms/op (1000 ops)")

    res
      .toStream
      .map(m => println(s"  - ${m.path}"))
      .drop(3)
  }

  def time[U](action: () => U): (U, Long) = {
    val start = System.nanoTime()
    val out = action()
    val end = System.nanoTime()

    (out, (end - start) / 1000)
  }

  def time[U](action: () => U, count: Long): Long = {
    val start = System.nanoTime()
    for (i <- 1L to count)
      action()
    val end = System.nanoTime()

    (end - start) / 1000 / count.toLong
  }
}
