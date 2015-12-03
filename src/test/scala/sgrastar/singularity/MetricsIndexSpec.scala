package com.criteo.sre.storage.sgrastar.singularity
package lucene

import org.scalatest.FlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableDrivenPropertyChecks._

class MetricsIndexSpec
    extends FlatSpec with TableDrivenPropertyChecks {

  val queries = Table(
    "Query" -> "Expected results",

    "a.single.metric" -> Set(
      "a.single.metric"
    ),
    "a.few.metrics.*" -> Set(
      "a.few.metrics.a",
      "a.few.metrics.b",
      "a.few.metrics.c"
    ),
    "lots.of.wildcards.******" -> Set(
      "lots.of.wildcards.a",
      "lots.of.wildcards.b",
      "lots.of.wildcards.c"
    ),
    "metrics.match{ed_by,ing}.s[ao]me.regexp.?[0-9]" -> Set(
      "metrics.matched_by.same.regexp.b2",
      "metrics.matched_by.some.regexp.a1",
      "metrics.matching.same.regexp.w5",
      "metrics.matching.some.regexp.z8"
    )
  )

  val malformedQueries = Table(
    "Malformed query",

    "dots.inside{.a,.choice}",
    "dots.inside.[a.].char"
  )

  def buildIndex(): MetricsIndex = {
    val idx = new MetricsIndex("full", None)

    queries.foreach { case (_, metrics) =>
      metrics.foreach { metric =>
        idx.insert(Metric fromString metric, 0L)
      }
    }

    // Wait for changes to be taken into account
    idx.commit(true)

    idx
  }

  "An empty index" should "return an result set for any given query" in {
    val idx = new MetricsIndex("empty", None)
    forAll(queries) { (query, _) =>
      assert(idx.search(query, Long.MinValue, Long.MaxValue).size == 0)
    }
  }

  "A full index" should "have a strictly positive size" in {
    val idx = buildIndex()
    assert(idx.size > 0)
  }

  it should "return the exact set of expected results for a given query" in {
    val idx = buildIndex()
    forAll(queries) { (query, expectedResults) =>
      val results = idx.search(query, Long.MinValue, Long.MaxValue)
      assert(results.size == expectedResults.size)
      assert(expectedResults.forall(metric => results contains (Metric fromString metric)))
    }
  }

  it should "return an empty result set when running malformed query" in {
    val idx = buildIndex()
    forAll(malformedQueries) { (query) =>
      val results = idx.search(query, Long.MinValue, Long.MaxValue)
      assert(results.size == 0)
    }
  }
}
