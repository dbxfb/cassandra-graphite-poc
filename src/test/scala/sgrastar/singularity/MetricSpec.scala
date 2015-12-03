package com.criteo.sre.storage.sgrastar.singularity

import org.apache.cassandra.db.marshal.{CompositeType, Int32Type, UTF8Type}
import org.scalatest.FlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableDrivenPropertyChecks._
import scala.util.Try
  import scala.util.matching.Regex

class MetricSpec
    extends FlatSpec with TableDrivenPropertyChecks {
  val samples = Table(
    "String" -> "Parsed metric",
    "" ->
      Metric("", 1),
    "one" ->
      Metric("one", 1),
    "hello.world" ->
      Metric("hello.world", 2),
    "sōmê.métric.wìth.rändðm.acçents.ænd.uŧf-8" ->
      Metric("sōmê.métric.wìth.rändðm.acçents.ænd.uŧf-8", 7),
    "some.random.overly.long.metric.which.is.getting.insane.with.words" ->
      Metric("some.random.overly.long.metric.which.is.getting.insane.with.words", 11)
  )

  "A valid UTF-8 string" should "parse to a valid Metric object" in {
    forAll(samples) { (str, expectedMetric) =>
      assert(Metric fromString str equals expectedMetric)
    }
  }

  "A valid metric" should "be convertible into a Lucene document and back into the same metric" in {
    forAll(samples) { (_, metric) =>
      assert(Metric fromDocument (metric toDocument) equals metric)
    }
  }

  it should "be convertible into a Cassandra composite key and back into the same metric" in {
    val ctype = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance)
    forAll(samples) { (_, metric) =>
      assert(Metric fromCompositeKey (metric toCompositeKey ctype) equals metric)
    }
  }

  it should "be able to generate a term which would matche itself in an index" in {
    forAll(samples) { (_, metric) =>
      assert(metric.toTerm.text == metric.path)
    }
  }

  val queries = Table(
    "Query" -> "Matching metrics",
    "metrics.match{ed_by,ing}.s[ao]me.regexp.?[0-9]" -> Set(
      "metrics.matched_by.same.regexp.b2",
      "metrics.matched_by.some.regexp.a1",
      "metrics.matching.same.regexp.w5",
      "metrics.matching.some.regexp.z8"
    )
  )

  "A valid path expression" should "match corresponding metrics after being translated into a regexp" in {
    forAll(queries) { (query, metrics) =>
      val regexpStr = "^" + query.split('.').map(Metric elementToRegex).mkString("\\.") + "$"
      val regexp = Try(regexpStr.r)
      assert(regexp.isSuccess)
      assert(metrics.forall(metric => (regexp.get findFirstIn metric).get equals metric))
    }
  }
}
