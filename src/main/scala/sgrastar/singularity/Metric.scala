package com.criteo.sre.storage.sgrastar.singularity

import java.nio.ByteBuffer
import org.apache.cassandra.db.marshal.{CompositeType, UTF8Type}
import org.apache.lucene.document._
import org.apache.lucene.index.Term

object Metric {
  val ElementSeparator = '.'

  val FieldID = "id"
  val FieldLength = "length"
  val FieldPrefix = "f"
  val FieldToken = "token"
  val FieldFacetPrefix = "ft"

  def fromString(str: String): Metric =
    // No checking on empty/invalid string because it should be done
    // at the time of data ingestion, not inside C*.
    Metric(str, str.count('.'==) + 1)

  /** Extracts metric from a Lucene document.
    */
  def fromDocument(doc: Document): Metric = {
    val path = doc.getField(FieldID).stringValue
    val length = doc.getField(FieldLength).numericValue.intValue

    Metric(path, length)
  }

  /** Extracts metric from a given composite row key.
    * The row key is expected to contain the metric path in its first component.
    */
  def fromCompositeKey(ck: ByteBuffer): Metric = {
    val pathBin = CompositeType.extractComponent(ck, 0)
    val path = UTF8Type.instance getString pathBin

    Metric fromString path
  }

  /** Converts part of a Graphite metric pattern into an equivalent regular expression.
    *
    * Warning: This method does not try to parse the pattern, and contains no correctness checks.
    *          As such, trying to use interrogation marks in a metric path with [?] will fail.
    */
  def elementToRegex(pattern: String): String =
    Seq("{" -> "(", "}" -> ")", "," -> "|", "?" -> ".", "*" -> ".*?")
      .foldLeft(pattern) { case (x, (from, to)) => x.replace(from, to) }
}

/** Represents a Graphite metric.
  * The path should be in the form "a.b.c.d", and the length is the number of
  * path elements (separated by full stops).
  */
case class Metric(path: String, length: Int) {
  override def toString: String =
    path

  /** Creates a Lucene document which fully represents this metric,
    * and could be used to index the current metric.
    */
  def toDocument(): Document = {
    val doc = new Document()
    doc.add(new StringField(Metric.FieldID, path, Field.Store.YES))
    doc.add(new IntField(Metric.FieldLength, length, Field.Store.YES))

    for ((part, idx) <- path.split(Metric.ElementSeparator).view.zipWithIndex)
      doc.add(new StringField(Metric.FieldPrefix + idx, part, Field.Store.NO))

    doc
  }

  /** Creates a composite row key for this metric.
    * The composite key is expected to be in the form (path, length).
    */
  def toCompositeKey(keyType: CompositeType): ByteBuffer =
    keyType.decompose(path, Integer valueOf length)

  /** Creates a Lucene Term which can be used to identify this metric.
    */
  def toTerm(): Term =
    new Term(Metric.FieldID, path)
}
