package com.criteo.sre.storage.sgrastar.singularity
package lucene

import org.apache.lucene.index._
import org.apache.lucene.search._
import scala.collection.mutable.ArrayBuffer

/** Result collector passed to the Lucene IndexSearcher.
  *
  * Document scoring is disabled as we do not need scores and they take some
  * time to compute.
  *
  * We store the current LeafReader from the index tree in order to directly
  * read documents from it (with their relative offset), as opposed to computing
  * the absolute document ID (leaf.baseID + offset) and going down the tree with
  * a call to the IndexSearcher.doc method.
  *
  * Finally, we extend ArrayBuffer so that we can both store the results and bec
  * used as an iterable container by external code.
  */
class ResultCollector()
    extends ArrayBuffer[Metric] with Collector with LeafCollector {
  private var reader: LeafReader = null

  override def setScorer(scorer: Scorer) = {}
  override def needsScores(): Boolean =
    false

  override def getLeafCollector(lrc: LeafReaderContext): LeafCollector = {
    reader = lrc.reader
    this
  }

  override def collect(id: Int) =
    this += Metric fromDocument reader.document(id)
}
