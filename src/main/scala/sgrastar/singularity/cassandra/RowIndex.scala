package com.criteo.sre.storage.sgrastar.singularity
package cassandra

import java.nio.ByteBuffer
import java.util.{Set => JSet}
import lucene.{MetricsIndex, MetricsIndexManager}
import org.apache.cassandra.config.ColumnDefinition
import org.apache.cassandra.cql3.CFDefinition
import org.apache.cassandra.db._
import org.apache.cassandra.db.index._
import org.apache.cassandra.dht.LongToken
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.lucene.analysis.Token
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class RowIndex
    extends PerRowSecondaryIndex {
  private val log = LoggerFactory.getLogger(classOf[RowIndex])

  private var columnDefinition: ColumnDefinition = null
  private var tableDefinition: CFDefinition = null

  private var keyspaceName = ""
  private var tableName = ""
  private var indexName = ""
  private var fullyQualifiedIndexName = ""

  private var luceneIndex: MetricsIndex = null
  private var cassandraSearcher: RowIndexSearcher = null

  private val murmur3 = new Murmur3Partitioner()

  override def getIndexCfs(): ColumnFamilyStore =
    // We're not using CFs in order to store index data
    null

  override def getIndexName(): String =
    indexName

  /** Called by C* after setting the columnDefs, but before setting the baseCfs
    */
  override def validateOptions() =
    assert(columnDefs != null && columnDefs.size() == 1)

  override def init() = synchronized {
    validateOptions()
    assert(baseCfs != null)

    log.info("Initializing index")

    columnDefinition = columnDefs.iterator.next
    tableDefinition = baseCfs.metadata.getCfDef

    keyspaceName = baseCfs.metadata.ksName
    tableName = baseCfs.name
    indexName = columnDefinition.getIndexName
    fullyQualifiedIndexName = keyspaceName + "." + tableName + "." + indexName

    luceneIndex = MetricsIndexManager.getOrCreateKeyspaceIndex(keyspaceName)
    cassandraSearcher = new RowIndexSearcher(baseCfs, luceneIndex, columnDefinition.name)

    log.info(
      "Index initialized with ks={}, cf={}, name={}",
      keyspaceName,
      tableName,
      indexName
    )
  }

  override def indexes(col: ByteBuffer): Boolean =
    // We will not be indexing any column
    false

  /** Called upon CF updates (insert/delete of rows).
    */
  override def index(key: ByteBuffer, cf: ColumnFamily) = {
    val metric = Metric fromCompositeKey key
    if (cf.isMarkedForDelete) {
      luceneIndex.delete(metric)
    } else {
      val token = murmur3.getToken(key).token
      luceneIndex.insert(metric, token)
    }
  }

  /** Called when dropping a whole row during cleanup.
    */
  override def delete(dk: DecoratedKey) = {
    val metric = Metric fromCompositeKey dk.key
    luceneIndex.delete(metric)
  }

  override def forceBlockingFlush() =
    luceneIndex.commit(true)

  override def getLiveSize(): Long =
    luceneIndex.size

  override protected def createSecondaryIndexSearcher(columns: JSet[ByteBuffer]): SecondaryIndexSearcher =
    cassandraSearcher

  /** Called upon index removal,
    * no-op because we share indices and this should not happen.
    */
  override def invalidate() =
    log.info("{} - Invalidate (no-op)", fullyQualifiedIndexName)

  /** Called upon index alteration (column addition/removal),
    * no-op because we share indices and this should not happen.
    */
  override def reload() =
    log.info("{} - Reload (no-op)", fullyQualifiedIndexName)

  /** Called upon removing a column index,
    * no-op because we share indices and this should not happen.
    */
  override def removeIndex(column: ByteBuffer) =
    log.info("{} - RemoveIndex (no-op)", fullyQualifiedIndexName)

  /** Called upon truncating a CF,
    * no-op because we share indices and this should not happen.
    */
  override def truncateBlocking(truncatedAt: Long) =
    log.info("{} - Truncate (no-op)", fullyQualifiedIndexName)
}
