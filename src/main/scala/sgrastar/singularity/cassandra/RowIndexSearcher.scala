package com.criteo.sre.storage.sgrastar.singularity
package cassandra

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{List => JList, Set => JSet}
import lucene.MetricsIndex
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter._
import org.apache.cassandra.db.index._
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.dht.LongToken
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.thrift._
import org.apache.cassandra.utils.ByteBufferUtil
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class RowIndexSearcher(cfs: ColumnFamilyStore, index: MetricsIndex, column: ByteBuffer)
    extends SecondaryIndexSearcher(cfs.indexManager, Set[ByteBuffer]()) {
  private val log = LoggerFactory.getLogger(classOf[RowIndexSearcher])
  private val keyValidator = cfs.metadata.getKeyValidator.asInstanceOf[CompositeType]

  override def isIndexing(clause: JList[IndexExpression]): Boolean =
    getMyPredicate(clause).isDefined

  override def search(filter: ExtendedFilter): JList[Row] = {
    getMyPredicate(filter.getClause) match {
      case Some(pred) => {
        val pattern = new String(pred.getValue, StandardCharsets.UTF_8)
        val from = filter.dataRange.startKey.getToken.asInstanceOf[LongToken].token
        val to = filter.dataRange.stopKey.getToken.asInstanceOf[LongToken].token

        val metrics = index.search(pattern, from, to)
        metrics map { getRow(filter, _) }
      }

      case None => {
        List.empty[Row]
      }
    }
  }

  private def getMyPredicate(clause: JList[IndexExpression]): Option[IndexExpression] = {
    for (pred <- clause) {
      if (pred.column_name equals column) {
        if (pred.getOp == IndexOperator.EQ)
          return Some(pred)
        else
          log.warn("Ignoring query using non-EQ operator")
      }
    }

    None
  }

  private def getRow(filter: ExtendedFilter, metric: Metric): Row = {
    val rowKey = metric toCompositeKey keyValidator
    val decoratedKey = StorageService.getPartitioner decorateKey rowKey
    val atomFilter = filter.dataRange columnFilter ByteBufferUtil.EMPTY_BYTE_BUFFER
    val queryFilter =  new QueryFilter(
      decoratedKey,
      cfs.name,
      atomFilter,
      filter.timestamp
    )

    cfs.keyspace getRow queryFilter
  }
}
