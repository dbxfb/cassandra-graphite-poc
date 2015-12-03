package com.criteo.sre.storage.sgrastar.singularity
package lucene

import java.nio.file.Paths
import org.apache.cassandra.config.DatabaseDescriptor
import org.slf4j.LoggerFactory
import scala.collection.mutable.HashMap

object MetricsIndexManager {
  private val log = LoggerFactory.getLogger(getClass)

  private val mainDataDirectory = DatabaseDescriptor.loadConfig().data_file_directories(0)
  private val indices = HashMap[String, MetricsIndex]()

  /** Gets or create a keyspace-specific metrics index.
    *
    * This method is thread-safe, and is not performance-critical.
    */
  def getOrCreateKeyspaceIndex(keyspace: String) = synchronized {
    indices.get(keyspace) match {
      case Some(index) => {
        index
      }

      case None => {
        val indexPath = Option(Paths.get(mainDataDirectory, "sgrastar-idx", keyspace))
        val index = new MetricsIndex(keyspace, indexPath, 100000)
        indices.put(keyspace, index)

        index
      }
    }
  }
}
