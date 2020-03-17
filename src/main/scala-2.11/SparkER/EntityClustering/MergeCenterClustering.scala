package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.apache.spark.rdd.RDD

object MergeCenterClustering extends EntityClusteringTrait {

  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    /** Generates the connected components */
    val cc = connectedComponents(edges.filter(_.weight > edgesThreshold))
    /** Then, in parallel, for each connected components computes the clusters */
    val res = cc.repartition(1).mapPartitions { partition =>
      /* Contains the ID of the center to which a profile is assigned */
      val currentAssignedCenter = Array.fill[Int](maxProfileID + 1) {
        -1
      }

      /* Check if a profile is not a center */
      val isNonCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }

      /* Check if a profile is a center */
      val isCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }

      /* Generated clusters */
      val clusters = scala.collection.mutable.Map[Int, Set[Int]]()

      /**
      * Merges two clusters.
      * @param cToKeep  id of the cluster to cluster to keep
      * @param cToMerge id of the cluster to merge
      * */
      def mergeClusters(cToKeep: Int, cToMerge: Int): Unit = {
        if (cToKeep > 0 && cToMerge > 0 && cToKeep != cToMerge) {
          //println("FONDO IL CLUSTER "+cToKeep+" con il cluster "+cToMerge)
          val cData = clusters.getOrElse(cToMerge, Set())
          clusters.update(cToKeep, clusters(cToKeep) ++ cData)
          cData.foreach(el => currentAssignedCenter.update(el.toInt, cToKeep))
          clusters.remove(cToMerge)
        }
      }

      /** Foreach connected component */
      partition.foreach { cluster =>
        /* Sorts the elements in the cluster descending by their similarity score */
        val sorted = cluster.toList.sortBy(x => (-x._3, x._1))

        /* Foreach element in the format (u, v, similarity) */
        sorted.foreach { case (u, v, _) =>

          //println("-------------------------------------------------------------------------------")

          //println("Prendo la coppia: u="+u+", v="+v)

          val uIsCenter = isCenter(u.toInt)
          val vIsCenter = isCenter(v.toInt)
          val uIsNonCenter = isNonCenter(u.toInt)
          val vIsNonCenter = isNonCenter(v.toInt)


          //println("U è un centro: "+uIsCenter)
          //println("V è un centro: "+vIsCenter)
          //println("U non è un centro: "+uIsNonCenter)
          //println("V non è un centro: "+vIsNonCenter)


          if (!(uIsCenter || vIsCenter || uIsNonCenter || vIsNonCenter)) {
            //println("Imposto u come nuovo centro, creo il cluster "+u)
            clusters.put(u, Set(u, v))
            currentAssignedCenter.update(u.toInt, u)
            currentAssignedCenter.update(v.toInt, u)

            isCenter.update(u.toInt, true)
            isNonCenter.update(v.toInt, true)
          }
          else if ((uIsCenter && vIsCenter) || (uIsNonCenter && vIsNonCenter)) {}
          else if (uIsCenter) {
            val currentUassignedCluster = currentAssignedCenter(u.toInt)
            //println("U è un centro, attualmente è contenuto nel cluster "+currentUassignedCluster)
            //println("Aggiungo v a quel cluster")
            clusters.put(currentUassignedCluster, clusters(currentUassignedCluster) + v)
            mergeClusters(currentUassignedCluster, currentAssignedCenter(v.toInt))
            currentAssignedCenter.update(v.toInt, u)
            isNonCenter.update(v.toInt, true)
          }
          else if (vIsCenter) {
            val currentVassignedCluster = currentAssignedCenter(v.toInt)
            //println("V è un centro, attualmente è contenuto nel cluster "+currentVassignedCluster)
            //println("Aggiungo v a quel cluster")
            clusters.put(currentVassignedCluster, clusters(currentVassignedCluster) + u)

            mergeClusters(currentVassignedCluster, currentAssignedCenter(u.toInt))
            currentAssignedCenter.update(u.toInt, v)
            isNonCenter.update(u.toInt, true)
          }

          //println("CLUSTER ATTUALI")
        }


      }

      clusters.toIterator
    }

    addUnclusteredProfiles(profiles, res)
  }
}
