package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

object EntityClusterUtils {


  def addUnclusteredProfiles(profiles: RDD[Profile], clusters: RDD[(Int, Set[Int])]): RDD[(Int, Set[Int])] = {
    val profilesIds = profiles.map(_.id)
    val clusteredProfiles = clusters.flatMap(_._2)
    val unclusteredProfiles = profilesIds.subtract(clusteredProfiles)

    val missingClusters = unclusteredProfiles.map(p => (p, Set(p)))

    clusters.union(missingClusters)
  }

  /**
    * Computes the connected components
    * First try to find the component using the Large/Small star algorithm, and if the
    * algorithm converge, find the edges of the component.
    *
    * If the algorithm doesn't converge, calculate the components using the GraphX library.
    **/
  def connectedComponents(weightedEdges: RDD[WeightedEdge]): RDD[Iterable[(Int, Int, Double)]] = {
    val ccNodes = ConnectedComponents.run(weightedEdges, 200)

    if (ccNodes._2) {
      val ccNodesBD = SparkContext.getOrCreate().broadcast(
        ConnectedComponents
          .run(weightedEdges, 100)._1
          .map(x => (x._2, Array(x._1)))
          .reduceByKey((a, b) => b ++ a)
          .map(x => x._2.filter(x._1 != _).map(Set(x._1, _)).reduce(_ ++ _))
          .zipWithUniqueId()
          .collect()
      )
        .value
        .map(x => x._1.map(node => Map(node -> x._2)).reduce(_ ++ _))
        .reduce(_ ++ _)

      weightedEdges
        .map(we => (ccNodesBD(we.firstProfileID), Array(we)))
        .reduceByKey(_ ++ _)
        .map(x => x._2.map(we => (we.firstProfileID, we.secondProfileID, we.weight)).toIterable)
    }
    else {
      val log = LogManager.getRootLogger
      log.info("[Clustering] Connected component algorithm does not converge, switch to GraphX Implementation!")

      val edgesG = weightedEdges.map(e =>
        Edge(e.firstProfileID, e.secondProfileID, e.weight)
      )
      val graph = Graph.fromEdges(edgesG, -1)
      val cc = graph.connectedComponents()
      cc.triplets.map(t => (t.dstAttr, t)).groupByKey().map { case (_, data) =>
        data.map { edge =>
          (edge.toTuple._1._1.asInstanceOf[Int], edge.toTuple._2._1.asInstanceOf[Int], edge.toTuple._3.asInstanceOf[Double])
        }
      }
    }

    /*val edgesG = weightedEdges.map(e =>
      Edge(e.firstProfileID, e.secondProfileID, e.weight)
    )
    val graph = Graph.fromEdges(edgesG, -1)
    val cc = graph.connectedComponents()
    val connectedComponents = cc.triplets.map(t => (t.dstAttr, t)).groupByKey().map { case (_, data) =>
      data.map { edge =>
        (edge.toTuple._1._1.asInstanceOf[Int], edge.toTuple._2._1.asInstanceOf[Int], edge.toTuple._3.asInstanceOf[Double])
      }
    }
    connectedComponents*/

  }


  /**
    * Given a cluster computes its precision and recall
    **/
  def calcPcPqCluster(clusters: RDD[(Int, Set[Int])], gtBroadcast: Broadcast[Set[(Int, Int)]], separatorID: Int = -1): (Double, Double) = {

    val numMatches = clusters.context.doubleAccumulator("numMatches")
    val numComparisons = clusters.context.doubleAccumulator("numComparisons")

    clusters.filter(_._2.size > 1).foreach { case (id, el) =>

      if (separatorID < 0) {
        for (i <- el) {
          for (j <- el; if i < j) {
            numComparisons.add(1)
            if (gtBroadcast.value.contains((i, j))) {
              numMatches.add(1)
            }
          }
        }
      }
      else {
        val x = el.partition(_ <= separatorID)
        for (i <- x._1) {
          for (j <- x._2) {
            numComparisons.add(1)
            if (gtBroadcast.value.contains((i, j))) {
              numMatches.add(1)
            }
          }
        }
      }
    }

    val recall = numMatches.value / gtBroadcast.value.size
    val precision = numMatches.value / numComparisons.value

    gtBroadcast.unpersist()
    (recall, precision)
  }

}
