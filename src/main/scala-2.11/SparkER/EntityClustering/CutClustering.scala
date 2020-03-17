package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.Utilities.GomoryHuTree
import org.apache.spark.rdd.RDD
import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultWeightedEdge, SimpleWeightedGraph}

object CutClustering extends EntityClusteringTrait {

  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    getClusters(profiles, edges, maxProfileID, edgesThreshold, separatorID, 0.3)
  }

  def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int, acap: Double): RDD[(Int, Set[Int])] = {

    val cc = EntityClusterUtils.connectedComponents(edges.filter(_.weight > edgesThreshold))

    val res = cc.mapPartitions { part =>
      part.map { connectedComponent =>

        val weightedGraph = new SimpleWeightedGraph[Long, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

        val sinkLabel = maxProfileID + 1

        weightedGraph.addVertex(sinkLabel)

        /** Connects all profiles in the connected component to the sinknode */
        val allProfiles = connectedComponent.flatMap(x => x._1 :: x._2 :: Nil).toSet

        allProfiles.foreach { p =>
          weightedGraph.addVertex(p)
          val e = weightedGraph.addEdge(sinkLabel, p)
          weightedGraph.setEdgeWeight(e, acap)
        }

        connectedComponent.foreach { case (u, v, w) =>
          val e = weightedGraph.addEdge(u, v)
          weightedGraph.setEdgeWeight(e, w)
        }

        val ght = new GomoryHuTree(weightedGraph)

        val duplicatesGraph = ght.MinCutTree()
        duplicatesGraph.removeVertex(maxProfileID.toInt + 1)
        val ci = new ConnectivityInspector(duplicatesGraph)

        val clusters = ci.connectedSets()

        var res: List[Set[Int]] = Nil

        val it = clusters.iterator()
        while (it.hasNext) {
          var s = Set.empty[Int]

          val c = it.next()
          val it2 = c.iterator()
          while (it2.hasNext) {
            val el = it2.next()
            s = s + el
          }

          res = s :: res

        }

        res.toIterator
      }
    }

    EntityClusterUtils.addUnclusteredProfiles(profiles, res.flatMap(x => x).zipWithIndex().map(x => (x._2.toInt, x._1)))
  }
}
