package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, VertexWeight, WeightedEdge}
import SparkER.EntityClustering.EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.apache.spark.rdd.RDD

/*
  * Ricochet Sequential Rippling Clustering
  * */
object RicochetSRClustering extends EntityClusteringTrait {

  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    /** Generates the connected components */
    val cc = connectedComponents(edges.filter(_.weight > edgesThreshold))
    /** Then, in parallel, for each connected components computes the clusters */
    //todo:rimuovere il repartition(1)
    val res = cc.mapPartitions { partition =>

      /** ---- Used in the pre-processing step to generate the statistics  ---- */
      val edgesWeight = Array.fill[Double](maxProfileID + 1) {
        0.0
      }
      val edgesAttached = Array.fill[Int](maxProfileID + 1) {
        0
      }
      val connections = Array.fill[List[(Int, Double)]](maxProfileID + 1) {
        Nil
      }
      val elements = Array.ofDim[Int](maxProfileID + 1)
      var numElements = 0
      /* --------------------------------------------------------------------- */

      /* Generated clusters */
      val clusters = scala.collection.mutable.Map[Int, scala.collection.mutable.HashSet[Int]]()


      /** Similarity between a profile and its center */
      val simWithCenter = Array.fill[Double](maxProfileID + 1) {
        0
      }
      /** Center in which a profile is contained */
      val currentCenter = Array.fill[Int](maxProfileID + 1) {
        -1
      }

      /** True if a profile is a center */
      val isCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }

      /** True if a profile is not a center */
      val isNonCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }

      /** Used to reassign the centers */
      val centersToReassign = new scala.collection.mutable.HashSet[Int]


      /** Now for each partition */
      partition.foreach { cluster =>


        /** Pre-processing step, computes statistics for each profile */
        cluster.foreach { case (u1, v1a, w) =>
          val u = u1.toInt
          val v = v1a.toInt

          if (edgesWeight(u) == 0) {
            elements.update(numElements, u)
            numElements += 1
          }
          if (edgesWeight(v) == 0) {
            elements.update(numElements, v)
            numElements += 1
          }

          edgesWeight.update(u, edgesWeight(u) + w)
          edgesWeight.update(v, edgesWeight(v) + w)
          edgesAttached.update(u, edgesAttached(u) + 1)
          edgesAttached.update(v, edgesAttached(v) + 1)
          connections.update(u, (v, w) :: connections(u))
          connections.update(v, (u, w) :: connections(v))


        }

        val vv = for (i <- 0 until numElements) yield {
          val e = elements(i)
          VertexWeight(e, edgesWeight(e), edgesAttached(e), connections(e).toMap)
        }
        numElements = 0

        val sorted = vv.toList.sorted
        /** ------------------- END STATISTICS COMPUTATION ---------------------------- */

        //Deal with the heaviest vertex first
        val v1 = sorted.head.profileId

        //Sets v1 as center of itself
        currentCenter.update(v1, v1)
        //Creates the v1 cluster
        clusters.put(v1, new scala.collection.mutable.HashSet[Int])
        //Adds v1 to its cluster
        clusters(v1).add(v1)
        //Sets v1 as a center
        isCenter.update(v1, true)
        //Sets the similarity of v1 to itself to 1
        simWithCenter.update(v1, 1.0)

        /** Now adds each element connected to v1 to the v1 cluster */
        sorted.head.connections.foreach { case (v2, v1V2Sim) =>
          //Sets v2 as non center, since its contained in the v1 cluster
          isNonCenter.update(v2, true)
          //Sets v1 as center of v2
          currentCenter.update(v2, v1)
          //Sets the similarity between v2 and v1
          simWithCenter.update(v2, v1V2Sim)
          //Adds v2 the the v1 cluster
          clusters(v1).add(v2)
        }

        //Then continue with the other elements
        sorted.tail.foreach { p =>
          //Reads the profile id
          val v1 = p.profileId
          //Reads the connections with the other profiles
          val v1Connections = p.connections

          //Clear the elements to re-assign
          val toReassign = new scala.collection.mutable.HashSet[Int]
          centersToReassign.clear()

          //For each connection
          v1Connections.foreach { case (v2, v1V2Sim) =>
            //If v2 is not a center and the similarity between v2 and v2 is grater than the current similarity between
            //the current center, re-assign v2 to another cluster
            if (!isCenter(v2) && v1V2Sim > simWithCenter(v2)) {
              toReassign.add(v2)
            }
          }

          //If there are elements to re-assign
          if (toReassign.nonEmpty) {
            //If v1 is not a center
            if (isNonCenter(v1)) {
              //Make v1 a center
              isNonCenter.update(v1, false)
              //Reads the cluster in which v1 was contained
              val prevV1Center = currentCenter(v1)
              //Removes v1 from its previous cluster
              clusters(prevV1Center).remove(v1)
              //If the size of the cluster after removing v1 is less than 2, the cluster have to be removed
              if (clusters(prevV1Center).size < 2) {
                centersToReassign.add(prevV1Center)
              }
            }
            //Sets v1 to be re-assigned to another cluster
            toReassign.add(v1)
            //Makes v1 as a center
            clusters.put(v1, toReassign)
            //Set that v1 is now a center
            isCenter.update(v1, true)
          }

          toReassign.foreach { v2 =>
            //Continue only if v2 is different than v1
            if (v2 != v1) {
              //if v2 was in another cluster already then deal with that cluster
              if (isNonCenter(v2)) {
                //Reads the current cluster of v2
                val prevClusterCenter = currentCenter(v2)
                //Removes v2 from the current cluster
                clusters(prevClusterCenter).remove(v2)
                //If the size of the cluster after removing v2 is less than 2, the cluster have to be removed
                if (clusters(prevClusterCenter).size < 2) {
                  centersToReassign.add(prevClusterCenter)
                }
              }
              //Sets v2 as non center
              isNonCenter.update(v2, true)
              currentCenter.update(v2, v1)
              simWithCenter.update(v2, v1Connections(v2))
            }
          }

          centersToReassign.foreach { centerId =>
            if (clusters.contains(centerId))
              if (clusters(centerId).size < 2) {
                isCenter.update(centerId, false)
                clusters.remove(centerId)

                var max = 0.0
                var newCenter = v1

                clusters.keySet.foreach { center =>
                  val currentConnections = connections(center).toMap
                  val newSim = currentConnections.getOrElse(centerId, 0.0)
                  if (newSim > max) {
                    max = newSim
                    newCenter = center
                  }
                }

                clusters(newCenter).add(centerId)
                isNonCenter.update(centerId, true)
                currentCenter.update(centerId, newCenter)
                simWithCenter.update(centerId, max)
              }
          }
        }
      }

      clusters.map(x => (x._1, x._2.toSet)).toIterator
    }
    addUnclusteredProfiles(profiles, res)
  }
}
