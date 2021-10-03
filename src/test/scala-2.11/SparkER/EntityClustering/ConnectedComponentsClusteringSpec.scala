package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._

class ConnectedComponentsClusteringSpec extends WordSpec with Matchers with SharedSparkContext {
  "ConnectedComponentsClustering.getClusters" should {
    "return the clusters derived from Connected Components Clustering" in {
      val profiles = sc.makeRDD[Profile](
        for (id <- 0 to 9) yield {
          Profile(
            id,
            attributes = scala.collection.mutable.MutableList(),
            originalID = "",
            sourceId = 0
          )
        }
      )
      val edges = sc.makeRDD[WeightedEdge](
        Seq(
          WeightedEdge(0, 6, 0.8),
          WeightedEdge(1, 4, 0.9),
          WeightedEdge(2, 9, 0.7),
          WeightedEdge(2, 7, 0.6),
          WeightedEdge(4, 8, 0.6),
          WeightedEdge(7, 9, 0.9)
        )
      )
      val actual = ConnectedComponentsClustering.getClusters(
        profiles,
        edges,
        maxProfileID = 9,
        edgesThreshold = 0.5,
        separatorID = -1
      ).collect.toList
      val expected = List((0,Set(0, 6)), (1,Set(1, 4, 8)), (2,Set(2, 9, 7)), (3,Set(3)), (5,Set(5)))
      actual shouldBe expected
    }
  }
}
