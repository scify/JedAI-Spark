package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._

class CenterClusteringSpec extends WordSpec with Matchers with SharedSparkContext {
//  "CenterClustering.getClusters" should {
//    "return the clusters derived from Center Clustering" in {
//      // NOTE:
//      // This example is from the book "The Four Generations of Entity Resolution." [0]. See chapter 3.4 for more detail.
//      //
//      // [0] Papadakis, George, et al. "The Four Generations of Entity Resolution." Synthesis Lectures on Data Management 16.2 (2021): 1-170.
//      val profiles = sc.makeRDD[Profile](
//        for (id <- 0 to 9) yield {
//          Profile(
//            id,
//            attributes = scala.collection.mutable.MutableList(),
//            originalID = "",
//            sourceId = 0
//          )
//        }
//      )
//      val edges = sc.makeRDD[WeightedEdge](
//        Seq(
//          WeightedEdge(0, 6, 0.8),
//          WeightedEdge(1, 4, 0.9),
//          WeightedEdge(2, 9, 0.7),
//          WeightedEdge(2, 7, 0.6),
//          WeightedEdge(4, 8, 0.6),
//          WeightedEdge(7, 9, 0.9)
//        )
//      )
//
//      val actual = CenterClustering.getClusters(
//        profiles,
//        edges,
//        maxProfileID = 9,
//        edgesThreshold = 0.5,
//        separatorID = -1
//      ).collect.toList
//      val expected = List((6,Set(0, 6)), (1,Set(1, 4)), (9,Set(7, 9, 2)), (3,Set(3)), (5,Set(5)), (8,Set(8)))
//      actual shouldBe expected
//    }
//  }
}
