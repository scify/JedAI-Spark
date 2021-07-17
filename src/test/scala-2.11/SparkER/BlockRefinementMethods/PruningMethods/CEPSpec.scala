package SparkER.BlockRefinementMethods.PruningMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.scalatest._
import SparkER.BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import SparkER.DataStructures.{BlockDirty, BlockAbstract, UnweightedEdge}
import SparkER.Utilities.Converters

class CEPSpec extends WordSpec with Matchers with SharedSparkContext {
  def blocksToBlockIndex(blocks: RDD[BlockAbstract]): Broadcast[scala.collection.Map[Int, Array[Set[Int]]]] = {
    val m = blocks.map { case block => (block.blockID, block.profiles) }.collect.toMap
    blocks.sparkContext.broadcast(m)
  }
  "CEP.CEP" should {
    "retain the K edges with the maximum weight" in {
      // A brief explanation of this algorithm is as follows:
      //
      // 1. Sort edges (i.e., profile pairs) by their weights.
      //    In this case, their weights are based on CBS (Common Blocks Scheme).
      //    (Note: current implimentation doesn't explicitly use profileID as a tie breaker):
      //
      // CBS(p0,p1)=2
      // CBS(p0,p2)=2
      // CBS(p1,p2)=2
      // CBS(p0,p3)=1
      // CBS(p0,p4)=1
      // CBS(p1,p3)=1
      // CBS(p1,p4)=1
      // CBS(p2,p3)=1
      // CBS(p2,p4)=1
      // CBS(p3,p4)=1
      //
      // 2. Calc the number of edges to keep (i.e., The global cardinality pruning criterion K):
      //
      //    K = BCin * |E|/2 = floor((5 + 3)/2) = 4
      //
      // Where
      // |E|: The cardinality of the set of entity profiles
      // BCin: The avg number of block assignments per entity of the input collection
      //
      // 3. Prune edges that are low-ranked at step.1:
      //
      // CBS(p0,p1)=2
      // CBS(p0,p2)=2
      // CBS(p1,p2)=2
      // CBS(p0,p3)=1
      // ^
      // |
      // retain the top four edges
      // ------------
      // prune the low-ranked edges
      // |
      // v
      // CBS(p0,p4)=1
      // CBS(p1,p3)=1
      // CBS(p1,p4)=1
      // CBS(p2,p3)=1
      // CBS(p2,p4)=1
      // CBS(p3,p4)=1
      //
      // For more detail, See 3.3.2:
      // http://disi.unitn.it/~themis/publications/tkde13-metablocking.pdf

      val blocks: RDD[BlockAbstract] = sc.makeRDD(
        Seq(
          BlockDirty(blockID = 0, profiles = Array(Set(0, 1, 2, 3, 4))),
          BlockDirty(blockID = 1, profiles = Array(Set(0, 1, 2)))
        )
      )
      val actual = CEP.CEP(
        profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
        blockIndex = blocksToBlockIndex(blocks),
        maxID = blocks.flatMap(_.profiles.flatMap(identity)).max,
        separatorIDs = Array.empty,
        groundtruth = sc.broadcast(Set.empty[(Int,Int)]),
        weightType = WeightTypes.CBS
      ).collect

      val expected = Seq(
        (3.0, 0.0,
          List(UnweightedEdge(0, 3), UnweightedEdge(0, 2), UnweightedEdge(0, 1))),
        (1.0, 0.0,
          List(UnweightedEdge(1, 2))),
        (0.0, 0.0, List()),
        (0.0, 0.0, List()),
        (0.0, 0.0, List())
      )
      actual should contain theSameElementsAs expected
    }
  }
}
