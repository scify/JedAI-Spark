package SparkER.BlockRefinementMethods.PruningMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.scalatest._
import SparkER.BlockRefinementMethods.PruningMethods.PruningUtils.{WeightTypes, ComparisonTypes, ThresholdTypes}
import SparkER.DataStructures.{BlockDirty, BlockClean, BlockAbstract, UnweightedEdge}
import SparkER.Utilities.Converters

class WNPSpec extends WordSpec with Matchers with SharedSparkContext {
  def blocksToBlockIndex(blocks: RDD[BlockAbstract]): Broadcast[scala.collection.Map[Int, Array[Set[Int]]]] = {
    val m = blocks.map { case block => (block.blockID, block.profiles) }.collect.toMap
    blocks.sparkContext.broadcast(m)
  }

  "WNP.WNP" should {
    "retain every adjacent edge with weight higher than threshold" when {
      "the given task is Dirty ER" in {
        // A brief explanation of this algorithm is as follows:
        //
        // 1. Calc edge weights. In this case, they are based on CBS (Common Blocks Scheme).
        // Note that current WNP implementation takes into account self-reference edges.
        //
        // CBS(p0,p0)=2
        // CBS(p0,p1)=2
        // CBS(p1,p1)=3
        // CBS(p1,p2)=1
        // CBS(p2,p2)=3
        // CBS(p2,p3)=2
        // CBS(p3,p3)=2
        //
        // 2. Calc the threshold t_v for each profile where t_v is average weight of the adjacent edges.
        //
        // t_v(p0->*) = (CBS(p0,p0) + CBS(p0,p1)) / 2              = (2+2)/2   = 2.0
        // t_v(p1->*) = (CBS(p1,p1) + CBS(p0,p1) + CBS(p1,p2)) / 3 = (3+2+1)/3 = 2.0
        // t_v(p2->*) = (CBS(p2,p2) + CBS(p1,p2) + CBS(p2,p3)) / 3 = (3+1+2)/3 = 2.0
        // t_v(p3->*) = (CBS(p3,p3) + CBS(p2,p3)) / 2              = (2+2)/2   = 2.0
        //
        // 3. Prune edges by the vertex-centric algorithm.
        //    Each edge has two thresholds: one for ingoing direction and one for outgoing direction.
        //    When comparisonType is set to ComparisonTypes.OR, WNP should retain given edge if its weight is equal or higher than either of the two thresholds.
        //    Note that unlike step.1, current WNP implementation discards self-reference edges when pruning.
        //
        // ✓ CBS(p0,p1) = 2 (2 >= t_v(p0->*) OR 2 >= t_v(p1->*)) is true
        //   CBS(p1,p2) = 1 (1 >= t_v(p1->*) OR 1 >= t_v(p2->*)) is false
        // ✓ CBS(p2,p3) = 2 (2 >= t_v(p2->*) OR 2 >= t_v(p3->*)) is true
        //
        // For more detail, See 3.3.3:
        // http://disi.unitn.it/~themis/publications/tkde13-metablocking.pdf

        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockDirty(blockID = 0, profiles = Array(Set(0, 1))),
            BlockDirty(blockID = 1, profiles = Array(Set(0, 1))),
            BlockDirty(blockID = 2, profiles = Array(Set(1, 2))),
            BlockDirty(blockID = 3, profiles = Array(Set(2, 3))),
            BlockDirty(blockID = 4, profiles = Array(Set(2, 3)))
          )
        )
        val actual = WNP.WNP(
          profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
          blockIndex = blocksToBlockIndex(blocks),
          maxID = blocks.flatMap(_.profiles.flatMap(identity)).max,
          separatorIDs = Array.empty,
          groundtruth = sc.broadcast(Set.empty[(Int,Int)]),
          thresholdType = PruningUtils.ThresholdTypes.AVG,
          weightType = WeightTypes.CBS,
          comparisonType = PruningUtils.ComparisonTypes.OR
        ).collect

        val expected = Seq(
          (1.0,0.0,List(UnweightedEdge(0,1))),
          (0.0,0.0,List()),
          (1.0,0.0,List(UnweightedEdge(2,3))),
          (0.0,0.0,List())
        )
        actual should contain theSameElementsAs expected
      }
      "the given task is Clean-Clean ER" in {
        // A brief explanation of this algorithm is as follows:
        //
        // 1. Calc edge weights. In this case, they are based on CBS (Common Blocks Scheme).
        //
        // CBS(p0,p2)=1
        // CBS(p0,p3)=2
        // CBS(p1,p2)=2
        //
        // 2. Calc the threshold t_v for each profile where t_v is average weight of the adjacent edges.
        //
        // t_v(p0->*) = (CBS(p0,p2) + CBS(p0,p3)) / 2  = (1+2)/2 = 1.5
        // t_v(p1->*) = CBS(p1,p2) / 1                 = 2/1     = 2.0
        // t_v(p2->*) = (CBS(p0,p2) + CBS(p1,p2)) / 2  = (1+2)/2 = 1.5
        // t_v(p3->*) = CBS(p0,p3) / 1                 = 2/1     = 2.0
        //
        // 3. Prune edges by the vertex-centric algorithm.
        //    Each edge has two thresholds: one for ingoing direction and one for outgoing direction.
        //    When comparisonType is set to ComparisonTypes.OR, WNP should retain given edge if its weight is equal or higher than either of the two thresholds.
        //
        //   CBS(p0,p2) = 1  (1 >= t_v(p0->*) OR 1 >= t_v(p2->*)) is false
        // ✓ CBS(p0,p3) = 2  (2 >= t_v(p0->*) OR 2 >= t_v(p3->*)) is true
        // ✓ CBS(p1,p2) = 2  (2 >= t_v(p1->*) OR 2 >= t_v(p2->*)) is true
        //
        // For more detail, See 3.3.3:
        // http://disi.unitn.it/~themis/publications/tkde13-metablocking.pdf

        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0), Set(3))),
            BlockClean(blockID = 1, profiles = Array(Set(0), Set(3))),
            BlockClean(blockID = 2, profiles = Array(Set(0), Set(2))),
            BlockClean(blockID = 3, profiles = Array(Set(1), Set(2))),
            BlockClean(blockID = 4, profiles = Array(Set(1), Set(2)))
          )
        )
        val actual = WNP.WNP(
          profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
          blockIndex = blocksToBlockIndex(blocks),
          maxID = blocks.flatMap(_.profiles.flatMap(identity)).max,
          separatorIDs = Array(1),
          groundtruth = sc.broadcast(Set.empty[(Int,Int)]),
          weightType = WeightTypes.CBS
        ).collect

        val expected = Seq(
          (1.0,0.0,List(UnweightedEdge(0,3))),
          (1.0,0.0,List(UnweightedEdge(1,2))),
          (0.0,0.0,List()),
          (0.0,0.0,List())
        )
        actual should contain theSameElementsAs expected
      }
    }
  }
}
