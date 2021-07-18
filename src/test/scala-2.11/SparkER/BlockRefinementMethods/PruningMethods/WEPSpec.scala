package SparkER.BlockRefinementMethods.PruningMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.scalatest._
import SparkER.BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import SparkER.DataStructures.{BlockDirty, BlockClean, BlockAbstract, UnweightedEdge}
import SparkER.Utilities.Converters

class WEPSpec extends WordSpec with Matchers with SharedSparkContext {
  def blocksToBlockIndex(blocks: RDD[BlockAbstract]): Broadcast[scala.collection.Map[Int, Array[Set[Int]]]] = {
    val m = blocks.map { case block => (block.blockID, block.profiles) }.collect.toMap
    blocks.sparkContext.broadcast(m)
  }

  "WEP.WEP" should {
    "discard every edge with weight lower than w_min" when {
      "the given task is Dirty ER" in {
        // A brief explanation of this algorithm is as follows:
        //
        // 1. Calc edge weights. In this case, they are based on CBS (Common Blocks Scheme).
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
        // 2. Calc the threshold w_min (i.e., The global weight pruning criterion).
        //    Where w_min is the average edge weight.
        //
        // w_min = (2*3 + 1*7) / 10 = 1.3
        //
        // 3. Prune edges that are lower than w_min.
        //
        // o CBS(p0,p1)=2
        // o CBS(p0,p2)=2
        // o CBS(p1,p2)=2 >= w_min
        // x CBS(p0,p3)=1 < w_min
        // x CBS(p0,p4)=1
        // x CBS(p1,p3)=1
        // x CBS(p1,p4)=1
        // x CBS(p2,p3)=1
        // x CBS(p2,p4)=1
        // x CBS(p3,p4)=1
        //
        // For more detail, See 3.3.1:
        // http://disi.unitn.it/~themis/publications/tkde13-metablocking.pdf

        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockDirty(blockID = 0, profiles = Array(Set(0, 1, 2, 3, 4))),
            BlockDirty(blockID = 1, profiles = Array(Set(0, 1, 2)))
          )
        )
        val actual = WEP.WEP(
          profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
          blockIndex = blocksToBlockIndex(blocks),
          maxID = blocks.flatMap(_.profiles.flatMap(identity)).max,
          separatorIDs = Array.empty,
          groundtruth = sc.broadcast(Set.empty[(Int,Int)]),
          weightType = WeightTypes.CBS
        ).collect

        val expected = Seq(
          (2.0, 0.0, List(UnweightedEdge(0, 2), UnweightedEdge(0, 1))),
          (1.0, 0.0, List(UnweightedEdge(1, 2))),
          (0.0, 0.0, List()), (0.0, 0.0, List()), (0.0, 0.0, List())
        )
        actual should contain theSameElementsAs expected
      }
      "the given task is Clean-Clean ER" in {
        // A brief explanation of this algorithm is as follows:
        //
        // 1. Calc edge weights. In this case, they are based on CBS (Common Blocks Scheme).
        //
        // CBS(p0,p2)=2
        // CBS(p0,p3)=1
        // CBS(p1,p2)=2
        // CBS(p1,p3)=1
        //
        // 2. Calc the threshold w_min (i.e., The global weight pruning criterion).
        //    Where w_min is the average edge weight.
        //
        // w_min = (2*2 + 1*2) / 4 = 1.5
        //
        // 3. Prune edges that are lower than w_min.
        //
        // o CBS(p0,p2)=2 >= w_min
        // x CBS(p0,p3)=1 < w_min
        // o CBS(p1,p2)=2 >= w_min
        // x CBS(p1,p3)=1 < w_min
        //
        // For more detail, See 3.3.1:
        // http://disi.unitn.it/~themis/publications/tkde13-metablocking.pdf

        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(0, 1), Set(2)))
          )
        )
        val actual = WEP.WEP(
          profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
          blockIndex = blocksToBlockIndex(blocks),
          maxID = blocks.flatMap(_.profiles.flatMap(identity)).max,
          separatorIDs = Array(1),
          groundtruth = sc.broadcast(Set.empty[(Int,Int)]),
          weightType = WeightTypes.CBS
        ).collect

        val expected = Seq(
          (1.0, 0.0, List(UnweightedEdge(0, 2))),
          (1.0, 0.0, List(UnweightedEdge(1, 2))),
          (0.0, 0.0, List()), (0.0, 0.0, List())
        )
        actual should contain theSameElementsAs expected
      }
    }
  }
}
