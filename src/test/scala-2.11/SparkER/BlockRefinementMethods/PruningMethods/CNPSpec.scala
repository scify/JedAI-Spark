package SparkER.BlockRefinementMethods.PruningMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.scalatest._
import SparkER.BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import SparkER.DataStructures.{BlockDirty, BlockClean, BlockAbstract, UnweightedEdge}
import SparkER.Utilities.Converters

class CNPSpec extends WordSpec with Matchers with SharedSparkContext {
  def blocksToBlockIndex(blocks: RDD[BlockAbstract]): Broadcast[scala.collection.Map[Int, Array[Set[Int]]]] = {
    val m = blocks.map { case block => (block.blockID, block.profiles) }.collect.toMap
    blocks.sparkContext.broadcast(m)
  }

  "CNP.CNP" should {
    "retain the top-k neighboring nodes for each node vi" when {
      "the given task is Dirty ER" in {
        // A brief explanation of this algorithm is as follows:
        //
        // 1. Calc weights for each edge (i.e., profile pair).
        //    In this case, their weights are based on CBS (Common Blocks Scheme).
        //    Note that current CNP implementation takes into account self-reference edges.
        //
        // CBS(p0,p0)=2
        // CBS(p0,p1)=2
        // CBS(p0,p2)=2
        // CBS(p0,p3)=1
        // CBS(p0,p4)=1
        // CBS(p1,p1)=2
        // CBS(p1,p2)=2
        // CBS(p1,p3)=1
        // CBS(p1,p4)=1
        // CBS(P2,p2)=2
        // CBS(p2,p3)=1
        // CBS(p2,p4)=1
        // CBS(p3,p3)=1
        // CBS(p3,p4)=1
        // CBS(p4,p4)=1
        //
        // 2. Calc the cardinality threshold (i.e., k)
        //
        // k = floor((BCin * |E| / numberOfProfiles) - 1) = floor((8 / 4) - 1) = 1
        //
        // Where
        // |E|: The cardinality of the set of entity profiles
        // BCin: The avg number of block assignments per entity of the input collection
        // numbefOfProfiles: A parameter. Its optimal value is |E|.
        //
        // Note that, for the testing purpose, this test sets numberOfProfiles=|E|-1.
        //
        // 3. For each node vi, get its neighborhood, and put them into the sorted stack,
        //    then pop top-K elements from the stack.
        //    (This stack sorts the elements by their weights. It doesn't use profileID as a tie-breaker explicitly.)
        //
        // vi | top-K | pruned since they aren't top-K
        // p0 | p0    | p1, p2, p3, p4
        // p1 | ✓  p0 | p1, p2, p3, p4
        // p2 | ✓  p0 | p1, p2, p3, p4
        // p3 | ✓  p0 | p1, p2, p3, p4
        // p4 | ✓  p0 | p1, p2, p3, p4
        //
        // Note that unlike step.1, current CNP implementation discards the p0-p0 pair since this one is a self-reference edge.
        //
        // For mode detail, See 3.3.4:
        // http://disi.unitn.it/~themis/publications/tkde13-metablocking.pdf

        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockDirty(blockID = 0, profiles = Array(Set(0, 1, 2, 3, 4))),
            BlockDirty(blockID = 1, profiles = Array(Set(0, 1, 2)))
          )
        )
        val actual = CNP.CNP(
          blocks,
          numberOfProfiles = 4,
          profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
          blockIndex = blocksToBlockIndex(blocks),
          maxID = blocks.flatMap(_.profiles.flatMap(identity)).max,
          separatorID = Array.empty,
          groundtruth = sc.broadcast(Set.empty[(Int,Int)]),
          weightType = WeightTypes.CBS
        ).collect

        val expected = Seq(
          (4.0, 0.0, List(UnweightedEdge(0, 4), UnweightedEdge(0, 3), UnweightedEdge(0, 2), UnweightedEdge(0, 1))),
          (0.0, 0.0, List()), (0.0, 0.0, List()), (0.0, 0.0, List()), (0.0, 0.0, List())
        )
        actual should contain theSameElementsAs expected
      }

      "the given task is Clean-Clean ER" in {
        // A brief explanation of this algorithm is as follows:
        //
        // 1. Calc weights for each edge (i.e., profile pair).
        //    In this case, their weights are based on CBS (Common Blocks Scheme).
        //
        // CBS(p0,p2)=2
        // CBS(p0,p3)=1
        // CBS(p1,p2)=2
        // CBS(p1,p3)=1
        //
        // 2. Calc the cardinality threshold (i.e., k)
        //
        // k = floor((BCin * |E| / numberOfProfiles) - 1) = floor((7 / 3) - 1) = 1
        //
        // Where
        // |E|: The cardinality of the set of entity profiles
        // BCin: The avg number of block assignments per entity of the input collection
        // numbefOfProfiles: A parameter. Its optimal value is |E|.
        //
        // Note that, for the testing purpose, this test sets numberOfProfiles=|E|-1.
        //
        // 3. For each node vi, get its neighborhood, and put them into the sorted stack,
        //    then pop top-K elements from the stack.
        //    (This stack sorts the elements by their weights. It doesn't use profileID as a tie-breaker explicitly.)
        //
        // vi | top-K | pruned since they aren't top-K
        // p0 | ✓  p2 | p3
        // p1 | ✓  p2 | p3
        // p2 | ✓  p0 | p1
        // p3 | ✓  p0 | p1
        //
        // For mode detail, See 3.3.4:
        // http://disi.unitn.it/~themis/publications/tkde13-metablocking.pdf

        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(0, 1), Set(2)))
          )
        )
        val actual = CNP.CNP(
          blocks,
          numberOfProfiles = 3,
          profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
          blockIndex = blocksToBlockIndex(blocks),
          maxID = blocks.flatMap(_.profiles.flatMap(identity)).max,
          separatorID = Array(1),
          groundtruth = sc.broadcast(Set.empty[(Int,Int)]),
          weightType = WeightTypes.CBS
        ).collect

        val expected = Seq(
          (2.0, 0.0,
            List(UnweightedEdge(0, 3), UnweightedEdge(0, 2))),
          (1.0, 0.0,
            List(UnweightedEdge(1, 2))),
          (0.0, 0.0, List()),
          (0.0, 0.0, List())
        )
        actual should contain theSameElementsAs expected
      }
    }
  }
}
