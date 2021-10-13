package SparkER.BlockRefinementMethods.PruningMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.DataStructures.{BlockDirty, BlockClean, BlockAbstract, UnweightedEdge, ProfileBlocks}
import SparkER.Utilities.Converters

class PruningUtilsSpec extends WordSpec with Matchers with SharedSparkContext {
    def blocksToBlockIndex(blocks: RDD[BlockAbstract]): Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]] = {
    val m = blocks.map { case block => (block.blockID.toLong, (block.profiles(0).map(_.toLong).toSet, block.profiles(1).map(_.toLong).toSet)) }.collect.toMap
    blocks.sparkContext.broadcast(m)
  }

  "PruningUtils.getAllNeighbors" should {
    "get all neighbors" in  {
      val actual = PruningUtils.getAllNeighbors(
        profileId = 0,
        block = Array(Set(0, 1, 2), Set(3, 4, 5)),
        separators = Array(2)
      )
      val expected = Set(3, 4, 5)
      actual shouldBe expected
    }
  }
  "PruningUtils.CalcPCPQ" should {
    "return (true-positive + false-positive count, true-positive edges) tuples" in {
      val blocks: RDD[BlockAbstract] = sc.makeRDD(
        Seq(
          BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
          BlockClean(blockID = 1, profiles = Array(Set(0, 1), Set(2)))
        )
      )

      val actual = PruningUtils.CalcPCPQ(
        profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
        blockIndex = blocksToBlockIndex(blocks),
        maxID = 3,
        separatorID = 1,
        groundtruth = sc.broadcast(Set((0,2),(1,3)))
      ).collect
      val expected = List(
        (2.0,List(UnweightedEdge(0,2))),
        (2.0,List(UnweightedEdge(1,3))),
        (0.0,List(UnweightedEdge(0,2))),
        (0.0,List(UnweightedEdge(1,3)))
      )
      actual shouldBe expected
    }
  }
}
