package SparkER.BlockRefinementMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.DataStructures.{BlockClean, BlockAbstract, ProfileBlocks, BlockWithComparisonSize}
import SparkER.Utilities.Converters

class BlockFilteringSpec extends WordSpec with Matchers with SharedSparkContext {
  "BlockFiltering.blockFiltering" should {
    "retain no blocks" when {
      "r = 0.0" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0), Set(1)))
          )
        )
        val actual = BlockFiltering.blockFiltering(
          profilesWithBlocks = Converters.blocksToProfileBlocks(blocks),
          r = 0.0
        ).collect
        val expected = Seq(
          ProfileBlocks(profileID = 0, blocks = Set()),
          ProfileBlocks(profileID = 1, blocks = Set())
        )
        actual shouldBe expected
      }
    }
    "retain all of the blocks" when {
      "r = 1.0" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0), Set(1))),
            BlockClean(blockID = 1, profiles = Array(Set(0), Set(1,2))),
            BlockClean(blockID = 2, profiles = Array(Set(0), Set(1,2,3))),
            BlockClean(blockID = 3, profiles = Array(Set(0), Set(1,2,3,4))),
            BlockClean(blockID = 4, profiles = Array(Set(0), Set(1,2,3,4,5)))
          )
        )
        val actual = BlockFiltering.blockFiltering(
          profilesWithBlocks = Converters.blocksToProfileBlocks(blocks),
          r = 1.0
        ).collect
        val expected = Converters.blocksToProfileBlocks(blocks).collect
        actual should contain theSameElementsAs expected
      }
    }
    "retain the n * r blocks" when {
      "r = 0.8" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0), Set(1))),
            BlockClean(blockID = 1, profiles = Array(Set(0), Set(1,2))),
            BlockClean(blockID = 2, profiles = Array(Set(0), Set(1,2,3))),
            BlockClean(blockID = 3, profiles = Array(Set(0), Set(1,2,3,4))),
            BlockClean(blockID = 4, profiles = Array(Set(0), Set(1,2,3,4,5)))
          )
        )
        val actual = BlockFiltering.blockFiltering(
          profilesWithBlocks = Converters.blocksToProfileBlocks(blocks),
          r = 0.8
        ).collect

        val expected = Seq(
          ProfileBlocks(
            profileID = 0,
            // n*r = 5*0.8 = 4.0 -> 4
            blocks = Set(
              BlockWithComparisonSize(0,1.0),
              BlockWithComparisonSize(1,2.0),
              BlockWithComparisonSize(2,3.0),
              BlockWithComparisonSize(3,4.0)
            )
          ),
          ProfileBlocks(
            profileID = 1,
            // n*r = 5*0.8 = 4.0 -> 4
            blocks = Set(
              BlockWithComparisonSize(0,1.0),
              BlockWithComparisonSize(1,2.0),
              BlockWithComparisonSize(2,3.0),
              BlockWithComparisonSize(3,4.0)
            )
          ),
          ProfileBlocks(
            profileID = 2,
            // n*r = 4*0.8 = 3.2 -> 3
            blocks = Set(
              BlockWithComparisonSize(1,2.0),
              BlockWithComparisonSize(2,3.0),
              BlockWithComparisonSize(3,4.0)
            )
          ),
          ProfileBlocks(
            profileID = 3,
            // n*r = 3*0.8 = 2.4 -> 2
            blocks = Set(
              BlockWithComparisonSize(2,3.0),
              BlockWithComparisonSize(3,4.0)
            )
          ),
          ProfileBlocks(
            profileID = 4,
            // n*r = 2*0.8 = 1.6 -> 2
            blocks = Set(
              BlockWithComparisonSize(3,4.0),
              BlockWithComparisonSize(4,5.0)
            )
          ),
          ProfileBlocks(
            profileID = 5,
            // n*r = 1*0.8 = 0.8 -> 1
            blocks = Set(BlockWithComparisonSize(4,5.0))
          )
        )
        actual should contain theSameElementsAs expected
      }
    }
  }
  "BlockFiltering.blockFilteringAdvanced" should {
    "cause error" when {
      "the number of blocks to keep is zero" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0), Set(1)))
          )
        )
        intercept[org.apache.spark.SparkException] {
          BlockFiltering.blockFilteringAdvanced(
            profilesWithBlocks = Converters.blocksToProfileBlocks(blocks),
            r = 0.0
          ).collect
        }
      }
    }
    "retain all of the blocks" when {
      "r = 1.0" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0), Set(1))),
            BlockClean(blockID = 1, profiles = Array(Set(0), Set(1,2))),
            BlockClean(blockID = 2, profiles = Array(Set(0), Set(1,2,3))),
            BlockClean(blockID = 3, profiles = Array(Set(0), Set(1,2,3,4))),
            BlockClean(blockID = 4, profiles = Array(Set(0), Set(1,2,3,4,5)))
          )
        )
        val actual = BlockFiltering.blockFilteringAdvanced(
          profilesWithBlocks = Converters.blocksToProfileBlocks(blocks),
          r = 1.0
        ).collect
        val expected = Converters.blocksToProfileBlocks(blocks).collect
        actual should contain theSameElementsAs expected
      }
    }
    "retain the additional blocks" when {
      "their comparison sizes are less than or equal to the last retained one's size" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0), Set(1))),
            BlockClean(blockID = 1, profiles = Array(Set(0), Set(1))),
            BlockClean(blockID = 2, profiles = Array(Set(0), Set(1))),
            BlockClean(blockID = 3, profiles = Array(Set(0), Set(1))),
            BlockClean(blockID = 4, profiles = Array(Set(0), Set(1)))
          )
        )
        val actual = BlockFiltering.blockFilteringAdvanced(
          profilesWithBlocks = Converters.blocksToProfileBlocks(blocks),
          r = 0.1
        ).collect
        val expected = Seq(
          ProfileBlocks(
            profileID = 0,
            // n*r = 5*0.1 = 0.5 -> 1
            blocks = Set(
              BlockWithComparisonSize(0,1.0),
              BlockWithComparisonSize(1,1.0),
              BlockWithComparisonSize(2,1.0),
              BlockWithComparisonSize(3,1.0),
              BlockWithComparisonSize(4,1.0)
            )
          ),
          ProfileBlocks(
            profileID = 1,
            // n*r = 5*0.1 = 0.5 -> 1
            blocks = Set(
              BlockWithComparisonSize(0,1.0),
              BlockWithComparisonSize(1,1.0),
              BlockWithComparisonSize(2,1.0),
              BlockWithComparisonSize(3,1.0),
              BlockWithComparisonSize(4,1.0)
            )
          )
        )
        actual should contain theSameElementsAs expected
      }
    }
  }
}
