package SparkER.BlockRefinementMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.DataStructures.{BlockClean, BlockAbstract}

class BlockPurgingSpec extends WordSpec with Matchers with SharedSparkContext {
  "BlockPurging.blockPurging" should {
    "purge blocks above the threshold comparison level" in {
      // A brief explanation of this algorithm is as follows:
      //
      // 1. Prepare the stats:
      // CL=1:  CC=1            BC=2
      // CL=2:  CC=1+2*1+2*1=5  BC=2+3+3=8
      // CL=3:  CC=5+3*1+3*1=11 BC=8+4+4=16
      // CL=16: CC=11+4*4=27    BC=16+8=24
      //
      // Where
      // CL: Comparison Level
      // CC: Comparison Cardinality
      // BC: Block Cardinality
      //
      // 2. Compare BC/CC between current and previous CLs.
      //    If current BC/CC is less than the previous one, return the size (i.e., previous CL):
      // size=0  24/27 > -INF (it's -INF since previous BC/CC is unknown)
      // size=16 16/11 > 24/27
      // size=3  8/5   > 16/11
      // size=2  1/2   < 8/5   -> return size=2
      //
      // For more detail, see 5.2.1:
      // http://helios.mi.parisdescartes.fr/~themisp/publications/tkde12-blockingframework.pdf

      val myblocks: RDD[BlockAbstract] = sc.makeRDD(
        Seq(
          BlockClean(blockID = 0, profiles = Array(Set(0), Set(4))),
          BlockClean(blockID = 1, profiles = Array(Set(0,1), Set(4))),
          BlockClean(blockID = 2, profiles = Array(Set(0,1), Set(4))),
          BlockClean(blockID = 3, profiles = Array(Set(0,1,2), Set(4))),
          BlockClean(blockID = 4, profiles = Array(Set(0,1,2), Set(4))),
          BlockClean(blockID = 5, profiles = Array(Set(0,1,2,3), Set(4,5,6,7)))
        )
      )
      val actual = BlockPurging.blockPurging(blocks = myblocks, smoothFactor = 1.0).collect
      val expected = Seq(
        BlockClean(blockID = 0, profiles = Array(Set(0), Set(4))),
        BlockClean(blockID = 1, profiles = Array(Set(0,1), Set(4))),
        BlockClean(blockID = 2, profiles = Array(Set(0,1), Set(4)))
      )
      actual.map(_.blockID) should contain theSameElementsAs expected.map(_.blockID)
      actual.map(_.profiles) should contain theSameElementsAs expected.map(_.profiles)
    }
  }
  "BlockPurging.sumPrecedentLevels" should {
    "sum precedent comparison level's BC and CC, respectively" in {
      val actual = BlockPurging.sumPrecedentLevels(
        input = Array(
          (1.0, (1.0, 2.0)),
          (2.0, (4.0, 6.0))
        )
      )
      val expected = Seq(
        (1.0, (1.0, 2.0)),
        (2.0, (5.0, 8.0))
      )
      actual shouldBe expected
    }
  }

  "BlockPurging.calcMaxComparisonNumber" should {
    "return the max comparison number" in {
      val actual = BlockPurging.calcMaxComparisonNumber(
        input = Array(
          (3.0, (8.0, 5.0)),
          (2.0, (1.0, 2.0))
        ),
        smoothFactor = 1.0
      )
      val expected = 2.0
      actual shouldBe expected
    }
  }
}
