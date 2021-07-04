package SparkER.BlockBuildingMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.DataStructures.{KeyValue, Profile, BlockAbstract, BlockClean, KeysCluster}

class BlockingUtilsSpec extends WordSpec with Matchers with SharedSparkContext {
  "BlockingUtils.associateKeysToProfileID" should {
    "return a list of (token, entityID)" in {
      val profileKs = (0, Seq("apple", "banana", "cat"))
      val expected = Seq(("apple", 0), ("banana", 0), ("cat", 0))
      val actual = BlockingUtils.associateKeysToProfileID(profileKs)
      actual shouldBe expected
    }
  }
  "BlockingUtils.associateKeysToProfileIdEntropy" should {
    "return a list of (token, (profileID, token hashes))" in {
      val profileKs = (0, Seq("apple", "banana", "cat"))
      val expected = Seq(
        ("apple", (0, Vector(97, 112, 112, 108, 101))),
        ("banana", (0, Vector(98, 97, 110, 97, 110, 97))),
        ("cat", (0, Vector(99, 97, 116)))
      )
      val actual = BlockingUtils.associateKeysToProfileIdEntropy(profileKs)
      actual shouldBe expected
    }
  }
}
