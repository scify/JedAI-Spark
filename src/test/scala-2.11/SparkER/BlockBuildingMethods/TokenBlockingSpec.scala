package SparkER.BlockBuildingMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.DataStructures.{KeyValue, Profile, BlockAbstract, BlockClean}

class TokenBlockingSpec extends WordSpec with Matchers with SharedSparkContext {
  "TokenBlocking.removeBadWords" should {
    "retain given key-profile pair" when {
      "its key contains alphabet" in {
        val actual = TokenBlocking.removeBadWords(
          input = sc.makeRDD[(String, Int)](Seq(("A", 0)))
        ).collect
        val expected = Seq(("A", 0))
        actual shouldBe expected
      }
      "its key contains number" in {
        val actual = TokenBlocking.removeBadWords(
          input = sc.makeRDD[(String, Int)](Seq(("0", 0)))
        ).collect
        val expected = Seq(("0", 0))
        actual shouldBe expected
      }
    }
    "remove given key-profile pair" when {
      "its key contains kanji character" in {
        val actual = TokenBlocking.removeBadWords(
          input = sc.makeRDD[(String, Int)](Seq(("花", 0)))
        ).collect
        val expected = Seq.empty
        actual shouldBe expected
      }
    }
  }

  "TokenBlocking.createBlocks" should {
    "create a block" when {
      "given two profiles share the same token" in {
        import scala.collection.mutable.MutableList
        val dataset1: RDD[Profile] = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 0,
              attributes = MutableList(KeyValue("name", "Tom")),
              originalID = "",
              sourceId = 0
            )
          )
        )
        val dataset2: RDD[Profile] = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 1,
              attributes = MutableList(KeyValue("name", "Tom")),
              originalID = "",
              sourceId = 1
            )
          )
        )

        val expected: Array[BlockAbstract] = Array(
          BlockClean(blockID = 0, profiles = Array(Set(0), Set(1)))
        )
        val actual = TokenBlocking.createBlocks(
          profiles = dataset1.union(dataset2),
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        actual.map(_.blockID) should contain theSameElementsAs expected.map(_.blockID)
        actual.map(_.profiles) should contain theSameElementsAs expected.map(_.profiles)
      }
    }
    "create no blocks" when {
      "given two profiles doesn't share the same token" in {
        import scala.collection.mutable.MutableList
        val dataset1: RDD[Profile] = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 0,
              attributes = MutableList(KeyValue("name", "Joel")),
              originalID = "",
              sourceId = 0
            )
          )
        )
        val dataset2: RDD[Profile] = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 1,
              attributes = MutableList(KeyValue("name", "Tom")),
              originalID = "",
              sourceId = 1
            )
          )
        )

        val expected: Array[BlockAbstract] = Array.empty
        val actual = TokenBlocking.createBlocks(
          profiles = dataset1.union(dataset2),
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        actual shouldBe expected
      }
    }
  }
  "TokenBlocking.separateProfiles" should {
    "separate given elements" in {
      val actual = TokenBlocking.separateProfiles(
        elements = Set(0,1,2,3,4,5,6),
        separators = Array(3)
      )
      val expected = Array(Set(0,1,2,3), Set(4,5,6))
      actual shouldBe expected
    }
  }
}
