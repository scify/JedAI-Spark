package SparkER.BlockBuildingMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.DataStructures.{KeyValue, Profile, BlockAbstract, BlockClean, KeysCluster}

class LSHSpec extends WordSpec with Matchers with SharedSparkContext {
  "LSH.getHashes2" should {
    "return a hash which contains the specified number of bits" in {
      val actual = LSH.getHashes2(10, 128)
      val expected = 128
      actual.size shouldBe expected
    }
    "return the same hash if given two strings are the same" in {
      val actual = LSH.getHashes2(10, 128)
      val expected = LSH.getHashes2(10, 128)
      actual shouldBe expected
    }
  }
  "LSH.getNumBands" should {
    "return the number of bands" when {
      "given threshold is 1.0" in {
        val actual = LSH.getNumBands(1.0, 128)
        val expected = 2
        actual shouldBe expected
      }
      "given threshold is 0.8" in {
        val actual = LSH.getNumBands(0.8, 128)
        val expected = 12
        actual shouldBe expected
      }
      "given threshold is 0.0" in {
        val actual = LSH.getNumBands(0.0, 128)
        val expected = 129
        actual shouldBe expected
      }
    }
  }
  "LSH.getNumRows" should {
    "return the number of rows" in {
      val actual = LSH.getNumRows(0.8, 128)
      val expected = 128 / 12
      actual shouldBe expected
    }
  }
  "LSH.calcSimilarity" should {
    "return the similarity" when {
      "sig1 and sig2 shares the totally same elements" in {
        val actual = LSH.calcSimilarity(
          sig1 = Array(0,1,2,3,4,5,6,7,8,9),
          sig2 = Array(0,1,2,3,4,5,6,7,8,9)
        )
        val expected = 1.0
        actual shouldBe expected
      }
    }
    "return NaN" when {
      "neither sig1 nor sig2 has elements" in {
        val actual = LSH.calcSimilarity(
          sig1 = Array(),
          sig2 = Array()
        )
        actual.isNaN shouldBe true
      }
    }
  }

  "LSH.clusterSimilarAttributes" should {
    import scala.collection.mutable.MutableList
    def createAttribute(key: String, baseToken: String, size: Int): MutableList[KeyValue] = {
      var list: MutableList[KeyValue] = MutableList.empty
      for (i <- 0 until size) {
        list += KeyValue(key, f"${baseToken}${i}")
      }
      list
    }

    "put name and label into the same cluster (excepting bar)" when {
      "their jaccard similarity is greater than threshold and LMI could capture them" in {
        val dataset1 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 0,
              attributes = createAttribute("name", "apple", 10) ++ createAttribute("bar", "banana", 1),
              originalID = "",
              sourceId = 0
            )
          )
        )
        val dataset2 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 1,
              attributes = createAttribute("label", "apple", 10) ++ createAttribute("bar", "cat", 1),
              originalID = "",
              sourceId = 1
            )
          )
        )
        val actual = LSH.clusterSimilarAttributes(
          profiles = dataset1.union(dataset2),
          numHashes = 128,
          targetThreshold = 0.8,
          maxFactor = 1.0
        )
        val expected = List(
          KeysCluster(0,List("0_name", "1_label"),1.0,0.8),
          KeysCluster(1,List("1_bar", "0_bar"),1.0,0.8)
        )
        actual.map(_.keys.sorted) should contain theSameElementsAs expected.map(_.keys.sorted)
      }
    }
    "put name, label and bar into the same cluster" when {
      "there are no similar attributes" in {
        val dataset1 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 0,
              attributes = createAttribute("name", "apple", 10) ++ createAttribute("bar", "banana", 1),
              originalID = "",
              sourceId = 0
            )
          )
        )
        val dataset2 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 1,
              attributes = createAttribute("label", "apple", 1) ++ createAttribute("bar", "cat", 1),
              originalID = "",
              sourceId = 1
            )
          )
        )
        val actual = LSH.clusterSimilarAttributes(
          profiles = dataset1.union(dataset2),
          numHashes = 128,
          targetThreshold = 0.8,
          maxFactor = 1.0
        )
        val expected = List(
          KeysCluster(0,List("0_name", "1_label", "0_bar", "1_bar"),1.0,0.8)
        )
        actual.map(_.keys.sorted) should contain theSameElementsAs expected.map(_.keys.sorted)
      }
    }
  }
  "LSH.createBlocks" should {
    import scala.collection.mutable.MutableList
    def createAttribute(key: String, baseToken: String, size: Int): MutableList[KeyValue] = {
      var list: MutableList[KeyValue] = MutableList.empty
      for (i <- 0 until size) {
        list += KeyValue(key, f"${baseToken}${i}")
      }
      list
    }

    "create no blocks" when {
      "given two profiles aren't similar (jaccard similarity(p0,p1) = 0.33 < threshold)" in {
        val dataset1 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 0,
              attributes = createAttribute("name", "apple", 3),
              originalID = "",
              sourceId = 0
            )
          )
        )
        val dataset2 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 1,
              attributes = createAttribute("label", "apple", 1),
              originalID = "",
              sourceId = 1
            )
          )
        )
        val actual = LSH.createBlocks(
          profiles = dataset1.union(dataset2),
          numHashes = 128,
          targetThreshold = 0.9,
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        val expected = Seq()
        actual shouldBe expected
      }
    }
    "create blocks" when {
      "given two profiles are similar (jaccard similarity(p0,p1) = 1.0 > threshold)" in {
        val dataset1 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 0,
              attributes = createAttribute("name", "apple", 1),
              originalID = "",
              sourceId = 0
            )
          )
        )
        val dataset2 = sc.makeRDD[Profile](
          Seq(
            Profile(
              id = 1,
              attributes = createAttribute("label", "apple", 1),
              originalID = "",
              sourceId = 1
            )
          )
        )
        val actual = LSH.createBlocks(
          profiles = dataset1.union(dataset2),
          numHashes = 128,
          targetThreshold = 0.5,
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        val expected = Seq(
          BlockClean(blockID = 0, profiles = Array(Set(0),Set(1)))
        )
        actual.map(_.blockID) should contain theSameElementsAs expected.map(_.blockID)
        actual.map(_.profiles) should contain theSameElementsAs expected.map(_.profiles)
      }
    }
  }
}
