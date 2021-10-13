package SparkER.BlockBuildingMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.DataStructures.KeyValue

class BlockingKeysStrategiesSpec extends WordSpec with Matchers with SharedSparkContext {
  "BlockingKeysStrategies.createKeysFromProfileAttributes" should {
    "create keys from profile attributes" in {
      val actual = BlockingKeysStrategies.createKeysFromProfileAttributes(
        attributes = Seq(
          KeyValue("name", "Tommy"),
          KeyValue("name", "Joel")
        )
      )
      val expected = Seq("tommy", "joel")
      actual should contain theSameElementsAs expected
    }
  }
  "BlockingKeysStrategies.createNgramsFromProfileAttributes" should {
    "create ngrams from profile attributes" in {
      val actual = BlockingKeysStrategies.createNgramsFromProfileAttributes(
        attributes = Seq(
          KeyValue("name", "Tommy"),
          KeyValue("name", "Joel")
        )
      )
      val expected = Seq("tom", "omm", "mmy", "joe", "oel")
      actual should contain theSameElementsAs expected
    }
  }

  "BlockingKeysStrategies.createSuffixesFromProfileAttributes" should {
    "create suffixes from profile attributes" in {
      val actual = BlockingKeysStrategies.createSuffixesFromProfileAttributes(
        attributes = Seq(
          KeyValue("name", "Tommy"),
          KeyValue("name", "Joel")
        )
      )
      val expected = Seq("my", "mmy", "ommy", "tommy", "el", "oel", "joel")
      actual should contain theSameElementsAs expected
    }
  }
  "BlockingKeysStrategies.createExtendedSuffixesFromProfileAttributes" should {
    "create extended suffixes from profile attributes" in {
      val actual = BlockingKeysStrategies.createExtendedSuffixesFromProfileAttributes(
        attributes = Seq(
          KeyValue("name", "Tommy"),
          KeyValue("name", "Joel")
        )
      )
      val expected = Seq("om", "tommy", "tomm", "to", "tom", "my", "mm", "ommy", "omm", "mmy", "jo", "oe", "el", "joel", "oel", "joe")
      actual should contain theSameElementsAs expected
    }
  }
  "BlockingKeysStrategies.createExtendedQgramsFromProfileAttributes" should {
    "create extended qgrams from profile attributes" in {
      val actual = BlockingKeysStrategies.createExtendedQgramsFromProfileAttributes(
        attributes = Seq(
          KeyValue("name", "Tommy"),
          KeyValue("name", "Joel")
        )
      )
      val expected = Seq("tomommmmy", "ommmmy", "tomomm", "oel", "joeoel", "tommmy", "joe")
      actual should contain theSameElementsAs expected
    }
  }

  "BlockingKeysStrategies.getCombinationsFor" should {
    "create combinations" when {
      "subListsLen <= sublists.size" in {
        val actual = BlockingKeysStrategies.getCombinationsFor(
          sublists = List("apple", "banana"),
          subListsLen = 2
        )
        val expected = Set("applebanana")
        actual shouldBe expected
      }
    }
    "create no combinations" when {
      "subListsLen = 0" in {
        val actual = BlockingKeysStrategies.getCombinationsFor(
          sublists = List("apple", "banana"),
          subListsLen = 0
        )
        val expected = Set()
        actual shouldBe expected
      }
      "subListsLen = 3 > sublists.size" in {
        val actual = BlockingKeysStrategies.getCombinationsFor(
          sublists = List("apple", "banana"),
          subListsLen = 3
        )
        val expected = Set()
        actual shouldBe expected
      }
    }
  }

  "BlockingKeysStrategies.getSuffixes" should {
    "get suffixes" when {
      "blockingKey.length < minimumLength" in {
        val actual = BlockingKeysStrategies.getSuffixes(
          minimumLength = 2,
          blockingKey = "apple"
        )
        val expected = Set("le", "ple", "pple", "apple")
        actual shouldBe expected
      }
      "blockingKey.length >= minimumLength" in {
        val actual = BlockingKeysStrategies.getSuffixes(
          minimumLength = 5,
          blockingKey = "apple"
        )
        val expected = Set("apple")
        actual shouldBe expected
      }
    }
  }

  "BlockingKeysStrategies.getExtendedSuffixes" should {
    "get extended suffixes" in {
      val actual = BlockingKeysStrategies.getExtendedSuffixes(
        minimumLength = 2,
        blockingKey = "apple"
      )
      val expected = Set("le", "pple", "appl", "ap", "ple", "ppl", "app", "apple", "pp", "pl")
      actual shouldBe expected
    }
  }

  "createNgramsFromProfileAttributes2" should {
    "create ngrams from profile attributes (2)" in {
      val actual = BlockingKeysStrategies.createNgramsFromProfileAttributes2(
        attributes = Seq(KeyValue("name", "apple"))
      )
      val expected = Seq("name_app", "name_ppl", "name_ple")
      actual shouldBe expected
    }
  }
}
