package SparkER.BlockBuildingMethods

import SparkER.DataStructures._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._

class SortedNeighborhoodSpec extends WordSpec with Matchers with SharedSparkContext {
  def createDataset: (RDD[Profile], RDD[Profile]) = {
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
        ),
        Profile(
          id = 2,
          attributes = MutableList(KeyValue("name", "Joel")),
          originalID = "",
          sourceId = 1
        ),
        Profile(
          id = 3,
          attributes = MutableList(KeyValue("name", "Joel")),
          originalID = "",
          sourceId = 1
        )
      )
    )
    (dataset1, dataset2)
  }

  "SortedNeighborhood.createBlocks" should {
    "return error" when {
      "wSize = 0" in {
        val (dataset1, dataset2) = createDataset
        intercept[org.apache.spark.SparkException] {
          SortedNeighborhood.createBlocks(
            profiles = dataset1.union(dataset2),
            wSize = 0,
            separatorIDs = Array(dataset1.map(_.id).max)
          ).collect
        }
      }
    }
    "create no blocks" when {
      "wSize = 1" in {
        val (dataset1, dataset2) = createDataset
        val expected: Array[BlockAbstract] = Array()
        val actual = SortedNeighborhood.createBlocks(
          profiles = dataset1.union(dataset2),
          wSize = 1,
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        actual.map(_.blockID) should contain theSameElementsAs expected.map(_.blockID)
        actual.map(_.profiles) should contain theSameElementsAs expected.map(_.profiles)
      }
    }

    "create blocks" when {
      "wSize = 2" in {
        val (dataset1, dataset2) = createDataset
        val expected: Array[BlockAbstract] = Array(
          BlockClean(blockID = 0, profiles = Array(Set(0), Set(1))),
          BlockClean(blockID = 1, profiles = Array(Set(0), Set(3)))
        )
        val actual = SortedNeighborhood.createBlocks(
          profiles = dataset1.union(dataset2),
          wSize = 2,
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        actual.map(_.blockID) should contain theSameElementsAs expected.map(_.blockID)
        actual.map(_.profiles) should contain theSameElementsAs expected.map(_.profiles)
      }

      "wSize = 3" in {
        val (dataset1, dataset2) = createDataset
        val expected: Array[BlockAbstract] = Array(
          BlockClean(blockID = 0, profiles = Array(Set(0), Set(1))),
          BlockClean(blockID = 1, profiles = Array(Set(0), Set(2,3))),
          BlockClean(blockID = 2, profiles = Array(Set(0), Set(3,1))),
          BlockClean(blockID = 3, profiles = Array(Set(0), Set(1)))
        )
        val actual = SortedNeighborhood.createBlocks(
          profiles = dataset1.union(dataset2),
          wSize = 3,
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        actual.map(_.blockID) should contain theSameElementsAs expected.map(_.blockID)
        actual.map(_.profiles) should contain theSameElementsAs expected.map(_.profiles)
      }

      "wSize = 4" in {
        val (dataset1, dataset2) = createDataset
        val expected: Array[BlockAbstract] = Array(
          BlockClean(blockID = 0, profiles = Array(Set(0), Set(1))),
          BlockClean(blockID = 1, profiles = Array(Set(0), Set(2,3,1))),
          BlockClean(blockID = 2, profiles = Array(Set(0), Set(1)))
        )
        val actual = SortedNeighborhood.createBlocks(
          profiles = dataset1.union(dataset2),
          wSize = 4,
          separatorIDs = Array(dataset1.map(_.id).max)
        ).collect
        actual.map(_.blockID) should contain theSameElementsAs expected.map(_.blockID)
        actual.map(_.profiles) should contain theSameElementsAs expected.map(_.profiles)
      }
    }
  }
}
