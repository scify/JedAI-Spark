package SparkER.BlockRefinementMethods.PruningMethods

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.scalatest._
import SparkER.BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import SparkER.DataStructures.{BlockDirty, BlockClean, BlockAbstract, UnweightedEdge, ProfileBlocks}
import SparkER.Utilities.Converters

class CommonNodePruningSpec extends WordSpec with Matchers with SharedSparkContext {
  def blocksToBlockIndex(blocks: RDD[BlockAbstract]): Broadcast[scala.collection.Map[Int, Array[Set[Int]]]] = {
    val m = blocks.map { case block => (block.blockID, block.profiles) }.collect.toMap
    blocks.sparkContext.broadcast(m)
  }
  def blocksToBlocksSize(blocks: RDD[ProfileBlocks]): Broadcast[scala.collection.Map[Int, Int]] = {
    val m = blocks.map(pb => (pb.profileID, pb.blocks.size)).collect.toMap
    blocks.sparkContext.broadcast(m)
  }

  "CommonNodePruning.computeStatistics" should {
    "return (number of distinct edges, (profileID, number of neighbours)) tuples" in {
      val blocks: RDD[BlockAbstract] = sc.makeRDD(
        Seq(
          BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
          BlockClean(blockID = 1, profiles = Array(Set(0, 1), Set(2)))
        )
      )
      val actual = CommonNodePruning.computeStatistics(
        profileBlocksFiltered = Converters.blocksToProfileBlocks(blocks),
        blockIndex = blocksToBlockIndex(blocks),
        maxID = 3,
        separatorID = Array(1)
      ).collect
      val expected = List((1,(0,1)), (1,(1,1)), (0,(2,2)), (0,(3,0)))
      actual shouldBe expected
    }
  }
  "CommonNodePruning.calcChiSquare" should {
    "calc Chi-Square" in {
      val actual = CommonNodePruning.calcChiSquare(
        CBS = 5.0,
        neighbourNumBlocks = 6.0,
        currentProfileNumBlocks = 7.0,
        totalNumberOfBlocks = 10.0
      )
      // NOTE:
      //
      //    |p1|^p1|
      //  p0| 5|  1| 6
      // ---+--+---+--
      // ^p0| 2|  2| 4
      // ---+--+---+--
      //    | 7|  3|10
      //
      // (E1=6*7/10.0 - O1=5)^2/E1 + (E2=4*7/10.0 - O2=2)^2/E2 + (E3=6*3/10 - O3=1)^2/E3 + (E4=4*3/10 - O4=2)^2/E4 = 1.269...
      // Where E1~E4 are the expected values and O1~O4 are the observed values.
      //
      val expected = 1.2698412698412698
      actual shouldBe expected
    }
  }
  "CommonNodePruning.calcCBS" should {
    "calc CBS" in {
      val blocks: RDD[BlockAbstract] = sc.makeRDD(
        Seq(
          BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
          BlockClean(blockID = 1, profiles = Array(Set(0, 1), Set(2)))
        )
      )

      val maxID = 3
      val actual = Converters.blocksToProfileBlocks(blocks).collect.map { case pb =>
        CommonNodePruning.calcCBS(
          pb,
          blockIndex = blocksToBlockIndex(blocks),
          separatorID = Array(1),
          useEntropy = false,
          blocksEntropies = sc.broadcast(Map.empty[Int, Double]),
          weights = Array.fill[Double](maxID + 1) { 0.0 },
          entropies = Array.ofDim[Double](maxID + 1),
          neighbours = Array.ofDim[Int](maxID + 1),
          firstStep = false
        )
      }
      val expected = List(2, 2, 0, 0)
      actual shouldBe expected
    }
  }
  "CommonNodePruning.calcWeights" should {
    def weightCalcHelper(blocks: RDD[BlockAbstract], maxID: Int, separatorID: Array[Int], weightType: String): List[List[Double]] = {
      val stats = CommonNodePruning.computeStatistics(
        Converters.blocksToProfileBlocks(blocks),
        blockIndex = blocksToBlockIndex(blocks),
        maxID,
        separatorID
      )
      val numberOfEdges = stats.map(_._1.toDouble).sum()
      val edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
      val profileToNeighbours: Map[Int, (Array[Int], Int, Array[Double])] = {
        Converters.blocksToProfileBlocks(blocks).collect.map { case pb =>
          val neighbours = Array.ofDim[Int](maxID + 1)
          val weights = Array.fill[Double](maxID + 1) { 0.0 }
          val neigboursNumber = CommonNodePruning.calcCBS(
            pb,
            blockIndex = blocksToBlockIndex(blocks),
            separatorID,
            useEntropy = false,
            blocksEntropies = sc.broadcast(Map.empty[Int, Double]),
            weights,
            entropies = Array.ofDim[Double](maxID + 1),
            neighbours,
            firstStep = false
          )
          (pb.profileID, (neighbours, neigboursNumber, weights))
        }.toMap
      }
      val actual = Converters.blocksToProfileBlocks(blocks).collect.map { case pb =>
        val (neighbours, neighboursNumber, weights) = profileToNeighbours(pb.profileID)
        CommonNodePruning.calcWeights(
          pb,
          weights,
          neighbours,
          entropies = Array.ofDim[Double](maxID + 1),
          neighboursNumber,
          blockIndex = blocksToBlockIndex(blocks),
          separatorID,
          weightType,
          profileBlocksSizeIndex = blocksToBlocksSize(Converters.blocksToProfileBlocks(blocks)),
          useEntropy = false,
          numberOfEdges,
          edgesPerProfile
        )
        weights.toList
      }.toList
      actual
    }
    "calc weights" when {
      "the given weightType is CBS" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(1), Set(3)))
          )
        )
        val maxID = 3
        val actual = weightCalcHelper(blocks, maxID, Array(1), WeightTypes.CBS)
        val expected = List(
          List(0.0, 0.0, 1.0, 1.0),
          List(0.0, 0.0, 1.0, 2.0),
          List(0.0, 0.0, 0.0, 0.0),
          List(0.0, 0.0, 0.0, 0.0)
        )
        val func: (List[Double] => List[Double]) = { case list => list.map(x => if(x.isNaN) { -1000.0 } else { x }) }
        actual.flatMap(func) shouldBe expected.flatMap(func)
      }

      "the given weightType is chiSquare" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(1), Set(3)))
          )
        )
        val maxID = 3
        val actual = weightCalcHelper(blocks, maxID, Array(1), WeightTypes.chiSquare)
        val expected = List(
          List(0.0, 0.0, 2.0, Double.NaN),
          List(0.0, 0.0, Double.NaN, Double.NaN),
          List(0.0, 0.0, 0.0, 0.0),
          List(0.0, 0.0, 0.0, 0.0)
        )
        val func: (List[Double] => List[Double]) = { case list => list.map(x => if(x.isNaN) { -1000.0 } else { x }) }
        actual.flatMap(func) shouldBe expected.flatMap(func)
      }
      "the given weightType is ARCS" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(1), Set(3)))
          )
        )
        val maxID = 3
        val actual = weightCalcHelper(blocks, maxID, Array(1), WeightTypes.ARCS)
        // Note:
        //
        // B(p0,p2) = {b0}
        // B(p0,p3) = {b0}
        // B(p1,p2) = {b0}
        // B(p1,p3) = {b0, b1}
        // ||b0|| = 4
        // ||b1|| = 1
        //
        // WHERE:
        // B(px,py) is the set of blocks that contain px and py.
        // ||bx|| is the block cardinality.
        //
        // ARCS(p0,p2,B) = CBS(p0,p2)/||b0|| = 1/4 = 0.25
        // ARCS(p0,p3,B) = CBS(p0,p3)/||b0|| = 1/4 = 0.25
        // ARCS(p1,p2,B) = CBS(p1,p2)/||b0|| = 1/4 = 0.25
        // ARCS(p1,p3,B) = CBS(p1,p3)/||b0|| + (CBS(p1,p3)/||b0||)/||b1|| = 2/4 + 0.5/1 = 0.5
        //
        // Note that this implementation depends on CBS but the original paper doesn't.
        // For more info, see fig.4:
        // https://www.researchgate.net/profile/George-Papadakis-4/publication/301657953_Comparative_Analysis_of_Approximate_Blocking_Techniques_for_Entity_Resolution/links/5a0a81eaaca272d40f413584/Comparative-Analysis-of-Approximate-Blocking-Techniques-for-Entity-Resolution.pdf
        val expected = List(
          List(0.0, 0.0, 0.25, 0.25),
          List(0.0, 0.0, 0.25, 0.5),
          List(0.0, 0.0, 0.0, 0.0),
          List(0.0, 0.0, 0.0, 0.0)
        )
        val func: (List[Double] => List[Double]) = { case list => list.map(x => if(x.isNaN) { -1000.0 } else { x }) }
        actual.flatMap(func) shouldBe expected.flatMap(func)
      }
      "the given weightType is JS" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(1), Set(3)))
          )
        )
        val maxID = 3
        val actual = weightCalcHelper(blocks, maxID, Array(1), WeightTypes.JS)
        // Note:
        //
        // |B(p0,p2)| = 1
        // |B(p0,p3)| = 1
        // |B(p1,p2)| = 1
        // |B(p1,p3)| = 2
        // |B(p0)| = |B(p0,*) v B(*,p0)| = |{b0}| = 1
        // |B(p1)| = |B(p1,*) v B(*,p1)| = |{b0,b1}| = 2
        // |B(p2)| = |B(p2,*) v B(*,p2)| = |{b0}| = 1
        // |B(p3)| = |B(p3,*) v B(*,p3)| = |{b0,b1}| = 2
        //
        // JS(p0,p2,B) = |B(p0,p2)| / (|B(p0)| + |B(p2)| - |B(p0,p2)|) = 1 / (1+1-1) = 1
        // JS(p0,p3,B) = |B(p0,p3)| / (|B(p0)| + |B(p3)| - |B(p0,p3)|) = 1 / (1+2-1) = 0.5
        // JS(p1,p2,B) = |B(p1,p2)| / (|B(p1)| + |B(p2)| - |B(p1,p2)|) = 1 / (1+2-1) = 0.5
        // JS(p1,p3,B) = |B(p1,p3)| / (|B(p1)| + |B(p3)| - |B(p1,p3)|) = 2 / (2+2-2) = 1
        val expected = List(
          List(0.0, 0.0, 1.0, 0.5),
          List(0.0, 0.0, 0.5, 1.0),
          List(0.0, 0.0, 0.0, 0.0),
          List(0.0, 0.0, 0.0, 0.0)
        )
        val func: (List[Double] => List[Double]) = { case list => list.map(x => if(x.isNaN) { -1000.0 } else { x }) }
        actual.flatMap(func) shouldBe expected.flatMap(func)
      }
      "the given weightType is EJS" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(0, 1), Set(2, 3)))
          )
        )
        val maxID = 3
        val actual = weightCalcHelper(blocks, maxID, Array(1), WeightTypes.EJS)
        // Note:
        // JS(p0,p2,B) = |B(p0,p2)| / (|B(p0)| + |B(p2)| - |B(p0,p2)|) = 2 / (2+2-2) = 1
        // JS(p0,p3,B) = |B(p0,p3)| / (|B(p0)| + |B(p3)| - |B(p0,p3)|) = 2 / (2+2-2) = 1
        // JS(p1,p2,B) = |B(p1,p2)| / (|B(p1)| + |B(p2)| - |B(p1,p2)|) = 2 / (2+2-2) = 1
        // JS(p1,p3,B) = |B(p1,p3)| / (|B(p1)| + |B(p3)| - |B(p1,p3)|) = 2 / (2+2-2) = 1
        //
        // EJS(p0,p2,B) = JS(p0,p2,B) * log10(4/2) * log10(4/2) = 1.0 * 0.090... = 0.090...
        // EJS(p0,p3,B) = JS(p0,p3,B) * log10(4/2) * log10(4/2) = 1.0 * 0.090... = 0.090...
        // EJS(p1,p2,B) = JS(p1,p2,B) * log10(4/2) * log10(4/2) = 1.0 * 0.090... = 0.090...
        // EJS(p1,p3,B) = JS(p1,p3,B) * log10(4/2) * log10(4/2) = 1.0 * 0.090... = 0.090...
        val expected = List(
          List(0.0, 0.0, 0.09061905828945654, 0.09061905828945654),
          List(0.0, 0.0, 0.09061905828945654, 0.09061905828945654),
          List(0.0, 0.0, 0.0, 0.0),
          List(0.0, 0.0, 0.0, 0.0)
        )
        val func: (List[Double] => List[Double]) = { case list => list.map(x => if(x.isNaN) { -1000.0 } else { x }) }
        actual.flatMap(func) shouldBe expected.flatMap(func)
      }
      "the given weightType is ECBS" in {
        val blocks: RDD[BlockAbstract] = sc.makeRDD(
          Seq(
            BlockClean(blockID = 0, profiles = Array(Set(0, 1), Set(2, 3))),
            BlockClean(blockID = 1, profiles = Array(Set(1), Set(3)))
          )
        )
        val maxID = 3
        val actual = weightCalcHelper(blocks, maxID, Array(1), WeightTypes.ECBS)
        // Note:
        // CBS(p0,p2,B) * log10(|B|/|B(p0)|) * log10(|B|/|B(p2)|) = 1 * log10(2/1) * log10(2/1) = 0.09...
        val expected = List(
          List(0.0, 0.0, 0.09061905828945654, 0.0),
          List(0.0, 0.0, 0.0, 0.0),
          List(0.0, 0.0, 0.0, 0.0),
          List(0.0, 0.0, 0.0, 0.0)
        )
        val func: (List[Double] => List[Double]) = { case list => list.map(x => if(x.isNaN) { -1000.0 } else { x }) }
        actual.flatMap(func) shouldBe expected.flatMap(func)
      }
    }
  }
  "CommonNodePruning.doReset" should {
    "reset the given weights" in {
      val weights = Array(1.0, 1.0, 1.0)
      CommonNodePruning.doReset(
        weights,
        neighbours = Array(0, 1, 2),
        entropies = Array(),
        useEntropy = false,
        neighboursNumber = 3
      )
      val actual = weights
      val expected = List(0.0, 0.0, 0.0)
      actual shouldBe expected
    }
  }
}
