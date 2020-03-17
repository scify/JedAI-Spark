package SparkER.DataStructures

trait PBlockAbstract extends Ordered[BlockAbstract] {
  val blockingKey: String
  /** Id of the block */
  val blockID: Long
  /** Entropy of the block */
  var entropy: Double
  /** Cluster */
  var clusterID: Integer
  /** Id of the profiles contained in the block */
  val profiles: Array[Set[(Long, Profile)]]//: Array[Set[Long]]

  /** Return the number of entities indexed in the block */
  def size: Double = profiles.map(_.size.toDouble).sum

  /* Return the number of comparisons entailed by this block */
  def getComparisonSize(): Double

  /* Returns all profiles */
  def getAllProfiles: Array[(Long, Profile)] = profiles.flatten

  /* Returns all the comparisons */
  def getComparisons() : Set[(Long, Long)]

  /** Default comparator, blocks will be ordered by its comparison size */
  def compare(that: BlockAbstract): Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}