package SparkER.DataStructures



case class PBlockDirty(blockID: Long, profiles: Array[Set[(Long, Profile)]], var entropy: Double = -1, var clusterID: Integer = -1, blockingKey: String = "") extends PBlockAbstract with Serializable  {

  override def getComparisonSize(): Double = {
    profiles.head.size.toDouble * (profiles.head.size.toDouble - 1)
  }

  override def getComparisons(): Set[(Long, Long)] = {
    profiles.head.toList.combinations(2).map { x =>
      if (x.head._1 < x.last._1) {
        (x.head._1, x.last._1)
      }
      else {
        (x.last._1, x.head._1)
      }
    }.toSet
  }
}