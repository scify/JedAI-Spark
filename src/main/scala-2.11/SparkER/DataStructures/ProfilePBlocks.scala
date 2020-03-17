package SparkER.DataStructures

case class ProfilePBlocks(profileID : Long, profile : Profile, blocks : Set[BlockWithComparisonSize]) extends Serializable{}