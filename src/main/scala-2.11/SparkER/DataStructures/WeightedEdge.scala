package SparkER.DataStructures

/**
  * Represents a weighted and undirected edge between two profiles
  * If it used in a clean-clean dataset the firstProfileID refers to first dataset, and the second one to the
  * second dataset.
  * @author Luca Gagliardelli
  * @since 2016/12/09
  */
case class WeightedEdge(firstProfileID : Int, secondProfileID : Int, weight : Double)  extends EdgeTrait with Serializable{

}
