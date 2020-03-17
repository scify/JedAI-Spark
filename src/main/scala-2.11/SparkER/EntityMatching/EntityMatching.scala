package SparkER.EntityMatching

import SparkER.DataStructures.{Profile, UnweightedEdge, WeightedEdge}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable


object EntityMatching {


  /**
    * Aggregate the comparisons of each Profile. The produced RDD will look like RDD[(Profile, Array(ProfileID)]
    *
    * @param candidatePairs   RDD containing the IDs of the profiles that must be compared
    * @param profiles         RDD of the Profiles
    * @return                 RDD[(profile, Array(profileID))] for which the profile must be compared with all
    *                         the profiles that their ids are in the Array(profileID)
    */
  def getComparisons(candidatePairs : RDD[UnweightedEdge], profiles : RDD[(Int, Profile)]) : RDD[(Profile, Array[Int])] ={
    candidatePairs
      .map(p => (p.firstProfileID.toInt, Array(p.secondProfileID.toInt)))
      .reduceByKey(_++_)
      .leftOuterJoin(profiles)
      .map(p => (p._2._2.get, p._2._1))
  }

  /**
    * For each partition return a Map with all the Profiles of the partition
    * The produced RDD will look like RDD[Map(ID -> Profile)]
    *
    * @param profiles   RDD containing the Profiles
    * @return           RDD in which each partition contains Map(ID -> Profile)
    */
  def getProfilesMap(profiles : RDD[(Int, Profile)] ) : RDD[Map[Int, Profile]] = {
    profiles
      .mapPartitions {
        partitions =>
          val m = partitions.map(p => Map(p._1 -> p._2)).reduce(_++_)
          Iterator(m)
      }
  }

  /**
    * Compare the input profiles and calculate the Weighted Edge that connects them
    *
    * @param profile1           Profile to be compared
    * @param profile2           Profile to be compared
    * @param matchingFunction   function that compares the profiles - returns weight
    * @return                   a WeightedEdge that connects these profiles
    */
  def profileMatching(profile1: Profile, profile2: Profile, matchingFunction: (Profile, Profile) => Double)
  :WeightedEdge = {
    val similarity = matchingFunction(profile1, profile2)
    WeightedEdge(profile1.id, profile2.id, similarity)
  }



  /**
    * Compare all the attributes of the Profiles and construct a Queue of similarity edges.
    * Then use it in order to calculate the similarity between the input Profiles
    *
    * @param profile1      Profile to be compared
    * @param profile2      Profile to be compared
    * @param threshold     the threshold which the similarity of two profiles must exceed in order
    *                      to be considered as a match
    * @return              a Weighted Edge which connects the input profiles
    */
  def groupLinkage(profile1: Profile, profile2: Profile, threshold: Double = 0.5)
  : WeightedEdge = {

    val similarityQueue: mutable.PriorityQueue[(Double, (String, String))] = MatchingFunctions.getSimilarityEdges(profile1, profile2, threshold)

    if (similarityQueue.nonEmpty ) {
      var edgesSet: Set[String] = Set()
      var nominator = 0.0
      var denominator = (profile1.attributes.length + profile2.attributes.length).toDouble
      similarityQueue.dequeueAll.foreach {
        edge =>
          if (!edgesSet.contains(edge._2._1) && !edgesSet.contains(edge._2._2)) {
            edgesSet += (edge._2._1, edge._2._2)
            nominator += edge._1
            denominator -= 1.0
          }
      }
      WeightedEdge(profile1.id, profile2.id, nominator / denominator)
    }
    else
      WeightedEdge(profile1.id, profile2.id, -1)
  }


  /** Entity Matching using Broadcast-Count
    * Aggregate all the comparisons of each profile and collect-broadcast them. // RDD[(Profile, Array(ProfileID)]
    * Then use the Profiles RDD to perform the comparisons
    *
    * @param profiles             RDD containing the profiles.
    * @param candidatePairs       RDD containing UnweightedEdges
    * @param bcstep               the number of partitions that will be broadcasted in each iteration
    * @param matchingMethodLabel  if it's "pm" use Profile Matcher, else Group Linkage
    * @param threshold            similarity thresholds for the Weighed Edges
    * @param matchingFunctions    the matching function that will compare the profiles
    * @return                     an RDD of WeighetedEdges
    * */
 def entityMatchingCB(profiles : RDD[Profile], candidatePairs : RDD[UnweightedEdge], bcstep : Int,
                     matchingMethodLabel : String = "pm", threshold: Double = 0.5,
                     matchingFunctions: (Profile, Profile) => Double = MatchingFunctions.jaccardSimilarity)
  : (RDD[WeightedEdge], Long) = {

    // set the entity matching function, either profile matcher or group linkage
    val  matcher = if (matchingMethodLabel == "pm")
        (p1: Profile, p2: Profile) => {this.profileMatching(p1, p2, matchingFunctions)}
    else (p1: Profile, p2: Profile) => {this.groupLinkage(p1, p2, threshold)}

    val sc = SparkContext.getOrCreate()

    val profilesID = profiles.map(p => (p.id.toInt, p))

    // construct the RDD of the comparisons - RDD[(Profile, Array(ProfileID)]
    val comparisonsRDD = getComparisons(candidatePairs, profilesID)
      .setName("ComparisonsPerPartition")
      .persist(StorageLevel.MEMORY_AND_DISK)

    // construct RDD containing the profiles og each partition as Map - RDD[Map(ID -> Profile)]
    val profilesMap = getProfilesMap(profilesID)
      .setName("profilesMap")
      .cache()

    // The RDD of the Matches is produced in a repetitive way
    // Collect and broadcast in parts (based on the step) the comparisonsRDD
    // and perform the comparisons. In the end unite the rdds containing the edges
    var edgesArrayRDD: Array[RDD[WeightedEdge]] = Array()

    val step = if (bcstep > 0) bcstep else comparisonsRDD.getNumPartitions
    val partitionGroupIter = (0 until comparisonsRDD.getNumPartitions).grouped(step)

    while (partitionGroupIter.hasNext) {
      val partitionGroup = partitionGroupIter.next()

      // collect and broadcast the comparisons
      val comparisonsPart = comparisonsRDD
        .mapPartitionsWithIndex((index, it) => if (partitionGroup.contains(index)) it else Iterator(), preservesPartitioning = true)
        .collect()

      val comparisonsPartBD = sc.broadcast(comparisonsPart)
      // perform comparisons
      val wEdges = profilesMap
        .flatMap { profilesMap =>
          comparisonsPartBD.value
            .flatMap { c =>
                  val profile1 = c._1
                  val comparisons = c._2
                  comparisons
                    .filter(profilesMap.contains)
                    .map(profilesMap(_))
                    .map(matcher(profile1, _))
                    .filter(_.weight >= 0.5)
              }
        }

      edgesArrayRDD = edgesArrayRDD :+ wEdges
    }

   // unite all the RDDs of edges
   val matches = sc.union(edgesArrayRDD)
     .setName("Matches")
     .persist(StorageLevel.MEMORY_AND_DISK)
   val matchesCount = matches.count()

   profilesMap.unpersist()
   comparisonsRDD.unpersist()

   (matches, matchesCount)
  }


  /**
    * Entity Matching using Broadcast-Join
    * Using twice reduceByKey and leftOuterJoin, aggregate the profiles that must compare.
    * Form the RDD RDD[(Profile, Array(Profile))] and perform the comparisons
    *
    * @param profiles             RDD containing the profiles.
    * @param candidatePairs       RDD containing UnweightedEdges
    * @param bcstep               the number of partitions that will be broadcasted in each iteration
    * @param matchingMethodLabel  if it's "pm" use Profile Matcher, else Group Linkage
    * @param threshold            similarity thresholds for the Weighed Edges
    * @param matchingFunctions    the matching function that will compare the profiles
    * @return                     an RDD of WeighetedEdges
    */
  def entityMatchingJOIN(profiles : RDD[Profile], candidatePairs : RDD[UnweightedEdge], bcstep : Int,
                       matchingMethodLabel : String = "pm", threshold: Double = 0.5,
                       matchingFunctions: (Profile, Profile) => Double = MatchingFunctions.jaccardSimilarity)
  : (RDD[WeightedEdge], Long) = {

    // set the entity matching function, either profile matcher or group linkage
    val  matcher = if (matchingMethodLabel == "pm")
      (p1: Profile, p2: Profile) => {this.profileMatching(p1, p2, matchingFunctions)}
    else (p1: Profile, p2: Profile) => {this.groupLinkage(p1, p2, threshold)}

    // aggregate the comparisons with their profiles
    val profilesID = profiles.map(p => (p.id.toInt, p))
    val comparisonsRDD = getComparisons(candidatePairs, profilesID)
      .flatMap(x => x._2.map(id => (id, Array(x._1))))
      .reduceByKey(_++_)
      .leftOuterJoin(profilesID)
      .map(p => (p._2._2.orNull, p._2._1))
      .filter(_._1 != null)
      .setName("ComparisonsRDD")
      .persist(StorageLevel.MEMORY_AND_DISK)

    // comparisons
    val matches = comparisonsRDD
      .flatMap { pc =>
        val profile1 = pc._1
        val comparisons = pc._2
        comparisons
          .map(matcher(profile1, _))
          .filter(_.weight >= 0.5)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    (matches, matches.count)
  }

}
