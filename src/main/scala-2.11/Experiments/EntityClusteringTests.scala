package Experiments

import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.{CutClustering, EntityClusterUtils}
import SparkER.Wrappers.CSVWrapper
import org.apache.spark.{SparkConf, SparkContext}

object EntityClusteringTests {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")
      .set("spark.local.dir", "/data2/sparkTmp")

    val sc = new SparkContext(conf)

    val edges = sc.textFile("C:/Users/gagli/Desktop/outf.txt").map { str =>
      val a = str.split(",")
      WeightedEdge(a(0).toInt, a(1).toInt, a(2).toDouble)
    }


    val maxProfileId = 1294

    val p = for (i <- 0 to maxProfileId) yield i
    val p2 = sc.parallelize(p)
    val profiles = p2.map(id => Profile(id))

    //val cc1 = ConnectedComponentsClustering.getClusters(profiles = profiles, edges, maxProfileId, edgesThreshold = 0.5)
    //val cc1 = CenterClustering.getClusters(profiles = profiles, edges, maxProfileId, edgesThreshold = 0.5)
    //val cc1 = MergeCenterClustering.getClusters(profiles = profiles, edges, maxProfileId, edgesThreshold = 0.5)
    //val cc1 = MarkovClustering.getClusters(profiles = profiles, edges, maxProfileId, edgesThreshold = 0.5, separatorID = -1)
    //val cc1 = RicochetSRClustering.getClusters(profiles = profiles, edges, maxProfileId, edgesThreshold = 0.5, separatorID = -1)
    val cc1 = CutClustering.getClusters(profiles = profiles, edges, maxProfileId, edgesThreshold = 0.5, separatorID = -1)


    val gt = CSVWrapper.loadGroundtruth("C:\\Users\\gagli\\Desktop\\gt.csv", header = true)

    val gtS = gt.map { x =>
      val e1 = x.firstEntityID.toInt
      val e2 = x.secondEntityID.toInt
      if (e1 < e2) {
        (e1, e2)
      }
      else {
        (e2, e1)
      }
    }.collect().toSet

    val gtBroadcast = sc.broadcast(gtS)

    println()
    println("NUMERO CLUSTER " + cc1.count())

    println("NUMERO PROFILI " + cc1.flatMap(_._2).count())

    println("Recall, precision: " + EntityClusterUtils.calcPcPqCluster(cc1, gtBroadcast))
  }



}
