package Experiments

import SparkER.ProgressiveMethods.{GSPSN, LSPSN}
import SparkER.Wrappers.JSONWrapper
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/* Tests the implementations of GSPSN and LSPSN */
object Progressive {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")

    //Log file path
    val logFilePath = "log.txt"
    //Main dataset path
    val path = "datasets/clean/abtBuy/"
    //Dataset1 path
    val dataset1Path = path + "dataset1.json"
    //Dataset2 path
    val dataset2Path = path + "dataset2.json"
    //Groundtruth path
    val groundtruthPath = path + "groundtruth.json"

    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, logFilePath, false)
    log.addAppender(appender)

    /**
      * Loads the dataset
      **/
    val dataset1 = JSONWrapper.loadProfiles(dataset1Path, realIDField = "realProfileID", sourceId = 1)
    val maxIdDataset1 = dataset1.map(_.id).max()
    val dataset2 = JSONWrapper.loadProfiles(dataset2Path, realIDField = "realProfileID", sourceId = 2, startIDFrom = maxIdDataset1 + 1)
    val maxProfileID = dataset2.map(_.id).max()
    val profiles = dataset1.union(dataset2)

    //LSPSN
    val lspsn = new LSPSN(profiles, 2, maxIdDataset1)
    lspsn.initialize()
    println("Best LSPSN comparison: "+lspsn.getNextComparison)

    //GSPSN
    val gspsn = new GSPSN(profiles, 2, maxIdDataset1)
    gspsn.initialize()
    println("Best GSPSN comparison: "+gspsn.getNextComparison)
  }
}
