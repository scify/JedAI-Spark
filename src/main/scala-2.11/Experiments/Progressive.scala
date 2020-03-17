package Experiments

import java.io.{File, PrintWriter}
import java.util.Calendar

import SparkER.BlockBuildingMethods.BlockingUtils
import SparkER.DataStructures.Profile
import SparkER.Wrappers.JSONWrapper
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONArray

object Progressive {

  def getSortedCandidatesGSPSN(profiles: RDD[Profile],
                               positionIndex: Broadcast[scala.collection.Map[Int, Iterable[Int]]],
                               neighborList: Broadcast[scala.collection.Map[Int, Int]],
                               maxProfileID: Int,
                               wMax: Int): RDD[(Int, Int, Double)] = {

    /** Per ogni profilo */
    val results = profiles.mapPartitions { part =>
      val neighbors = Array.ofDim[Int](maxProfileID + 1)
      var neighborsNum = 0
      val cbs = Array.ofDim[Int](maxProfileID + 1)
      var res: List[(Int, Int, Double)] = Nil

      /** Per ogni partizione */
      part.foreach { profile =>

        /** Leggo le posizioni in cui compare il profilo */
        val positions = positionIndex.value(profile.id)
        positions.foreach { pos =>
          /** Per ogni finestra dalla 1 a wMax */
          for (windowSize <- 1 to wMax) {
            /** Leggo i vicini a distanza windowSize, sia prima che dopo */
            for (i <- -1 to 1 by 2) {
              val w = (windowSize * i)

              /** Verifico di essere all'interno dell'array */
              if ((w > 0 && (pos + w) < neighborList.value.size) || (w < 0 && (pos + w) > 0)) {
                /** Ottengo l'id del vicino nella posizione indicata */
                val pi = neighborList.value(pos + w)

                /** Controllo per evitare doppia emissione */
                if (pi < profile.id) {

                  /** Se è la prima volta che lo vedo, aggiungo all'elenco dei vicini */
                  if (cbs(pi.toInt) == 0) {
                    neighbors.update(neighborsNum, pi)
                    neighborsNum += 1
                  }

                  /** Incremento il numero di volte che ho trovato il vicino */
                  cbs.update(pi.toInt, cbs(pi.toInt) + 1)
                }
              }
            }
          }
        }

        //Adesso bisogna calcolare i pesi e aggiungere le coppie nella lista
        for (i <- 0 until neighborsNum) {
          val nId = neighbors(i).toInt
          val weight = cbs(nId) / (positions.size + positionIndex.value(nId).size - cbs(nId)).toDouble
          res = (neighbors(i), profile.id, weight) :: res
        }

        //Resetto i CBS e il numero di vicini per il profilo dopo
        for (i <- 0 until neighborsNum) {
          cbs.update(neighbors(i).toInt, 0)
        }
        neighborsNum = 0

      }

      res.toIterator
    }

    results.sortBy(-_._3)
  }

  def getSortedCandidatesLSPSN(profiles: RDD[Profile],
                               positionIndex: Broadcast[scala.collection.Map[Int, Iterable[Int]]],
                               neighborList: Broadcast[scala.collection.Map[Int, Int]],
                               maxProfileID: Int,
                               windowSize: Int): RDD[(Int, Int, Double)] = {

    /** Per ogni profilo */
    val results = profiles.mapPartitions { part =>
      val neighbors = Array.ofDim[Int](maxProfileID + 1)
      var neighborsNum = 0
      val cbs = Array.ofDim[Int](maxProfileID + 1)
      var res: List[(Int, Int, Double)] = Nil

      /** Per ogni partizione */
      part.foreach { profile =>

        /** Leggo le posizioni in cui compare il profilo */
        val positions = positionIndex.value(profile.id)
        positions.foreach { pos =>

          /** Leggo i vicini a distanza windowSize, sia prima che dopo */
          for (i <- -1 to 1 by 2) {
            val w = (windowSize * i)

            /** Verifico di essere all'interno dell'array */
            if ((w > 0 && (pos + w) < neighborList.value.size) || (w < 0 && (pos + w) > 0)) {
              /** Ottengo l'id del vicino nella posizione indicata */
              val pi = neighborList.value(pos + w)

              /** Controllo per evitare doppia emissione */
              if (pi < profile.id) {

                /** Se è la prima volta che lo vedo, aggiungo all'elenco dei vicini */
                if (cbs(pi.toInt) == 0) {
                  neighbors.update(neighborsNum, pi)
                  neighborsNum += 1
                }

                /** Incremento il numero di volte che ho trovato il vicino */
                cbs.update(pi.toInt, cbs(pi.toInt) + 1)
              }
            }
          }
        }

        //Adesso qua bisogna calcolare i pesi e aggiungere le coppie nella lista
        for (i <- 0 until neighborsNum) {
          val nId = neighbors(i).toInt
          val weight = cbs(nId) / (positions.size + positionIndex.value(nId).size - cbs(nId)).toDouble
          res = (neighbors(i), profile.id, weight) :: res
        }

        //Resetto i CBS e il numero di vicini per il profilo dopo
        for (i <- 0 until neighborsNum) {
          cbs.update(neighbors(i).toInt, 0)
        }
        neighborsNum = 0

      }

      res.toIterator
    }

    results.sortBy(-_._3)
  }

  def method(profiles: RDD[Profile], maxProfileID: Int, maxWindowSize: Int): Unit = {
    val sc = profiles.context

    /** Genera i token per ogni profilo, emette (token, idProfilo) */
    val tokens = profiles.flatMap { p =>
      p.attributes.flatMap { attr =>
        val set = attr.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).toSet
        set.map(t => (t, p.id))
      }
    }
    /** Ordina i token in ordine alfabetico, e sostituisce i token con un id univoco che rappresnta la posizione */
    val sorted = tokens.sortBy(_._1).zipWithIndex().map(x => (x._2.toInt, x._1._2))

    /** Position index, data una posizione ritorna l'id del profilo in quella posizione */
    val positionIndex: Broadcast[scala.collection.Map[Int, Iterable[Int]]] = sc.broadcast(sorted.map(_.swap).groupByKey().collectAsMap())
    /** Per ogni profilo crea una mappa che ritorna l'elenco di tutte le sue posizioni */
    val neighborList: Broadcast[scala.collection.Map[Int, Int]] = sc.broadcast(sorted.collectAsMap())

    for (wSize <- 1 to maxWindowSize) {
      val candidates = getSortedCandidatesGSPSN(profiles, positionIndex, neighborList, maxProfileID.toInt, wSize)
      candidates.take(10).foreach(println)
    }

    positionIndex.unpersist()
    neighborList.unpersist()
  }


  def main(args: Array[String]): Unit = {




    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")
      .set("spark.local.dir", "/data2/sparkTmp")


    //Log file path
    val logFilePath = "C:/users/gagli/Desktop/log.txt"
    //Main dataset path
    val path = "C:/Users/gagli/Desktop/progetti_intellij/SparkERmulti - Copia/datasets/clean/DblpAcm/"
    //Dataset1 path
    val dataset1Path = path + "dataset1.json"
    //Dataset2 path
    val dataset2Path = path + "dataset2.json"
    //Groundtruth path
    val groundtruthPath = path + "groundtruth.json"


    val sc = new SparkContext(conf)


    val sparkSession = SparkSession.builder().getOrCreate()
    val x = sparkSession.read.option("header", true).csv("C:\\Users\\gagli\\Desktop\\gt.csv")
    x.show()


    val ids = x.rdd.map(r => (r.getString(0).toLong, r.getString(3)))
    ids.take(10).foreach(println)

    ids.groupByKey().filter(_._2.size > 1).flatMap(x => x._2.toList.combinations(2)).map(x => x.mkString(",")).repartition(1).saveAsTextFile("C:/Users/gagli/Desktop/groundtruth")

    ???


      x.repartition(1).write.option("header","true") .csv("C:/Users/gagli/Desktop/csvout.csv")

    ???

    val a = sc.textFile("C:/Users/gagli/Desktop/matches.txt")
    val b = a.map(x => x.split(",")).filter(_.length > 1).zipWithIndex()
    b.flatMap(x => x._1.map(y => (x._2, y))).repartition(1).saveAsTextFile("C:/Users/gagli/Desktop/out.txt")

    ???

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, logFilePath, false)
    log.addAppender(appender)

    /**
      * Loads two datasets
      **/
    val dataset1 = JSONWrapper.loadProfiles(dataset1Path, realIDField = "realProfileID", sourceId = 1)
    val maxIdDataset1 = dataset1.map(_.id).max()
    val dataset2 = JSONWrapper.loadProfiles(dataset2Path, realIDField = "realProfileID", sourceId = 2, startIDFrom = maxIdDataset1 + 1)
    val maxProfileID = dataset2.map(_.id).max()
    val profiles = dataset1.union(dataset2)

    method(profiles, maxProfileID, 2)


  }
}
