package SparkER.EntityClustering

import java.util.Calendar

import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.EntityClusterUtils.connectedComponents
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks.{break, breakable}

/**
  * Il problema è che ogni volta crea la matrice 16k x 16k
  * bisogna far si che la matrice creata abbia dimensione del connected component
  * in un qualche modo però bisogna avere a che fare con il separator id, è questo il problema.
  **/
object MarkovClustering extends EntityClusteringTrait {

  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    getClusters(profiles, edges, maxProfileID, edgesThreshold, separatorID, matrixSimThreshold = 1.0E-5, similarityChecksLimit = 2, clusterThreshold = 0.001)
  }

  def addSelfLoop(a: Array[Array[Double]]): Unit = {
    for (i <- a.indices) {
      a(i).update(i, 1.0)
    }
  }

  def printMatrix(simMatrix: Array[Array[Double]]): Unit = {
    for (i <- simMatrix.indices) {
      for (j <- simMatrix(i).indices) {
        print(simMatrix(i)(j) + " ")
      }
      //println()
    }
  }

  def normalizeColumns(a: Array[Array[Double]]): Unit = {
    for (j <- a(0).indices) {
      var sumCol = 0.0
      for (i <- a.indices) {
        sumCol += a(i)(j)
      }

      for (i <- a.indices) {
        a(i).update(j, a(i)(j) / sumCol)
      }
    }
  }

  def areSimilar(a: Array[Array[Double]], b: Array[Array[Double]], matrixSimThreshold: Double): Boolean = {
    if (a.length != b.length) {
      false
    }
    else if (a(0).length != b(0).length) {
      false
    }
    else {
      var equals = true
      breakable {
        for (i <- a.indices) {
          for (j <- a(0).indices) {
            if (math.abs(a(i)(j) - b(i)(j)) > matrixSimThreshold) {
              equals = false
              break
            }
          }
        }
      }
      equals
    }
  }

  def expand2(inputMatrix: Array[Array[Double]], separatorID: Long): Unit = {
    val t1 = Calendar.getInstance().getTimeInMillis
    val input = multiply(inputMatrix, inputMatrix, separatorID)
    val t2 = Calendar.getInstance().getTimeInMillis
    //println("Multiply " + (t2 - t1))

    for (i <- inputMatrix.indices) {
      System.arraycopy(input(i), 0, inputMatrix(i), 0, inputMatrix(0).length)
      /*for (j <- inputMatrix(i).indices) {
        inputMatrix(i).update(j, input(i)(j))
      }*/
    }
    val t3 = Calendar.getInstance().getTimeInMillis

    //println("Array copy " + (t3 - t2))
  }

  def hadamard(a: Array[Array[Double]], pow: Int): Unit = {
    for (i <- a.indices) {
      for (j <- a(i).indices) {
        a(i).update(j, math.pow(a(i)(j), pow))
      }
    }
  }

  def multiply(a: Array[Array[Double]], b: Array[Array[Double]], separatorID: Long): Array[Array[Double]] = {
    if (a.length != a(0).length) {
      throw new Exception("Illegal matrix size")
    }

    val t1 = Calendar.getInstance().getTimeInMillis

    val n1 = a.length
    val upLimit: Int = {
      if (separatorID >= 0) {
        separatorID.toInt
      }
      else {
        n1
      }
    }
    val lowLimit: Int = {
      if (separatorID >= 0) {
        separatorID.toInt
      }
      else {
        0
      }
    }

    val t2 = Calendar.getInstance().getTimeInMillis
    //println("CALCOLO LIMITI " + (t2 - t1))

    val c = Array.fill[Array[Double]](n1) {
      Array.fill[Double](n1) {
        0
      }
    }

    val t3 = Calendar.getInstance().getTimeInMillis
    //println("CREAZIONE MATRICE VUOTA " + (t3 - t2))


    for (i <- 0 until upLimit) {
      for (j <- lowLimit until n1) {
        for (k <- 0 until n1) {
          val res = c(i)(j) + a(i)(k) * b(k)(j)
          c(i).update(j, res)
        }
      }
    }

    val t4 = Calendar.getInstance().getTimeInMillis
    //println("PRODOTTO  " + (t4 - t3))


    if (separatorID >= 0) {

      for (i <- 0 until upLimit) {
        c(i).update(i, c(i)(i) + a(i)(i) * b(i)(i))
      }

      for (j <- lowLimit until n1) {
        c(j).update(j, c(j)(j) + a(j)(j) * b(j)(j))
      }
    }

    val t5 = Calendar.getInstance().getTimeInMillis

    //println("STEP FINALE  " + (t5 - t4))

    c
  }

  def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int, similarityChecksLimit: Int,
                  matrixSimThreshold: Double, clusterThreshold: Double): RDD[(Int, Set[Int])] = {

    //println("INPUT PARAM")
    //println("Edge threshold " + edgesThreshold)
    //println("similarityChecksLimit " + similarityChecksLimit)
    //println("matrixSimThreshold " + matrixSimThreshold)
    //println("clusterThreshold " + clusterThreshold)

    /** Generates the connected components */
    val cc = connectedComponents(edges.filter(_.weight > edgesThreshold))

    //println("CREO CONNECTED COMPONENTS")

    //val cc1 = profiles.context.parallelize(cc.take(2))
    //println("COMPONENTI CONNESSE")
    //cc1.foreach(println)
    //println("END")

    val res = cc.mapPartitions { partition =>

      var result: List[WeightedEdge] = Nil

      //println("INIZIALIZZO SIMILARITY MATRIX")

      partition.foreach { cluster =>

        val profilesMap = cluster.flatMap(x => x._1 :: x._2 :: Nil).toSet.toList.sorted.zipWithIndex.map(_.swap).toMap
        val invMap = profilesMap.map(_.swap)

        val internalSeparator = {
          if (separatorID >= 0) {
            val mSep = profilesMap.values.filter(_ <= separatorID).max
            invMap(mSep)
          }
          else {
            separatorID
          }
        }

        val matrixSize = profilesMap.keys.max + 1


        val simMatrix = Array.fill[Array[Double]](matrixSize) {
          Array.fill[Double](matrixSize) {
            0
          }
        }


        cluster.foreach { case (u, v, sim) =>
          //if (sim > threshold) {
          simMatrix(invMap(u)).update(invMap(v), sim)
          // }
        }

        addSelfLoop(simMatrix)
        //println("ADD SELF LOOP DONE")
        normalizeColumns(simMatrix)
        //println("NORMALIZE DONE")

        val atStart = Array.fill[Array[Double]](matrixSize) {
          Array.fill[Double](matrixSize) {
            0
          }
        }

        var cont = 0
        do {
          for (i <- simMatrix.indices) {
            System.arraycopy(simMatrix(i), 0, atStart(i), 0, simMatrix(0).length)
          }

          val t1 = Calendar.getInstance().getTimeInMillis
          expand2(simMatrix, internalSeparator)
          val t2 = Calendar.getInstance().getTimeInMillis
          //println("EXPAND2 " + (t2 - t1))
          normalizeColumns(simMatrix)
          val t3 = Calendar.getInstance().getTimeInMillis
          //println("NORMALIZE " + (t3 - t2))
          hadamard(simMatrix, 2)
          val t4 = Calendar.getInstance().getTimeInMillis
          //println("HADAMARD " + (t4 - t3))
          normalizeColumns(simMatrix)
          val t5 = Calendar.getInstance().getTimeInMillis
          //println("NORMALIZE " + (t5 - t4))
          cont += 1
        } while (!areSimilar(atStart, simMatrix, matrixSimThreshold) && cont < similarityChecksLimit)

        //println("LOOP DONE")

        val n1 = simMatrix.length
        val upLimit: Int = {
          if (internalSeparator >= 0) {
            internalSeparator.toInt
          }
          else {
            n1
          }
        }
        val lowLimit: Int = {
          if (internalSeparator >= 0) {
            internalSeparator.toInt
          }
          else {
            0
          }
        }

        for (i <- 0 until upLimit) {
          for (j <- lowLimit until n1) {
            val sim = math.max(simMatrix(i)(j), simMatrix(j)(i))
            if (sim > clusterThreshold && i != j) {
              result = WeightedEdge(profilesMap(i), profilesMap(j), sim) :: result
            }
          }
        }


        for (i <- simMatrix.indices) {
          for (j <- simMatrix(i).indices) {
            simMatrix(i).update(j, 0)
          }
        }

      }


      result.toIterator
    }

    ConnectedComponentsClustering.getClusters(profiles, res, maxProfileID, 0, separatorID)
  }
}
