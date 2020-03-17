package SparkER.BlockBuildingMethods

import SparkER.BlockBuildingMethods.TokenBlocking.removeBadWords
import SparkER.DataStructures._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

object SortedNeighborhood {

  class KeyPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }


  /**
    * Performs the token blocking
    *
    * @param profiles      input to profiles to create blocks
    * @param separatorIDs  id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocks(profiles: RDD[Profile], wSize: Int, separatorIDs: Array[Int] = Array.emptyIntArray, keysToExclude: Iterable[String] = Nil, removeStopWords: Boolean = false,
                   createKeysFunctions: (Iterable[KeyValue], Iterable[String]) => Iterable[String] = BlockingKeysStrategies.createKeysFromProfileAttributes): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFunctions(profile.attributes, keysToExclude).filter(_.trim.length > 0).toSet))
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    //val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(_._2.size > 1)
    val a = if (removeStopWords) {
      removeBadWords(tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID))
    } else {
      tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID)
    }


    val sorted = a.sortByKey()

    val partialRes = sorted.mapPartitionsWithIndex { case (partID, partData) =>

      val pDataLst = partData.toList

      //Genero i blocchi per questa partizione, però dovrò andare anche a fare quelli che mancano che sono in comune
      //con le partizioni successive
      val blocks = pDataLst.map(_._2).sliding(wSize)

      //Se non è il primo devo prendere i primi W-1 elementi per passarli alla partizione precedente
      val first = {
        if (partID != 0) {
          (partID - 1, pDataLst.take(wSize - 1)) //Emetto gli elementi associati all'id della partizione che dovrà vederli
        }
        else {
          (partID, Nil)
        }
      }

      //Se non è l'ultimo devo prendere gli ultimi W-1 elementi per passarli alla partizione successiva
      val last = {
        if (partID != sorted.getNumPartitions - 1) {
          (partID, pDataLst.takeRight(wSize - 1)) //Emetto gli elementi associati all'id della partizione attuale
        }
        else {
          (partID, Nil)
        }
      }
      List((blocks, first :: last :: Nil)).toIterator
    }

    //Prima parte dei blocchi generati
    val b1 = partialRes.flatMap(_._1)

    val b2 = partialRes.flatMap(_._2).partitionBy(new KeyPartitioner(sorted.getNumPartitions)).mapPartitions { part =>
      part.flatMap(_._2).toList.sortBy(_._1).map(_._2).sliding(wSize)
    }

    val blocks = b1.union(b2)

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = blocks.map {
      c =>
        val entityIds = c.toSet

        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds)
          }
          else {
            TokenBlocking.separateProfiles(entityIds, separatorIDs)
          }
        }
        blockEntities
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1

    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case (entityIds, blockId) =>
        if (separatorIDs.isEmpty)
          BlockDirty(blockId.toInt, entityIds)
        else BlockClean(blockId.toInt, entityIds)
    }
  }
}
