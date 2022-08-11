package SparkER.SimJoins.SimJoins

import java.util.Calendar

import SparkER.DataStructures.EDJoinPrefixIndexPartitioner
import SparkER.SimJoins.Commons.ED.{CommonEdFunctions, EdFilters}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object EDJoin {
  def buildPrefixIndex(sortedDocs: RDD[(Int, String, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])] = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)

    val allQgrams = sortedDocs.flatMap { case (docId, doc, qgrams) =>
      val prefix = qgrams.take(prefixLen)
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, (docId, index, qgrams, doc))
      }
    }

    val blocks = allQgrams.groupByKey().filter(_._2.size > 1)

    blocks.map(b => (b._1, b._2.toArray.sortBy(_._3.length)))
  }

  /**
    * Returns true if the token of the current block is the last common token
    *
    * @param doc1Tokens   tokens of the first document
    * @param doc2Tokens   tokens of the second document
    * @param currentToken id of the current block in which the documents co-occurs
    *
    */
  def isLastCommonTokenPosition(doc1Tokens: Array[(Int, Int)], doc2Tokens: Array[(Int, Int)], currentToken: Int, qgramLen: Int, threshold: Int): Boolean = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)
    var d1Index = math.min(doc1Tokens.length - 1, prefixLen - 1)
    var d2Index = math.min(doc2Tokens.length - 1, prefixLen - 1)
    var valid = true
    var continue = true

    /**
      * Starting from the prefix looking for the last common token
      * One exists for sure
      **/
    while (d1Index >= 0 && d2Index >= 0 && continue) {
      /**
        * Common token
        **/
      if (doc1Tokens(d1Index)._1 == doc2Tokens(d2Index)._1) {
        /**
          * If the token is the same of the current block, stop the process
          **/
        if (currentToken == doc1Tokens(d1Index)._1) {
          continue = false
        }
        else {
          /**
            * If it is different, it is not considered valid: needed to avoid to emit duplicates
            **/
          continue = false
          valid = false
        }
      }

      /**
        * Decrement the indexes (note: the tokens are sorted)
        **/
      else if (doc1Tokens(d1Index)._1 > doc2Tokens(d2Index)._1) {
        d1Index -= 1
      }
      else {
        d2Index -= 1
      }
    }
    valid
  }


  def getCandidatePairs(prefixIndex: RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])], qgramLength: Int, threshold: Int): RDD[((Int, String), (Int, String))] = {
    /**
      * Repartitions the blocks of the index based on the number of maximum comparisons involved by each block
      */
    val customPartitioner = new EDJoinPrefixIndexPartitioner(prefixIndex.getNumPartitions)
    val repartitionIndex = prefixIndex.map(_.swap).sortBy(x => -(x._1.length * (x._1.length - 1))).partitionBy(customPartitioner)


    repartitionIndex.flatMap { case (block, blockId) =>
      val results = new scala.collection.mutable.HashSet[((Int, String), (Int, String))]()

      var i = 0
      while (i < block.length) {
        var j = i + 1
        val d1Id = block(i)._1
        val d1Pos = block(i)._2
        val d1Qgrams = block(i)._3
        val d1 = block(i)._4

        while (j < block.length) {
          val d2Id = block(j)._1
          val d2Pos = block(j)._2
          val d2Qgrams = block(j)._3
          val d2 = block(j)._4

          if (d1Id != d2Id &&
            isLastCommonTokenPosition(d1Qgrams, d2Qgrams, blockId, qgramLength, threshold) &&
            math.abs(d1Pos - d2Pos) <= threshold &&
            math.abs(d1Qgrams.length - d2Qgrams.length) <= threshold
          ) {
            if (EdFilters.commonFilter(d1Qgrams, d2Qgrams, qgramLength, threshold)) {
              if (d1Id < d2Id) {
                results.add(((d1Id, d1), (d2Id, d2)))
              }
              else {
                results.add(((d2Id, d2), (d1Id, d1)))
              }
            }
          }
          j += 1
        }
        i += 1
      }
      results
    }
  }


  def getCandidates(documents: RDD[(Int, String)], qgramLength: Int, threshold: Int): RDD[((Int, String), (Int, String))] = {
    //Transforms the documents into n-grams
    val docs = documents.map(x => (x._1, x._2, CommonEdFunctions.getQgrams(x._2, qgramLength)))

    //Sorts the n-grams by their document frequency
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    sortedDocs.persist(StorageLevel.MEMORY_AND_DISK)
    sortedDocs.count()

    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = buildPrefixIndex(sortedDocs, qgramLength, threshold)
    prefixIndex.persist(StorageLevel.MEMORY_AND_DISK)
    val np = prefixIndex.count()
    sortedDocs.unpersist()
    val te = Calendar.getInstance().getTimeInMillis
    val log = LogManager.getRootLogger

    val a = prefixIndex.map(x => x._2.length.toDouble * (x._2.length - 1))
    val min = a.min()
    val max = a.max()
    val cnum = a.sum()
    val avg = cnum / np

    log.info("[EDJoin] Number of elements in the index " + np)
    log.info("[EDJoin] Min number of comparisons " + min)
    log.info("[EDJoin] Max number of comparisons " + max)
    log.info("[EDJoin] Avg number of comparisons " + avg)
    log.info("[EDJoin] Estimated comparisons " + cnum)

    log.info("[EDJoin] EDJOIN index time (s) " + (te - ts) / 1000.0)

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidatePairs(prefixIndex, qgramLength, threshold)
    val nc = candidates.count()
    prefixIndex.unpersist()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Candidates number " + nc)
    log.info("[EDJoin] EDJOIN join time (s) " + (t2 - t1) / 1000.0)

    candidates
  }

  /**
    * Perform the ED Join, returning the pairs of documents that satisfies the condition.
    * @param documents documents to join
    * @param qgramLength length of the q-grams
    * @param threshold maximum edit distance
    * */
  def getMatches(documents: RDD[(Int, String)], qgramLength: Int, threshold: Int): RDD[(Int, Int)] = {

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidates(documents, qgramLength, threshold)
    val log = LogManager.getRootLogger

    val t2 = Calendar.getInstance().getTimeInMillis

    val m = candidates
      .filter { case ((d1Id, d1), (d2Id, d2)) => CommonEdFunctions.editDist(d1, d2) <= threshold }
      .map { case ((d1Id, d1), (d2Id, d2)) => (d1Id, d2Id) }
    m.persist(StorageLevel.MEMORY_AND_DISK)
    val nm = m.count()
    val t3 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Num matches " + nm)
    log.info("[EDJoin] Verify time (s) " + (t3 - t2) / 1000.0)
    log.info("[EDJoin] Global time (s) " + (t3 - t1) / 1000.0)
    m
  }
}
