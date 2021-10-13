package SparkER.LivyJobs

import SparkER.DataStructures.MatchingEntities
import SparkER.Wrappers.{CSVWrapper, JSONWrapper, SerializedObjectLoader}
import org.apache.livy.{Job, JobContext}
import org.apache.spark.rdd.RDD



/**
 * @author George Mandilaras (NKUA)
 */
case class ReadGroundTruthJob(filepath: String) extends Job[Array[MatchingEntities]] {

	def call(jobContext: JobContext): Array[MatchingEntities] = {
		val extension: String = filepath.split("\\.").last
		val entitiesRDD: RDD[MatchingEntities] = extension match {
				case "csv" => CSVWrapper.loadGroundtruth(this.filepath)
				case "json" => JSONWrapper.loadGroundtruth(this.filepath, "id1", "id2")
				case _ => SerializedObjectLoader.loadGroundtruth(this.filepath)
			}
		entitiesRDD.take(100)
	}
}


