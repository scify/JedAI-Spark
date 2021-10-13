package SparkER.LivyJobs

import SparkER.DataStructures.Profile
import SparkER.Wrappers.{CSVWrapper, JSONWrapper, SerializedObjectLoader}
import org.apache.livy.Job
import org.apache.livy.JobContext
import org.apache.spark.rdd.RDD

/**
 * @author George Mandilaras (NKUA)
 */
case class ReadProfilesJob(filepath: String) extends Job[Array[Profile]] {

    def call(jobContext:JobContext): Array[Profile] = {
        val extension: String = filepath.split("\\.").last
        val entitiesRDD: RDD[Profile] = extension match {
            case "csv" => CSVWrapper.loadProfiles(this.filepath, 0, "id")
            case "json" => JSONWrapper.loadProfiles(this.filepath, 0, "id")
            case _ => SerializedObjectLoader.loadProfiles(this.filepath, 0, "id")
        }
        entitiesRDD.take(100)
    }

}
