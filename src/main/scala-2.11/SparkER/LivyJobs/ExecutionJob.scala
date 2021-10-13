package SparkER.LivyJobs

import org.apache.livy.{Job, JobContext}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext

case class ExecutionJob(n: Int) extends Job[Int] with Serializable {

	def call(jobContext: JobContext):Int= {
		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		val log = LogManager.getRootLogger
		log.setLevel(Level.INFO)
		val sc:SparkContext = jobContext.sc()
		val range = Range(0, n)
		sc.parallelize(range).sum().toInt
	}
}

