import java.io.{File, FileWriter}

import com.typesafe.config.Config
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import spark.jobserver.{SparkJobValidation, SparkJobInvalid, SparkJobValid, SparkJob}


/**
 * Created by liusheng on 2/2/15.
 * step1:upload the jar to job-server
 * curl --data-binary @EmptyStreaming.jar 172.16.2.121:8090/jars/EmptyStreaming
 * step2:run it
 * curl -d "topic=\"liusheng\",zk=\"172.16.2.101\",kafkastreamnum=\"3\"" '172.16.2.101:8090/jobs?appName=EmptyStreaming&classPath=EmptyStreaming&context=jobapp&sync=false'
 */
class EmptyStreaming extends SparkJob with Logging{
  var ssc: StreamingContext = null

  override def stop(): Any = {
    ssc.stop(false)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    logInfo("validate")
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    logInfo("runJob")
    ssc = new StreamingContext(sc, Milliseconds(1000))
    val topic = Map(config.getString("topic") -> 1)
    val kafkaParams = Map(
      "zookeeper.connect" -> config.getString("zk"),
      "group.id" -> "streaming_test",
      "auto.commit.enable" -> "true",
      "auto.commit.interval.ms" -> "500")
    val kafkaStreams = for (i <- 1 to 3)
    yield KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams.toMap, topic, StorageLevel.RAMFS_AND_DISK)
    val unionStream = ssc.union(kafkaStreams)

    var total = 0

    unionStream.map(x => new String(x._2).toInt)
    .foreachRDD(rdd => {
      total += rdd.reduce(_+_)
    })

//    unionStream.foreachRDD { rdd =>
//      rdd.foreachPartition { part: Iterator[(String, Array[Byte])] =>
//        while (part.hasNext) {
//          val (_, rowVal): (String, Array[Byte]) = part.next()
//          val record = new String(rowVal)
//          logInfo(record + " shuai")
//        }
//      }
//    }

    ssc.start()
    ssc.awaitTermination(60000)
    ssc.stop(false)
    logInfo(total.toString + " shuai")
    val writer = new FileWriter(new File("/root/result.txt"))
    writer.write(total.toString)
    writer.close()
  }
}

