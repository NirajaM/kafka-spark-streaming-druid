package pipeline

import pipeline.spark.SparkStreamingTasks

object Runner {

  def main(args: Array[String]): Unit = {
    
    SparkStreamingTasks.streamFromKafka()
    
  }
  
}