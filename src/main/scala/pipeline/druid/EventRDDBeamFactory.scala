package pipeline.druid

import com.metamx.tranquility.spark.BeamFactory
import com.metamx.tranquility.beam.Beam
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import io.druid.query.aggregation.CountAggregatorFactory
import org.joda.time.DateTime
import com.metamx.tranquility.druid._
import io.druid.granularity.QueryGranularity
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.common.Granularity
import org.joda.time.Period

class EventRDDBeamFactory extends BeamFactory[Map[String,String]] {

  lazy val makeBeam: Beam[Map[String,String]] = {
    val curator = CuratorFrameworkFactory.newClient(
      "192.168.99.100:3181",
      new BoundedExponentialBackoffRetry(100, 3000, 5))
    curator.start()

    val indexService = "overlord" 
    val discoveryPath = "/druid/discovery"
    
    val dataSource = "test"
    val dimensions = IndexedSeq("ip")
    val aggregators = Seq(new CountAggregatorFactory("website"))
    
    val timestampFn = (message: Map[String,String]) => new DateTime(message.get("time").get)
    
    DruidBeams
      .builder(timestampFn)
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation.create(indexService, dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularity.MINUTE))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          windowPeriod = new Period("PT10M"),
          partitions = 1,
          replicants = 1
        )
      )
      .buildBeam()
  }
}