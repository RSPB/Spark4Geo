import java.util

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.apache.spark.storage.StorageLevel
import com.vividsolutions.jts.geom.Polygon

import scala.collection.JavaConverters._



object SpatialJoinShp extends App {

  def loadShapefile(path: String, numPartitions: Int = 20): PolygonRDD = {
    val shp = new ShapefileRDD(sc, path)
    val polygon = new PolygonRDD(shp.getPolygonRDD, StorageLevel.MEMORY_ONLY)
    polygon.rawSpatialRDD = polygon.rawSpatialRDD.repartition(numPartitions)
    polygon.analyze()
    polygon
  }

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("SpatialJoinSpeciesPA").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val wdpa = loadShapefile("/home/tracek/Data/Spark4Geo/wdpa/wdpa.shp")
  val species = loadShapefile("/home/tracek/Data/Spark4Geo/amphib/amphib.shp")

  wdpa.spatialPartitioning(GridType.QUADTREE)
  species.spatialPartitioning(wdpa.partitionTree)

  val query = JoinQuery.SpatialJoinQuery(wdpa, species, false, false)

  val user_data_sample = JoinQuery.SpatialJoinQuery(wdpa, species, false, false).first()._1.getUserData
  if (user_data_sample.toString.isEmpty) println("UserData is empty") else println(user_data_sample)

//  val join_result = query.rdd.map((tuple: (Polygon, util.HashSet[Polygon])) => (tuple._1, tuple._2.asScala.map(tuple._1.intersection(_).getArea)) )
//  val intersections = join_result.collect()
}
