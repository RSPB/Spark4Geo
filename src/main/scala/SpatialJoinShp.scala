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
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("SpatialJoinSpeciesPA").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val wdpa_shp = new ShapefileRDD(sc, "/home/tracek/Data/Spark4Geo/wdpa/wdpa.shp")
  val species_shp = new ShapefileRDD(sc, "/home/tracek/Data/Spark4Geo/amphib/amphib.shp")

  val wdpa = new PolygonRDD(wdpa_shp.getPolygonRDD, StorageLevel.MEMORY_ONLY)
  val species = new PolygonRDD(species_shp.getPolygonRDD, StorageLevel.MEMORY_ONLY)

  species.rawSpatialRDD = species.rawSpatialRDD.repartition(20)
  wdpa.rawSpatialRDD = wdpa.rawSpatialRDD.repartition(20)

  wdpa.analyze()
  species.analyze()

  wdpa.spatialPartitioning(GridType.QUADTREE)
  species.spatialPartitioning(wdpa.partitionTree)

  println("Species count: " + species.countWithoutDuplicates())
  println("WDPA count: " + wdpa.countWithoutDuplicates())

  val query = JoinQuery.SpatialJoinQuery(wdpa, species, false, false)
  val join_result = query.rdd.map((tuple: (Polygon, util.HashSet[Polygon])) => (tuple._1, tuple._2.asScala.map(tuple._1.intersection(_).getArea)) )
  val intersections = join_result.collect()
}
