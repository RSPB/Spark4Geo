import java.util
import java.io._

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

  def getMetadata(polygon: Polygon) = {
    val metadata = polygon.getUserData.toString.split("\t")
    val id: Int = metadata(0).toInt
    val name: String = metadata(1)
    (id, name)
  }

  def getIntersection(species: Polygon)(wdpa: Polygon) = {
    val intersection = wdpa.intersection(species)

    val wdpa_metadata = wdpa.getUserData.toString.split("\t")
    val wdpa_id: Int = wdpa_metadata(0).toInt
    val wdpa_name: String = wdpa_metadata(3)

    val species_metadata = species.getUserData.toString.split("\t")
    val species_id: Int = species_metadata(0).toInt
    val species_name: String = species_metadata(1)

    (wdpa_id, wdpa_name, species_id, species_name, intersection.getArea)
  }

  def getIntersection2(species: Polygon)(wdpa: Polygon) = {
    val intersection = wdpa.intersection(species)

    val wdpa_metadata = wdpa.getUserData.toString.split("\t")
    val wdpa_id: Int = wdpa_metadata(0).toInt
    val wdpa_name: String = wdpa_metadata(3)

    val species_metadata = species.getUserData.toString.split("\t")
    val species_id: Int = species_metadata(0).toInt
    val species_name: String = species_metadata(1)

    ((wdpa_id, species_id), intersection.getArea)
  }

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("SpatialJoinSpeciesPA").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val wdpa = loadShapefile("/home/tracek/Data/spark4geo_subset/wdpa/")
  val species = loadShapefile("/home/tracek/Data/spark4geo_subset/amphib/")

  wdpa.spatialPartitioning(GridType.QUADTREE)
  species.spatialPartitioning(wdpa.partitionTree)

  val query = JoinQuery.SpatialJoinQuery(wdpa, species, false, false)

  // val join_result = query.rdd.map((tuple: (Polygon, util.HashSet[Polygon])) => (tuple._1, tuple._2.asScala.map(tuple._1.intersection(_).getArea)) )
  // val join_result = query.rdd.map((tuple: (Polygon, util.HashSet[Polygon])) => tuple._2.asScala.map(x => getIntersection2(tuple._1)(x)  ) )
  val join_result = query.rdd.flatMap((tuple: (Polygon, util.HashSet[Polygon])) => tuple._2.asScala.map(x => getIntersection2(tuple._1)(x)  ) )
  val merge_results = join_result.reduceByKey(_ + _)
  val intersections_area = merge_results.collect()

  val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/tracek/results.txt")))

  for (row <- intersections_area) {
    writer.write(row._1._1 + "," + row._1._2 + "," + row._2 + "\n")
  }
  writer.close()
}

