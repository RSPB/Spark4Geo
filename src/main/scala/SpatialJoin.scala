import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree

object SpatialJoin extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("SpatialJoinSpeciesPA").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val wdpa = new PolygonRDD(sc, "/home/tracek/Data/JRC/wdpa/wdpa_simplified.json", FileDataSplitter.GEOJSON, true, 20)
  val species = new PolygonRDD(sc, "/home/tracek/Data/JRC/amphib/amphib_simplified.json", FileDataSplitter.GEOJSON, true, 20)

  wdpa.analyze()
  species.analyze()

  wdpa.spatialPartitioning(GridType.QUADTREE)
  species.spatialPartitioning(wdpa.partitionTree)

  println("Species count: " + species.countWithoutDuplicates())
  println("WDPA count: " + wdpa.countWithoutDuplicates())

  val jq = JoinQuery.SpatialJoinQuery(wdpa, species, false, false)
  println(jq.first())
}
