import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialOperator.JoinQuery

object SpatialJoin extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("SpatialJoinSpeciesPA").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val wdpa = new PolygonRDD(sc, "/home/tracek/Data/JRC/wdpa/wdpa.json", FileDataSplitter.GEOJSON, true)
  val species = new PolygonRDD(sc, "/home/tracek/Data/JRC/amph_unioned/amphib.json", FileDataSplitter.GEOJSON, true)

  wdpa.analyze()
  species.analyze()

  wdpa.spatialPartitioning(GridType.RTREE)
  species.spatialPartitioning(GridType.RTREE)

  println("Species count: " + species.countWithoutDuplicates())
  println("WDPA count: " + wdpa.countWithoutDuplicates())

  val jq = JoinQuery.SpatialJoinQuery(wdpa, species, false, false)
}
