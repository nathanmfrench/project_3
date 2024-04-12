package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  case class VertexProperties(id: Long, degree: Int, value: Double, active: String, inMIS: String)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var misSet = Set[Long]()
    val vertexDegrees = g_in.degrees
    var activeGraph = g_in.outerJoinVertices(vertexDegrees) {
      case (_, _, degreeOpt) => VertexProperties(-1, degreeOpt.getOrElse(0), -0.1, "active", "No")
    }.cache()

    var iterationCount = 0
    while (activeGraph.vertices.filter { case (_, vp) => vp.active == "active" }.count() > 0) {
      activeGraph = activeGraph.mapVertices { case (_, vp) =>
        if (vp.active == "active")
          VertexProperties(vp.id, vp.degree, scala.util.Random.nextDouble(), vp.active, vp.inMIS)
        else
          vp
      }

      val maxNeighborValue: VertexRDD[VertexProperties] = activeGraph.aggregateMessages[VertexProperties](
        triplet => {
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (vp1, vp2) => if (vp1.value > vp2.value) vp1 else vp2
      )

      val candidates = activeGraph.vertices.join(maxNeighborValue)
        .filter { case (_, (vp, highest)) => vp.active == "active" && vp.value > highest.value }

      val newMISVertices = candidates.map(_._1).collect().toSet
      misSet ++= newMISVertices

      activeGraph = activeGraph.mapVertices { case (id, vp) =>
        if (newMISVertices.contains(id))
          VertexProperties(vp.id, vp.degree, -0.1, "inactive", "Yes")
        else if (misSet.contains(id) || misSet.exists(newMISVertices.contains))
          VertexProperties(vp.id, vp.degree, -0.1, "inactive", "No")
        else
          vp
      }.cache()

      println(s"\tNumber of active vertices remaining: ${activeGraph.vertices.filter { case (_, vp) => vp.active == "active" }.count()}")
      iterationCount += 1
    }
    println(s"Total iterations: $iterationCount")

    val finalVertices = activeGraph.vertices
      .map { case (id, vp) => (id, if (vp.inMIS == "Yes") 1 else -1) }
      .cache() // Make sure it's materialized and cached before usage

    // Use join to combine the properties
    val finalGraph = g_in.outerJoinVertices(finalVertices) {
      case (id, _, optProp) => optProp.getOrElse(-1)
    }

    return finalGraph

  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    // Preprocessing step
    if (g_in.vertices.filter { case (_, x) => x == 1 || x == -1 }.count() < g_in.vertices.count()) {
      println("Failed First Test")
      return false
    }

    // Aggregate messages to find the max and min labels of neighbors
    val messages = g_in.aggregateMessages[(Int, Int)](
      triplet => {
        triplet.sendToDst(triplet.srcAttr, triplet.srcAttr)
        triplet.sendToSrc(triplet.dstAttr, triplet.dstAttr)
      },
      (a, b) => (math.max(a._1, b._1), math.min(a._2, b._2))
    )

    // Join the vertices with the aggregated messages to check conditions
    val joined = g_in.outerJoinVertices(messages) {
      case (_, self, maxMinOption) =>
        val maxMin = maxMinOption.getOrElse((-1, -1))
        if (self == 1) {
          if (maxMin._1 == -1) 1 else 0
        } else {
          if (maxMin._1 == 1) 1 else 0
        }
    }

    // Check if there are any vertices labeled incorrectly according to the MIS rules
    val ans = if (joined.vertices.filter { case (_, x) => x == 0 }.count() >= 1) {
      return false
    } else {
      return true
    }

    return ans
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      println("Luby")
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      println("VERIFY")
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("YES the graph is an MIS")
      else
        println("NO the graph is not an MIS")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
