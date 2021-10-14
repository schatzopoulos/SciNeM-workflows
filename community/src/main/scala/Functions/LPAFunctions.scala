package Functions

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.graphx._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.{Map, mutable}
import org.apache.spark.sql.functions.{col, explode_outer}

object LPAFunctions {
  /***
   * This function reads the .csv file(s) containing the edges of the transformed Graph and creates a new Graph from them.
   *
   * @param inputDirectoryPath the relations which will be used to create the transformed graph
   * @param delimiter The delimiter which will be used to read the csv files in the folders
   * @return the transformed graph to be used in the (O)LPA algorithm
   */
  def readTGraph(inputDirectoryPath: String, delimiter: String = "\t") :Graph[VertexId, Long]= {

    require(inputDirectoryPath.nonEmpty, s"Path must not be empty")
    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate

    // Build edges DataFrame
    var edgesDF: DataFrame = null
    val hdfsFolder = getHdfs(inputDirectoryPath)
    val outPutPath = new Path(inputDirectoryPath)
    val findStatus = hdfsFolder.getFileStatus(outPutPath)

    if (hdfsFolder.exists(outPutPath) && findStatus.isDirectory) {
      hdfsFolder.listStatus(outPutPath)
        .toList
        .foreach(file => {
          if (file.getPath.getName.endsWith(".csv")) {
            val tempDF = spark.read
              .format("csv")
              .option("header", "true")
              .option("mode", "DROPMALFORMED")
              .option("delimiter", delimiter)
              .option("inferSchema", "true")
              .load(file.getPath.toString)
              .withColumn("src", col("src").cast(LongType))
              .withColumn("dst", col("dst").cast(LongType))
              .withColumn("numberOfPaths", col("numberOfPaths").cast(LongType))
            if(!tempDF.isEmpty){
              if(edgesDF == null){
                edgesDF = tempDF
              }
              else{
                edgesDF = edgesDF.union(tempDF)
              }
              tempDF.unpersist()
            }
          }
        })
    }
    val edgesRDD = edgesDF.rdd.map(node => Edge(node.getAs[Long]("src"), node.getAs[Long]("dst"), node.getAs[Long]("numberOfPaths")))
    edgesDF.unpersist()
    val tGraph = Graph.fromEdges(edgesRDD, 0L)
    edgesRDD.unpersist()
    tGraph
  }

  /***
   *
   * This function executes the Label Propagation Algorithm (LPA) in the transformed Graph using GraphX's Pregel.
   * Each vertex is initially assigned to its own community. At every superstep, vertices
   * send their community affiliation to all neighbors and update their state to the vertex community
   * affiliation of incoming messages. A vertex does not send its community affiliation to a neighbor
   * if said neighbor satisfied the stopping criterion. The algorithm runs for a user-defined number of times.
   *
   *
   * @param graph The transformed graph for whose vertices we will compute their community affiliations.
   * @param maxSteps The maximum number of supersteps of LPA to be performed.
   * @param stopCriterion The number of times that a vertex can have the same community affiliation
   *                      before it stops been included in the remaining supersteps of the LPA.
   * @return a graph with vertex attributes containing the label of community affiliation
   */
  def runLPA(graph: Graph[VertexId, Long], maxSteps: Int, stopCriterion: Int): Graph[VertexId, Long] = {

    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")
    require(stopCriterion > 0, s"Number of times with the same label must be greater than 0, but got $stopCriterion")
    val lpaGraph: Graph[(VertexId, Int), VertexId] = graph.mapVertices { case (vid, _) => (vid, 0) }.cache()
    graph.unpersistVertices()
    graph.edges.unpersist()

    def sendMessage(e: EdgeTriplet[(VertexId, Int), Long]): Iterator[(VertexId, Map[VertexId, Long])] = {
      if (e.srcAttr._2 == stopCriterion && e.dstAttr._2 == stopCriterion){
        Iterator.empty
      }
      else if(e.srcAttr._2 == stopCriterion && e.dstAttr._2 < stopCriterion){
        Iterator((e.srcId, Map[VertexId, Long]()), (e.dstId, Map(e.srcAttr._1 -> e.attr)))
      }
      else if(e.srcAttr._2 < stopCriterion && e.dstAttr._2 == stopCriterion){
        Iterator((e.srcId, Map(e.dstAttr._1 -> e.attr)), (e.dstId, Map[VertexId, Long]()))
      }
      else{
        Iterator((e.srcId, Map(e.dstAttr._1 -> e.attr)), (e.dstId, Map(e.srcAttr._1 -> e.attr)))
      }
    }
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
    : Map[VertexId, Long] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }(collection.breakOut)
    }
    def vertexProgram(vid: VertexId, attr: (Long, Int), message: Map[VertexId, Long]): (VertexId, Int) = {
      if (message.isEmpty) attr
      else {
        if (attr._2 != stopCriterion){
          val maxVId = message.maxBy(_._2)._1
          if (maxVId == attr._1){
            (maxVId, attr._2 + 1)
          }
          else{
            (maxVId, 1)
          }
        }
        else{
          attr
        }

      }
    }
    val initialMessage = Map[VertexId, Long]()
    val communitiesGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    lpaGraph.unpersistVertices()
    lpaGraph.edges.unpersist()

    communitiesGraph.mapVertices({case (_,attr) => attr._1})
  }


  /***
   * This function executes the Overlapping Label Propagation Algorithm (OLPA) in the transformed Graph using GraphX's Pregel.
   * It is based on the abovementioned LPA but instead of single community per node , multiple communities can be assigned
   * per node. Each vertex is initially assigned to its own community. At every superstep, vertices
   * send their community affiliations to all neighbors and update their state to the vertex community
   * affiliations of incoming messages that overcome a user-defined threshold. A vertex does not send its community
   * affiliation to a neighbor if said neighbor satisfied the stopping criterion.
   * The algorithm runs for a user-defined number of times.
   *
   *
   * @param graph    The transformed graph for whose vertices we will compute their community affiliations.
   * @param maxSteps  the number of supersteps of OLPA to be performed.
   * @param threshold     the threshold which the incoming communities must pass to be included in
   *                      the community affiliations of a vertex.
   * @param stopCriterion the number of times that a vertex can have the same community affiliations
   *                      before it stops been included in the remaining supersteps of the OLPA.
   * @return a graph with vertex attributes containing community affiliations and their scores.
   */
  def runOLPA(graph: Graph[VertexId, Long], maxSteps: Int, threshold: Double, stopCriterion: Int): Graph[Map[VertexId, Double], Long] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")
    require(threshold >= 0.0 & threshold < 1, s"Threshold must be greater or equal than 0 and less than 1, but got $threshold")
    require(stopCriterion > 0, s"Number of times with the same label must be greater than 0, but got $stopCriterion")

    val lpaGraph: Graph[(Map[VertexId, Double], Int), Long] = graph.mapVertices { case (vid, _) => (Map[VertexId, Double](vid -> 1), 0) }.cache()
    graph.unpersistVertices()
    graph.edges.unpersist()
    def sendMessage(e: EdgeTriplet[(Map[VertexId, Double], Int), Long]): Iterator[(VertexId, Map[VertexId, Double])] = {
      if (e.srcAttr._2 == stopCriterion && e.dstAttr._2 == stopCriterion){
        Iterator.empty
      }
      else if(e.srcAttr._2 == stopCriterion && e.dstAttr._2 < stopCriterion){
        Iterator((e.srcId, Map[VertexId, Double]()), (e.dstId, e.srcAttr._1.mapValues(_/2 * e.attr).map(identity)))
      }
      else if(e.srcAttr._2 < stopCriterion && e.dstAttr._2 == stopCriterion){
        Iterator((e.srcId, e.dstAttr._1.mapValues(_/2 * e.attr).map(identity)),(e.dstId, Map[VertexId, Double]()))
      }
      else{
        Iterator((e.srcId, e.dstAttr._1.mapValues(_/2 * e.attr).map(identity)), (e.dstId, e.srcAttr._1.mapValues(_/2 * e.attr).map(identity)))
      }
    }

    def mergeMessage(count1: Map[VertexId, Double], count2: Map[VertexId, Double])
    : Map[VertexId, Double] = {
      (count1.keySet ++ count2.keySet).map(key => {
        val count1Val = count1.getOrElse(key, 0.0)
        val count2Val = count2.getOrElse(key, 0.0)
        key -> (count1Val + count2Val)
      })(collection.breakOut)
    }

    def vertexProgram(vid: VertexId, attr: (Map[VertexId, Double], Int), message: Map[VertexId, Double]): (Map[VertexId, Double], Int) = {
      if (message.isEmpty)
        attr
      else {
        var coefficientSum = message.values.sum

        if (attr._2 != stopCriterion) {
          // normalize the map so that every node has total coefficientSum as 1
          val normalizedMap: Map[VertexId, Double] = message.map(row => {
            row._1 -> (row._2/coefficientSum)
          })

          var resMap: mutable.Map[VertexId, Double] = new mutable.HashMap[VertexId, Double]
          var maxRow: VertexId = 0L
          var maxRowValue: Double = Double.MinValue

          normalizedMap.foreach(row => {
            if (row._2 >= threshold) {
              resMap += row
            } else if (row._2 > maxRowValue) {
              maxRow = row._1
              maxRowValue = row._2
            }
          })

          // add maximum value node in result map if there is no node with sum greater then threshold
          if (resMap.isEmpty) {
            resMap += (maxRow -> maxRowValue)
          }

          coefficientSum = resMap.values.sum
          // if same nodes as previous iteration then increase stop criterion
          if (resMap.keySet.equals(attr._1.keySet)) {
            (resMap.map(row => {
              row._1 -> (row._2/coefficientSum)
            }), attr._2 + 1)
          }
          else {
            (resMap.map(row => {
              row._1 -> (row._2/coefficientSum)
            }), 1)
          }
        }
        else {
          attr
        }
      }
    }

    val initialMessage = Map[VertexId, Double]()

    val overlapCommunitiesGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    lpaGraph.unpersistVertices()
    lpaGraph.edges.unpersist()
    overlapCommunitiesGraph.mapVertices((_, vertexProperties) => vertexProperties._1)
  }

  /***
   * This function writes the vertices and their community affiliations.
   *
   * @param vertices the vertices of the graph that is produced after LPA
   * @param outputLPA the output path where we will write the results of the LPA algorithm
   */
  def writeResultsLPA(vertices: VertexRDD[VertexId], outputLPA: String) :Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    val print_lpa = vertices.sortBy(_._2, ascending = false).collect().toList.toDF()
    vertices.unpersist()
    print_lpa.coalesce(1).write.mode(SaveMode.Overwrite).format("csv")
      .option("delimiter", "\t").save(path = outputLPA)
  }

  /***
   * This function writes the vertices and their community affiliations along with their scores.
   * The greater the score of a community the greater the chance that the vertex belong to that community.
   *
   * @param vertices the vertices of the graph that is produced after OLPA
   * @param outputOLPA the output path where we will write the results of the OLPA algorithm
   */
  def writeResultsOLPA(vertices: VertexRDD[Map[VertexId, Double]], outputOLPA: String) :Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    val print_olpa = vertices.sortBy(_._1, ascending = false)
      .collect().toList.toDF("nodes","communities")
      .select($"nodes", explode_outer($"communities"))
        .sort($"key".asc)

    print_olpa.coalesce(1).write.mode(SaveMode.Overwrite).format("csv")
      .option("nullValue", "null").option("delimiter", "\t").save(path = outputOLPA)
  }

  /***
   *
   * @param path the path to the hdfs folder/file
   * @return Generate hadoop FileSystem
   */
  private def getHdfs(path: String): FileSystem = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }
}
