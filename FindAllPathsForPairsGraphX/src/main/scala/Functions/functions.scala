package Functions

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.graphx._
import org.apache.spark.sql.functions.{col, expr, lit, map, max, min}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object functions extends Serializable {
  /** *
   * This function creates the Initial Graph with vertices in nodes_dir folder and edges in relations_dir folder.
   * To create the vertices it reads all the vertex files, with user-defined delimiter, from nodes_dir folder
   * and creates a DataFrame with id, type, Map[String, String] columns where we have:
   * * id: The id of the vertex which may not given as unique but is made unique internally as GraphX does not
   *   distinguise the ids based on type.
   * * type: A String corresponding to the file name which is the type of the vertex
   * * Map[String, String] : Each column that is not named id and type is part of the attribute Map
   *   with the name of the column as key and its value in the corresponding row as value.
   *
   * Similarly, to create the edges it reads all the edge files, with user-defined delimiter, from relations_dir folder
   * and creates a DataFrame with src, dst, type, Map[String, String] columns where we have:
   * * src: The id of the src vertex which based on the src vertex type changes its value to its unique which is used internally.
   * * dst: The id of the dst vertex which based on the dst vertex type changes its value to its unique which is used internally.
   * * type: A String corresponding to the file name which is the type of the edge
   * * Map[String, String] : Each column that is not named src, dst and type is part of the attribute Map
   *   with the name of the column as key and its value in the corresponding row as value.
   *
   * From verticesDF we create the VertexRDD and from edgesDF we create the EdgesRDD used to create the Graph.
   * Additionally, we return a Map[String, Long] which has vertex type as key and
   * the value added to each vertex of this type to create its unique id.
   *
   * @param nodes_dir The path where the nodes folder exists
   * @param relations_dir The path where the relations folder exists
   * @param delimiter      The delimiter which will be used to read the csv files in the folders
   * @return (a map with has vertex types as keys and the value added to each vertex of this type to create its
   *         unique id, a graph, filled with the data whose path we have given, to be used in custom methods)
   */
  def createInputGraph(nodes_dir: String = "", relations_dir: String = "", delimiter: String = "\t"): (Map[String, VertexId], Graph[(String, Map[String, String]), (String, Map[String, String])]) = {
    require(nodes_dir.nonEmpty, s"Path must not be empty")
    require(relations_dir.nonEmpty, s"Path must not be empty")
    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate()
    import spark.implicits._

    // keep in map the (previous max - current min + 1) of previous dataFrame calculation or 0 (if no increase)
    // and compare it to the min of the next so as not to have conflicts in relations
    var mapGlobalIncrease: Map[String, VertexId] = Map[String, VertexId]()

    var prMaxId = 0L
    var prMinId = 0L
    //Build vertices DataFrame
    var verticesDF: DataFrame = null
    val nodesHdfsFolder = getHdfs(nodes_dir)
    val inputNodesPath = new Path(nodes_dir)
    var findStatus = nodesHdfsFolder.getFileStatus(inputNodesPath)
    if (nodesHdfsFolder.exists(inputNodesPath) && findStatus.isDirectory) {
      nodesHdfsFolder.listStatus(inputNodesPath)
        .toList
        .foreach(file => {
          if (file.getPath.getName.contains(".csv")) {
            val vType = file.getPath.getName.split("\\.")(0).trim
            var df = spark.read
              .format("csv")
              .option("header", "true") // first line in file has headers and one of them must be id
              .option("mode", "DROPMALFORMED")
              .option("delimiter", delimiter)
              .option("inferSchema", "true")
              .load(file.getPath.toString)

            // add new column type which is filled with the name of the file for all elements
            df = df.withColumn("id", col("id").cast(LongType)).withColumn("type", lit(vType))
            // put all the attribute columns except id and type to a map
            val vAttrColumns = df.columns.filterNot(elm => elm == "id" | elm == "type")
            val kvCols = vAttrColumns.flatMap { c =>
              Seq(lit(c), col(c))
            }
            df = df
              .withColumn("vAttr", map(kvCols: _*))
              .select($"id", $"type", $"vAttr")

            // take max of previous .csv and change it with max of current .csv
            // also if max previous greater than current min then increase all number by (previous max - current min + 1)
            val dfMinId = df.agg(min("id")).head().getLong(0)
            val dfMaxId = df.agg(max("id")).head().getLong(0)
            var increaseIdByValue = 0L

            if (prMaxId == 0) {
              prMinId = dfMinId
              prMaxId = dfMaxId
              mapGlobalIncrease += vType -> 0L
            }
            else {
              if (prMaxId >= dfMinId) {
                if (prMinId > dfMaxId & prMaxId > dfMinId) {
                  prMinId = dfMinId
                  mapGlobalIncrease += vType -> 0L
                }
                else {
                  increaseIdByValue = prMaxId - dfMinId + 1L
                  prMaxId = dfMaxId + increaseIdByValue
                  df = df.withColumn("id", expr("id + " + increaseIdByValue).cast(LongType))
                  mapGlobalIncrease += vType -> increaseIdByValue
                }
              }
              else {
                prMaxId = dfMaxId
                mapGlobalIncrease += vType -> 0L
              }
            }

            if (verticesDF == null) {
              verticesDF = df
            }
            else {
              verticesDF = verticesDF.union(df)
            }
            df.unpersist()
          }
        })
    }
    nodesHdfsFolder.close()

    //Build edges DataFrame
    var edgesDF: DataFrame = null
    val edgesHdfsFolder = getHdfs(relations_dir)
    val inputEdgesPath = new Path(relations_dir)
    findStatus = edgesHdfsFolder.getFileStatus(inputEdgesPath)
    if (edgesHdfsFolder.exists(inputEdgesPath) && findStatus.isDirectory) {
      edgesHdfsFolder.listStatus(inputEdgesPath)
        .toList
        .foreach(file => {
          if (file.getPath.getName.contains(".csv")) {
            val vType = file.getPath.getName.split("\\.")(0).trim
            val getType = vType.toArray[Char]
            var df = spark.read
              .format("csv")
              .option("header", "true") // first line in file has headers and two of them must be src and dst
              .option("mode", "DROPMALFORMED")
              .option("delimiter", delimiter)
              .option("inferSchema", "true")
              .load(file.getPath.toString)
              .withColumn("src", col("src").cast(LongType))
              .withColumn("dst", col("dst").cast(LongType))

            // add new column type which is filled with the name of the file for all elements
            df = df.withColumn("type", lit(vType))
            // put all the attribute columns except src, dst and type to a map
            val eAttrColumns = df.columns.filterNot(elm => elm == "src" | elm == "dst" | elm == "type")
            val kvCols = eAttrColumns.flatMap { c =>
              Seq(lit(c), col(c))
            }
            df = df
              .withColumn("eAttr", map(kvCols: _*))
              .select($"src", $"dst", $"type", $"eAttr")

            // take the 2 Char of the name and for each take from map the number
            // and for both (src and dst) increase all values, src by vType(0) and dst by vType(1), following the map
            var increaseIdByValue = mapGlobalIncrease(getType(0).toString)
            df = df.withColumn("src", expr("src + " + increaseIdByValue).cast(LongType))
            increaseIdByValue = mapGlobalIncrease(getType(1).toString)
            df = df.withColumn("dst", expr("dst + " + increaseIdByValue).cast(LongType))

            if (edgesDF == null) {
              edgesDF = df
            }
            else {
              edgesDF = edgesDF.union(df)
            }
            df.unpersist()
          }
        })
    }
    edgesHdfsFolder.close()

    // Build RDDs and Graph
    val verticesRDD = verticesDF.rdd.map(node => (node.getAs[Long]("id"), (node.getAs[String]("type"), node.getAs[Map[String, String]]("vAttr"))))
    verticesDF.unpersist()
    val edgesRDD = edgesDF.rdd.map(node => Edge(node.getAs[Long]("src"), node.getAs[Long]("dst"), (node.getAs[String]("type"), node.getAs[Map[String, String]]("eAttr"))))
    edgesDF.unpersist()
    val graph = Graph(verticesRDD, edgesRDD)
    verticesRDD.unpersist()
    edgesRDD.unpersist()

    // The edges create null vertices if they cannot find them, so we have to filter them out
    (mapGlobalIncrease, graph.filter(graph => graph, vpred = (_: VertexId, vData: (String, Map[String, String])) => vData != null))
  }

  /** *
   * This function reads the vertices pairs which act as start and end vertices in the paths that we are searching.
   * We make use of the delimiter to obtain the first and second vertices in each pair.
   * Each vertex is of the form (Type1.id 'delimiter' Type2.id)
   * and based on the type we increase the id so that it matches its unique value.
   *
   * @param inputFilePath path to the .csv file with the input pairs
   * @param mapGlobalIncrease how much the id, based on the Type of vertex,
   *                          had to be increased to transform into unique id
   * @param delimiter     The delimiter which will be used to read the csv file
   * @return a list which contains the pairs to be used in findAllPaths
   */
  def createPairList(inputFilePath: String, mapGlobalIncrease: Map[String, VertexId], delimiter: String = ","): List[(VertexId, VertexId)] = {
    require(inputFilePath.nonEmpty, s"Path must not be empty")
    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate
    var listSet = mutable.Set[(VertexId, VertexId)]()
    val rddFromFile = spark.sparkContext.textFile(inputFilePath)
    rddFromFile.collect().foreach { f =>
      val splitIds = f.split(delimiter)
      val splitFirstType = splitIds(0).split("\\.").map(_.trim)
      val splitSecondType = splitIds(1).split("\\.").map(_.trim)
      val firstId = splitFirstType(1).toLong + mapGlobalIncrease.getOrElse(splitFirstType(0), 0L)
      val secondId = splitSecondType(1).toLong + mapGlobalIncrease.getOrElse(splitSecondType(0), 0L)

      listSet += ((firstId, secondId))
    }
    rddFromFile.unpersist()
    listSet.toList
  }



  /***
   * This function uses the input Graph and makes use of GraphX's Pregel to send messages from all its vertices to
   * each of their neighbors. In the beginning, the active vertices (those that send messages) are only the ones listed
   * as start vertices in each of the given pairs. In each iteration, except the first one, if a vertex that has received
   * at least one message belongs to the end vertices then it appends the paths that began from a start vertex
   * it has received to a List[String] and deletes them from the ones it will send to its neighbors. The paths that
   * already contain the vertex they are about to be send are discarded so as not to have circles in the resulted paths.
   * After the final iteration we append, from the end vertices, all the List[String] containing the paths
   * in a final List[String].
   *
   * @param graph This graph is produced by the method createInputGraph
   * @param listOfVertices List with (start, end) pair of vertices
   * @param length              number of jumps from start to end vertex
   * @param mapGlobalIncrease how much the id, based on the Type of vertex,
   *                          had to be increased to transform into unique id
   * @return a list of String where each String corresponds to a path
   */
  def findAllPathsForPairs(graph: Graph[(String, Map[String, String]), (String, Map[String, String])],
                           listOfVertices: List[(VertexId, VertexId)], length: Int, mapGlobalIncrease: Map[String, VertexId]): List[String] = {
    require(length > 1, s"The source has to make more than 1 hops to reach the destination")

    // a map with destination vertices as keys and as value a list of sources
    // a list of sources used to send the initial message on Pregel
    // a list of final destinations from all the pairs
    val (connectionsStartMap, listOfSources, listOfDestinations) = fill(listOfVertices)
    // Initialize the preliminaryGraph with each edge attribute
    // having level 0 and each vertex with attributes 0 and an empty Map.
    val preliminaryGraph: Graph[(Int, Map[VertexId, List[String]], String, List[String]), Int] = graph
      // Start with level 0 and an empty Map which will be used for the paths
      .mapVertices {
        case (_, vData) => (0, Map[VertexId, List[String]](), vData._1, List[String]())
      }
      // Set the level on the edges as 0
      .mapTriplets(_ => 0)
    graph.unpersistVertices()
    graph.edges.unpersist()

    // Define the three functions needed to implement the creation of the Graph
    // in the GraphX version of Pregel.
    def vertexProgram(id: VertexId, value: (Int, Map[VertexId, List[String]], String, List[String]),
                      msgSum: (Int, Map[VertexId, List[String]])): (Int, Map[VertexId, List[String]], String, List[String]) = {
      val (level, _, vType, list) = value
      var (msg_level, msg_map) = msgSum

      if (msg_level == 0 & level == msg_level & listOfSources.contains(id)) {
        val temp = vType + "." + (id - mapGlobalIncrease.getOrElse(vType, 0L)).toString
        connectionsStartMap(id).foreach { endVertex =>
          var checkIfExists = msg_map.getOrElse(endVertex, List[String]())
          if (checkIfExists.isEmpty) {
            msg_map += (endVertex -> List.fill(1)(temp))
          }
          else {
            checkIfExists = temp :: checkIfExists
            msg_map += (endVertex -> checkIfExists)
          }
        }
        (1, msg_map, vType, list)
      }
      else if (0 < msg_level & msg_level <= length & level <= msg_level) {
        // if vertex is final destination and hops != 1 (direct connection) then save to the list of paths
        // if 1-hop and vertex is final destination search
        // and discard what it has received from src vertices where it is the destination
        if (msg_level == 1) {
          if (msg_map.keySet.toList.contains(id)) {
            msg_map = msg_map.-(id)
            (msg_level + 1, msg_map, vType, list)
          }
          else {
            (msg_level + 1, msg_map, vType, list)
          }
        }
        else {
          if (msg_map.keySet.toList.contains(id)) {
            val newList = list ++ msg_map.getOrElse(id, List[String]())
            msg_map = msg_map.-(id)
            (msg_level + 1, msg_map, vType, newList)
          }
          else {
            (msg_level + 1, msg_map, vType, list)
          }
        }
      }
      else {
        value
      }
    }

    def sendMessage(edge: EdgeTriplet[(Int, Map[VertexId, List[String]], String, List[String]),
      Int]): Iterator[(VertexId, (Int, Map[VertexId, List[String]]))] = {

      if (edge.srcAttr._1 == 0 | edge.srcAttr._1 < edge.dstAttr._1) {
        Iterator.empty
      }
      else if (edge.srcAttr._1 == 1) {
        if (edge.srcAttr._2.isEmpty) {
          Iterator.empty
        }
        else {
          var newMap: Map[VertexId, List[String]] = Map[VertexId, List[String]]()
          val destination = edge.dstAttr._3 + "." + (edge.dstId - mapGlobalIncrease.getOrElse(edge.dstAttr._3, 0L)).toString
          edge.srcAttr._2.foreach {
            outerValueMap => {
              var newList: List[String] = outerValueMap._2.filterNot {
                (_: String).trim.contains(edge.dstId.toString)
              }
              if (newList.nonEmpty) {
                newList = newList.map { s =>
                  var newString = ""
                  if (s != destination) {
                    newString = s + "," + destination
                  }
                  newString
                }
                val filteredList = newList.filter(_ != "")
                if (filteredList.nonEmpty) {
                  newMap += (outerValueMap._1 -> filteredList)
                }
              }
            }
          }
          if (newMap.nonEmpty) {
            Iterator((edge.dstId, (edge.srcAttr._1, newMap)))
          }
          else {
            Iterator.empty
          }
        }
      }
      else if (1 < edge.srcAttr._1 & edge.srcAttr._1 <= length) {
        if (edge.srcAttr._2.isEmpty) {
          Iterator.empty
        }
        else {
          var newMap: Map[VertexId, List[String]] = Map[VertexId, List[String]]()
          val destination = edge.dstAttr._3 + "." + (edge.dstId - mapGlobalIncrease.getOrElse(edge.dstAttr._3, 0L)).toString
          edge.srcAttr._2.foreach {
            outerValueMap => {
              var newList: List[String] = outerValueMap._2.filterNot {
                (_: String).split(",").map(_.trim).contains(edge.dstId.toString)
              }
              if (newList.nonEmpty) {
                newList = newList.map { s =>
                  var newString = ""
                  val splitString = s.split(",")
                  var writeDestination = true
                  for (vertex <- splitString) {
                    if (vertex == destination) writeDestination = false
                  }
                  if (writeDestination) {
                    newString = s + "," + destination
                  }
                  newString
                }
                val filteredList = newList.filter(_ != "")
                if (filteredList.nonEmpty) {
                  newMap += (outerValueMap._1 -> filteredList)
                }
              }
            }
          }
          if (newMap.nonEmpty) {
            Iterator((edge.dstId, (edge.srcAttr._1, newMap)))
          }
          else {
            Iterator.empty
          }
        }
      }
      else {
        Iterator.empty
      }
    }

    def messageCombiner(a: (Int, Map[VertexId, List[String]]),
                        b: (Int, Map[VertexId, List[String]])): (Int, Map[VertexId, List[String]]) = {

      val (level1, msg_map1) = a
      val (level2, msg_map2) = b

      if (level1 == level2) {
        (level1,
          (msg_map1.keySet ++ msg_map2.keySet).map { i =>
            val count1Val = msg_map1.getOrElse(i, List[String]())
            val count2Val = msg_map2.getOrElse(i, List[String]())
            if (count1Val.nonEmpty & count2Val.nonEmpty) {
              i -> (count1Val ::: count2Val)
            }
            else if (count1Val.isEmpty & count2Val.nonEmpty) {
              i -> count2Val
            }
            else {
              i -> count1Val
            }
          }(collection.breakOut)
        )
      }
      else if (level1 > level2) {
        a
      }
      else {
        b
      }
    }

    // The initial message received by all vertices in the preliminaryGraph
    val initialMessage = (0, Map[VertexId, List[String]]())

    // Execute a static version of Pregel.
    val vp = {
      (id: VertexId, value: (Int, Map[VertexId, List[String]], String, List[String]), msgSum: (Int, Map[VertexId, List[String]])) =>
        vertexProgram(id, value, msgSum)
    }

    val finalGraph = Pregel(preliminaryGraph, initialMessage, activeDirection = EdgeDirection.Out, maxIterations = length)(vp, sendMessage, messageCombiner)
    preliminaryGraph.unpersistVertices()
    preliminaryGraph.edges.unpersist()

    var listOfPaths = List[String]()
    finalGraph.vertices.filter({
      vertex => listOfDestinations.contains(vertex._1)
    }).map({ vertex => vertex._2._4 }).collect()
      // for each end vertex
      .foreach(list => {
        listOfPaths = listOfPaths ++ list
      })
    finalGraph.unpersistVertices()
    finalGraph.edges.unpersist()

    listOfPaths
  }

  /** *
   * This function is used in findAllPathsForPairs to extract necessary information from the List of pairs.
   *
   * @param listOfVertices a list which contains the pairs to be used in findAllPaths
   * @return a map with destination vertices as keys and as value a list of sources ,
   *         a list of sources used to send the initial message on Pregel ,
   *         a list of final destinations from all the pairs
   */
  private def fill(listOfVertices: List[(VertexId, VertexId)]): (Map[VertexId, List[VertexId]], List[VertexId], List[VertexId]) = {
    var newStartMap = Map[VertexId, List[VertexId]]()
    var newSetSources = mutable.Set[VertexId]()
    var newSetDestinations = mutable.Set[VertexId]()

    listOfVertices.foreach { k =>
      newSetSources += k._1
      newSetDestinations += k._2
      if (newStartMap.contains(k._1)) {
        val newInnerList = k._2 :: newStartMap(k._1)
        newStartMap += (k._1 -> newInnerList)
      }
      else {
        newStartMap += (k._1 -> List(k._2))
      }
    }
    (newStartMap, newSetSources.toList, newSetDestinations.toList)
  }

  /** *
   * This function writes the resulting paths to a user-defined output path.
   *
   * @param paths       the paths produced after findAllPathsForPairs
   * @param pathsOutput the output path where we will write the results of the findAllPathsForPairs algorithm
   */
  def writeResults(paths: List[String], pathsOutput: String): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    paths.toDF().coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("delimiter", "\t").save(path = pathsOutput)
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