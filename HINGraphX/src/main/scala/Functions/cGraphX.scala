package Functions
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.spark.graphx._
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.types.{LongType, MapType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object cGraphX extends Serializable {
  /***
   * This function creates the Initial Graph with vertices in nodes_dir folder and edges in relations_dir folder.
   * As each file corresponds to an entity type we take only the types listed in the metapath.
   * To create the vertices it reads all the vertex files, with user-defined delimiter, from nodes_dir folder
   * and creates a DataFrame with id, type, Map[String, String] columns where we have:
   * * id: The id of the vertex which may not be given as unique but is made unique internally as GraphX does not
   *   distinguish the ids based on type.
   * * type: A String corresponding to the file name which is the type of the vertex.
   * * Map[String, String] : Each column that is not named id and type is part of the attribute Map
   *   with the name of the column as key and its value in the corresponding row as value.
   *
   * Similarly, to create the edges it reads all the edge files, with user-defined delimiter, from relations_dir folder
   * and creates a DataFrame with src, dst, type, Map[String, String] columns where we have:
   * * src: The id of the src vertex which based on the src vertex type changes its value to its unique which is used internally.
   * * dst: The id of the dst vertex which based on the dst vertex type changes its value to its unique which is used internally.
   * * type: A String corresponding to the file name which is the type of the edge.
   * * Map[String, String] : Each column that is not named src, dst and type is part of the attribute Map
   *   with the name of the column as key and its value in the corresponding row as value.
   *
   * From verticesDF we create the VertexRDD and from edgesDF we create the EdgesRDD used to create the Graph.
   * Additionally, we return a Map[String, Long] which has vertex type as key and
   * the value added to each vertex of this type to create its unique id.
   *
   * @param nodes_dir The path where the nodes folder exists
   * @param relations_dir The path where the relations folder exists
   * @param delimiter The delimiter which will be used to read the csv files in the folders
   * @param typesInMetaPath An Array[String] with all the entity types.
   * @return (a map with keys as types (both vertex and edge) and as value its count, a map with has vertex types
   *         as keys and the value added to each vertex of this type to create its unique id, a graph, filled
   *         with the data whose path we have given, to be used in custom methods)
   */
  def createInputGraph(nodes_dir: String = "", relations_dir: String = "", delimiter: String = "\t",
                       typesInMetaPath: Array[String]): (Map[String, Long], Map[String, VertexId], Graph[(String, Map[String, String]),(String, Map[String, String])]) ={
    require(nodes_dir.nonEmpty, s"Path must not be empty")
    require(relations_dir.nonEmpty, s"Path must not be empty")
    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate
    import spark.implicits._

    // keep in map the (previous max - current min + 1) of previous dataFrame calculation or 0 (if no increase)
    // and compare it to the min of the next so as not to have conflicts in relations
    var mapGlobalIncrease: Map[String, VertexId] = Map[String, VertexId]()

    // a map with keys as types (both vertex and edge) and as value its count
    var mapCountVerticesEdges: Map[String, Long] = Map[String, Long]()
    var prMaxId = 0L
    var prMinId = 0L
    //Build vertices DataFrame
    var verticesDF: DataFrame = null
    val nodesHdfsFolder = getHdfs(nodes_dir)
    val inputNodesPath = new Path(nodes_dir)
    var findStatus = nodesHdfsFolder.getFileStatus(inputNodesPath)
    if (nodesHdfsFolder.exists(inputNodesPath) && findStatus.isDirectory)  {
      nodesHdfsFolder.listStatus(inputNodesPath)
        .toList
        .foreach(file => {
          if (file.getPath.getName.contains(".csv")) {
            val vType = file.getPath.getName.split("\\.")(0).trim
            if (typesInMetaPath.contains(vType)) {
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

              // take records count for this dataFrame with key as the file name
              mapCountVerticesEdges+= vType -> df.count()

              // take max of previous .csv and change it with max of current .csv

              val dfMinId = df.agg(min("id")).head().getLong(0)
              val dfMaxId = df.agg(max("id")).head().getLong(0)
              var increaseIdByValue = 0L

              // if max previous greater than current min then increase all number by (previous max - current min + 1)
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
            if (typesInMetaPath.contains(getType(0).toString) & typesInMetaPath.contains(getType(1).toString)) {
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

              // take records count for this dataFrame with key as the file name
              mapCountVerticesEdges+= vType -> df.count()

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
          }
        })
    }
    edgesHdfsFolder.close()

    // Build RDDs and Graph
    val verticesRDD = verticesDF.rdd.map(node => (node.getAs[Long]("id"), (node.getAs[String]("type"), node.getAs[Map[String, String]]("vAttr"))))
    verticesDF.unpersist()
    val edgesRDD = edgesDF.rdd.map(node => Edge(node.getAs[Long]("src"), node.getAs[Long]("dst"),(node.getAs[String]("type"), node.getAs[Map[String, String]]("eAttr"))))
    edgesDF.unpersist()
    val graph = Graph(verticesRDD, edgesRDD)
    verticesRDD.unpersist()
    edgesRDD.unpersist()

    // The edges create null vertices if they cannot find them, so we have to filter them out
    (mapCountVerticesEdges, mapGlobalIncrease, graph.filter(graph => graph, vpred = (_: VertexId, vData: (String, Map[String, String])) => vData != null))
  }

  /***
   * This function is used to determine whether it is better to send messages following
   * the original metapath or the reverse on based on the number of vertices and edges at both sides
   * and/or the existance of conditions in both vertices and edges at both sides.
   *
   * @param metaPath The metapath given by the user in the json file
   * @param mapCountVerticesEdges A map with keys the types of both vertices and edges
   *                              and values the number of those type of vertices and edges in the graph
   * @return a boolean that helps us decide which method to use in the creation of the 1-step graph (true -> reverse)
   */
  def chooseMethod(metaPath: String, mapCountVerticesEdges: Map[String, Long]): Boolean ={
    val typesInMetaPath = new ParseConditions(metaPath, false)
    val pathLength = typesInMetaPath.pathLength
    val mapNodeTypes = typesInMetaPath.mapNodeTypes
    val nodesWithConditions = typesInMetaPath.mapNodeConditions.keySet
    val edgesWithConditions = typesInMetaPath.mapEdgeConditions.keySet
    val evenOrOddLength = pathLength % 2

    // check if number of nodes is even or odd
    // for even if the position of the node is equal to pathLength/2.0 then add to extraIncrease 1
    // for odd if the position of the node is equal to (pathLength-1)/2.0 then add to extraIncrease 0.5
    // and if greater then add 1
    var extraIncrease = 0.0
    for (nodePosition <- nodesWithConditions){
      if(evenOrOddLength == 0){
        if(nodePosition >= pathLength/2.0){
          extraIncrease += 1.0
        }
      }
      else{
        if(nodePosition > (pathLength-1)/2.0){
          extraIncrease += 1.0
        }
        else if(nodePosition == (pathLength-1)/2.0){
          extraIncrease += 0.5
        }
      }
    }

    if(nodesWithConditions.nonEmpty & edgesWithConditions.nonEmpty) {
      val majorityOfConditions: Double = (((nodesWithConditions.sum + extraIncrease)/ nodesWithConditions.size) +
        ((edgesWithConditions.sum + edgesWithConditions.size) / edgesWithConditions.size))/2.0

      if (majorityOfConditions > pathLength/2.0){
        true
      }
      else if (majorityOfConditions < pathLength/2.0) {
        false
      }
      else{
        chooseMethodByCount(pathLength, mapNodeTypes, mapCountVerticesEdges)
      }
    }
    else if(nodesWithConditions.nonEmpty & edgesWithConditions.isEmpty) {
      val majorityOfConditions: Double = (nodesWithConditions.sum + extraIncrease) / nodesWithConditions.size

      if (majorityOfConditions > pathLength/2.0){
        true
      }
      else if (majorityOfConditions < pathLength/2.0) {
        false
      }
      else{
        chooseMethodByCount(pathLength, mapNodeTypes, mapCountVerticesEdges)
      }
    }
    else if (nodesWithConditions.isEmpty & edgesWithConditions.nonEmpty){
      val majorityOfConditions: Double = (edgesWithConditions.sum + edgesWithConditions.size) / edgesWithConditions.size

      if (majorityOfConditions > pathLength/2.0){
        true
      }
      else if (majorityOfConditions < pathLength/2.0) {
        false
      }
      else{
        chooseMethodByCount(pathLength, mapNodeTypes, mapCountVerticesEdges)
      }
    }
    else{
      chooseMethodByCount(pathLength, mapNodeTypes, mapCountVerticesEdges)
    }
  }

  /***
   *
   * @param pathLength The length of the metaPath
   * @param mapNodeTypes A map with values the type of nodes in the metaPath and keys their positions
   * @param mapCountVerticesEdges A map with keys the types of both vertices and edges
   *                              and values the number of those type of vertices and edges in the graph
   * @return a boolean that helps un decide which method to use in the creation of the 1-step graph (true -> reverse)
   */
  private def chooseMethodByCount(pathLength: Int, mapNodeTypes: Map[Int, String], mapCountVerticesEdges: Map[String, Long]): Boolean ={

    var countRecordsFirstHalf = 0L
    var countRecordsSecondHalf = 0L
    val evenOrOddLength = pathLength % 2

    // different calculation for even/odd metapath length
    if (evenOrOddLength == 0){ // even

      // find the number of vertices and edges in first half of the metaPath
      for (first <- 0 until (pathLength/2)){
        countRecordsFirstHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(first, ""), 0L)
        if (first < ((pathLength/2) - 1)){
          countRecordsFirstHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(first, "") + mapNodeTypes.getOrElse(first + 1, ""), 0L)
        }
      }

      // find the number of vertices and edges in second half of the metaPath
      for (second <- (pathLength/2)  until pathLength){
        countRecordsSecondHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(second, ""), 0L)
        if (second < (pathLength - 1)){
          countRecordsSecondHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(second, "") + mapNodeTypes.getOrElse(second + 1, ""), 0L)

        }

      }
    }
    else{ // odd
      // find the number of vertices and edges in first half of the metaPath
      for (first <- 0 until ((pathLength/2) + 1)){
        countRecordsFirstHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(first, ""), 0L)
        if (first < (pathLength/2)){
          countRecordsFirstHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(first, "") + mapNodeTypes.getOrElse(first + 1, ""), 0L)
        }
      }

      // find the number of vertices and edges in second half of the metaPath
      for (second <- (pathLength/2) until pathLength){
        countRecordsSecondHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(second, ""), 0L)
        if (second < (pathLength - 1)){
          countRecordsSecondHalf += mapCountVerticesEdges.getOrElse(mapNodeTypes.getOrElse(second, "") + mapNodeTypes.getOrElse(second + 1, ""), 0L)
        }
      }
    }

    if (countRecordsSecondHalf < countRecordsFirstHalf){
      return true
    }
    false
  }

  /***
   * This function uses the input Graph and makes use of GraphX's Pregel to send messages from all its vertices to
   * each of their neighbors. In the beginning, the active vertices are those of the first entity type in the metapath.
   * In each iteration each active vertex that is connected through an outgoing edge with another checks first
   * whether the edge itself satisfies all the constraints (if there are any), then whether its neighbor has the same
   * type as the next type in the metapath and finally whether its neighbor satisfies all the constraints (if there are any).
   * If the previous conditions are met then and only then the vertex sends a message towards that particular neighbor through
   * that edge. Each vertex that receives at least one message from its neighbors combines the values of those messages if
   * there are more than one and takes a Map[VertexId, Long] with the ids corresponding to vertices that sent the first
   * message (those of the first entity type in the metapath) as keys and number of paths that exist from them to this vertex
   * as value. The number of iterations is the same as the length of the user-defined metapath.
   * After the final iteration we filter the vertices that are of the last entity type in the metapath and have received
   * a message in the final iteration. Each of those vertices has a Map[VertexId, Long] where each element
   * contains as key the id of a vertex of the first entity type in the metapath and as values the number of paths connecting
   * the first entity type vertex (key) with the vertex that houses the Map. We iterate every Map and for each of the
   * (key, value) pairs we create an edge with the key as the src, the vertex that houses the Map as the dst and the value
   * as edge attribute which we append in a List. We create the transformed Graph that has as vertices only those
   * listed either as source or as destination in the edges on the list and as edges the edges on the list.
   *
   * @param graph This graph is produced by the method createInputGraph
   * @param metaPath The metapath given by the user in the json file
   * @param mapGlobalIncrease how much the id, based on the Type of vertex,
   *                          had to be increased to transform into unique id
   * @return a graph much smaller from the original with only start/end Type vertices as src/dst
   *         and the number of different metapaths from the src to the dst following the given metapath
   */
  def createMetaPathGraph( graph: Graph[(String, Map[String, String]), (String, Map[String, String])],
                           metaPath: String, mapGlobalIncrease: Map[String, VertexId]): VertexRDD[Map[VertexId, Long]] ={

    // Initialize the preliminaryGraph
    val preliminaryGraph: Graph[(Int, String, Map[String, String], Map[VertexId, Long], Long), (Int, String, Map[String, String], Long)] = graph
      // Start with level 0, Type Vertex_Type, Map of vertex attributes and number of paths 0 in each vertex
      .mapVertices {
        case (_, vData) => (0, vData._1, vData._2, Map[VertexId, Long](), 0L)
      }
      // Set the level 0, the edge type, the Map of edge attributes and number of paths 0 in each edge
      .mapTriplets(edge => (0,edge.attr._1,edge.attr._2, 0L)).cache()
    graph.unpersistVertices()
    graph.edges.unpersist()
    // Take the reverse metaPath and its conditions
    val typesAndConditions = new ParseConditions(metaPath, false)
    val pathLength = typesAndConditions.pathLength

    // Define the three functions needed to implement the creation of the Graph
    // in the GraphX version of Pregel.
    def vertexProgram(id: VertexId, value: (Int, String, Map[String,String], Map[VertexId, Long], Long),
                      msgSum: (Int, Map[VertexId, Long], Long)): (Int, String, Map[String,String], Map[VertexId, Long], Long) = {
      val (level, vType, vMap, vFinalMap, counter) = value
      val (msgLevel, msgMap, msgValue) = msgSum
      val temp = java.lang.Math.floorMod(msgLevel, pathLength)
      if (msgLevel == 0 & vType.equals(typesAndConditions.mapNodeTypes(0))){
        // check if first node of the metapath has constraints
        if(typesAndConditions.mapNodeHasConditions(0)){
          //if it has constraints check if it meets them
          if (checkConditions(id - mapGlobalIncrease(vType), vMap, typesAndConditions.mapNodeConditions(0))){
            (1, vType, vMap, vFinalMap, 0L)
          }
          else{ // if it doesn't meet the conditions its level == 0 and it doesn't send any messages
            (level, vType, vMap, vFinalMap, counter)
          }
        }
        else{
          (1, vType, vMap, vFinalMap, 0L)
        }
      }
      // if msgLevel > 0 then it meets the conditions because it wouldn't have received a message otherwise
      else if (0 < msgLevel & temp < (pathLength - 1) & level <= msgLevel){
        (msgLevel + 1, vType, vMap, msgMap, msgValue)
      }
      else if (0 < msgLevel & temp == (pathLength - 1) & level <= msgLevel){
        (pathLength, vType, vMap, msgMap,  msgValue)
      }
      else{
        (-1, vType, vMap, Map[VertexId, Long](), 0L)
      }
    }

    def sendMessage(edge: EdgeTriplet[(Int, String, Map[String,String], Map[VertexId, Long], Long),
      (Int, String, Map[String,String], Long)]): Iterator[(VertexId, (Int, Map[VertexId, Long], Long))] = {

      val temp = java.lang.Math.floorMod(edge.srcAttr._1, pathLength)
      // check and if dstLevel == 0 then don't send any messages
      if (edge.srcAttr._1 == 0 | edge.srcAttr._1 == -1 | edge.dstAttr._1 >= edge.srcAttr._1) {
        Iterator.empty
      }
      else if( 0 < edge.srcAttr._1 & edge.srcAttr._1 < pathLength){
        //check whether or not the edge conditions, if they exist, are met
        if (typesAndConditions.mapEdgeHasConditions(temp - 1)){
          if (!checkEdgeConditions(edge.attr._2, edge.attr._3, typesAndConditions.mapEdgeConditions(temp - 1))){
            return Iterator.empty
          }
        }

        if (temp == 1){
          //check if the src node conditions are met and if they are send a message
          if (edge.srcAttr._2.equals(typesAndConditions.mapNodeTypes(temp - 1)) & edge.dstAttr._2.equals(typesAndConditions.mapNodeTypes(temp))) {
            if (typesAndConditions.mapNodeHasConditions(temp)){
              if (checkConditions(edge.dstId - mapGlobalIncrease(edge.dstAttr._2), edge.dstAttr._3, typesAndConditions.mapNodeConditions(temp))){
                Iterator((edge.dstId, (edge.srcAttr._1, Map(edge.srcId -> 1L), 1L)), (edge.srcId, (-1, Map[VertexId, Long](), 0L)))
              }
              else{
                Iterator.empty
              }
            }
            else{
              Iterator((edge.dstId, (edge.srcAttr._1, Map(edge.srcId -> 1L), 1L)), (edge.srcId, (-1, Map[VertexId, Long](), 0L)))
            }
          }
          else {
            Iterator.empty
          }
        }
        else {
          if (edge.srcAttr._2.equals(typesAndConditions.mapNodeTypes(temp - 1)) & edge.dstAttr._2.equals(typesAndConditions.mapNodeTypes(temp))) {
            if (typesAndConditions.mapNodeHasConditions(temp)){
              if (checkConditions(edge.dstId - mapGlobalIncrease(edge.dstAttr._2), edge.dstAttr._3, typesAndConditions.mapNodeConditions(temp))){
                Iterator((edge.dstId, (edge.srcAttr._1, edge.srcAttr._4, edge.srcAttr._5)), (edge.srcId, (-1, Map[VertexId, Long](), 0L)))
              }
              else{
                Iterator.empty
              }
            }
            else{
              Iterator((edge.dstId, (edge.srcAttr._1, edge.srcAttr._4, edge.srcAttr._5)), (edge.srcId, (-1, Map[VertexId, Long](), 0L)))
            }
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

    def messageCombiner(a: (Int, Map[VertexId, Long], Long),
                        b: (Int, Map[VertexId, Long], Long)): (Int, Map[VertexId, Long], Long) = {

      val (level1, msgMap1, counter1) = a
      val (level2, msgMap2, counter2) = b

      if(level1 == level2){
        if(level1 == -1){
          a
        }
        else {
          (level1,
            (msgMap1.keySet ++ msgMap2.keySet).map { i =>
              val count1Val = msgMap1.getOrElse(i, 0L)
              val count2Val = msgMap2.getOrElse(i, 0L)
              i -> (count1Val + count2Val)
            }(collection.breakOut),
            counter1 + counter2)
        }
      }
      else if(level1 > level2){
        a
      }
      else{
        b
      }
    }

    // The initial message received by all vertices in the preliminaryGraph
    val initialMessage = (0, Map[VertexId, Long](), 0L)

    // Execute a static version of Pregel.
    val vp = {
      (id: VertexId, value: (Int, String, Map[String,String], Map[VertexId, Long], Long), msgSum: (Int, Map[VertexId, Long], Long)) =>
        vertexProgram(id, value, msgSum)
    }

    // We follow the reverse metaPath so as to send the final results to the start-Type vertex of the metaPath
    val metaPathGraph = Pregel(preliminaryGraph, initialMessage, maxIterations = pathLength - 1, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapTriplets(edge => edge.attr._2)
      .mapVertices { case (_, attr) =>
        (attr._1, attr._4)
      }

    preliminaryGraph.unpersistVertices()
    preliminaryGraph.edges.unpersist()
    metaPathGraph.edges.unpersist()

    val vertices = metaPathGraph.vertices.filter({
      vertex => vertex._2._1 == pathLength
    }).mapValues{vertex => vertex._2}

    metaPathGraph.unpersistVertices()

    vertices
  }

  /***
   * This function uses the input Graph and makes use of GraphX's Pregel to send messages from all its vertices to
   * each of their neighbors. In the beginning, the active vertices are those of the last entity type in the metapath.
   * In each iteration each active vertex that is connected through an incoming edge with another checks first
   * whether the edge itself satisfies all the constraints (if there are any), then whether its neighbor has the same
   * type as the next type in the metapath and finally whether its neighbor satisfies all the constraints (if there are any).
   * If the previous conditions are met then and only then the vertex sends a message towards that particular neighbor through
   * that edge. Each vertex that receives at least one message from its neighbors combines the values of those messages if
   * there are more than one and takes a Map[VertexId, Long] with the ids corresponding to vertices that sent the first
   * message (those of the last entity type in the metapath) as keys and number of paths that exist from them to this vertex
   * as value. The number of iterations is the same as the length of the user-defined metapath.
   * After the final iteration we filter the vertices that are of the first entity type in the metapath and have received
   * a message in the final iteration. Each of those vertices has a Map[VertexId, Long] where each element
   * contains as key the id of a vertex of the last entity type in the metapath and as values the number of paths connecting
   * the vertex that houses the Map with the last entity type vertex (key). We iterate every Map and for each of the
   * (key, value) pairs we create an edge with the vertex that houses the Map as the src, the key as the dst and the value
   * as edge attribute which we append in a List. We create the transformed Graph that has as vertices only those
   * listed either as source or as destination in the edges on the list and as edges the edges on the list.
   *
   * @param graph This graph is produced by the method createInputGraph
   * @param metaPath The metapath given by the user in the json file
   * @param mapGlobalIncrease how much the id, based on the Type of vertex,
   *                          had to be increased to transform into unique id
   * @return a graph much smaller from the original with only start/end Type vertices as src/dst
   *         and the number of different metapaths from the src to the dst following the given metapath
   */
  def createMetaPathGraphInReverse( graph: Graph[(String, Map[String, String]), (String, Map[String, String])],
                           metaPath: String, mapGlobalIncrease: Map[String, VertexId]): VertexRDD[Map[VertexId, Long]] ={

    // Initialize the preliminaryGraph
    val preliminaryGraph: Graph[(Int, String, Map[String, String], Map[VertexId, Long], Long), (Int, String, Map[String, String], Long)] = graph
      // Start with level 0, Type Vertex_Type, Map of vertex attributes and number of paths 0 in each vertex
      .mapVertices {
        case (_, vData) => (0, vData._1, vData._2, Map[VertexId, Long](), 0L)
      }
      // Set the level 0, the edge type, the Map of edge attributes and number of paths 0 in each edge
      .mapTriplets(edge => (0,edge.attr._1,edge.attr._2, 0L)).cache()
    graph.unpersistVertices()
    graph.edges.unpersist()

    // Take the reverse metaPath and its conditions
    val typesAndConditions = new ParseConditions(metaPath, true)
    val pathLength = typesAndConditions.pathLength

    // Define the three functions needed to implement the creation of the Graph
    // in the GraphX version of Pregel.
    def vertexProgram(id: VertexId, value: (Int, String, Map[String,String], Map[VertexId, Long], Long),
                      msgSum: (Int, Map[VertexId, Long], Long)): (Int, String, Map[String,String], Map[VertexId, Long], Long) = {
      val (level, vType, vMap, vFinalMap, counter) = value
      val (msgLevel, msgMap, msgValue) = msgSum
      val temp = java.lang.Math.floorMod(msgLevel, pathLength)

      if (msgLevel == 0 & vType.equals(typesAndConditions.mapNodeTypes(0))){
        // check if first node of the metapath has constraints
        if(typesAndConditions.mapNodeHasConditions(0)){
          //if it has constraints check if it meets them
          if (checkConditions(id - mapGlobalIncrease(vType), vMap, typesAndConditions.mapNodeConditions(0))){
            (1, vType, vMap, vFinalMap, 0L)
          }
          else{ // if it doesn't meet the conditions its level == 0 and it doesn't send any messages
            (level, vType, vMap, vFinalMap, counter)
          }
        }
        else{
          (1, vType, vMap, vFinalMap, 0L)
        }
      }
      // if msgLevel > 0 then it meets the conditions because it wouldn't have received a message otherwise
      else if (0 < msgLevel & temp < (pathLength - 1) & level <= msgLevel){
        (msgLevel + 1, vType, vMap, msgMap, msgValue)
      }
      else if (0 < msgLevel & temp == (pathLength - 1) & level <= msgLevel){
        (pathLength, vType, vMap, msgMap,  msgValue)
      }
      else{
        (-1, vType, vMap, Map[VertexId, Long](), 0L)
      }
    }

    def sendMessage(edge: EdgeTriplet[(Int, String, Map[String,String], Map[VertexId, Long], Long),
      (Int, String, Map[String,String], Long)]): Iterator[(VertexId, (Int, Map[VertexId, Long], Long))] = {

      val temp = java.lang.Math.floorMod(edge.dstAttr._1, pathLength)
      // check and if dstLevel == 0 then don't send any messages
      if (edge.dstAttr._1 == 0 | edge.dstAttr._1 == -1 | edge.srcAttr._1 >= edge.dstAttr._1) {
        Iterator.empty
      }
      else if( 0 < edge.dstAttr._1 & edge.dstAttr._1 < pathLength){
        //check whether or not the edge conditions, if they exist, are met
        if (typesAndConditions.mapEdgeHasConditions(temp - 1)){
          if (!checkEdgeConditions(edge.attr._2, edge.attr._3, typesAndConditions.mapEdgeConditions(temp - 1))){
            return Iterator.empty
          }
        }

        if (temp == 1){
          //check if the src node conditions are met and if they are send a message
          if (edge.dstAttr._2.equals(typesAndConditions.mapNodeTypes(temp - 1)) & edge.srcAttr._2.equals(typesAndConditions.mapNodeTypes(temp))) {
            if (typesAndConditions.mapNodeHasConditions(temp)){
              if (checkConditions(edge.srcId - mapGlobalIncrease(edge.srcAttr._2), edge.srcAttr._3, typesAndConditions.mapNodeConditions(temp))){
                Iterator((edge.srcId, (edge.dstAttr._1, Map(edge.dstId -> 1L), 1L)), (edge.dstId, (-1, Map[VertexId, Long](), 0L)))
              }
              else{
                Iterator.empty
              }
            }
            else{
              Iterator((edge.srcId, (edge.dstAttr._1, Map(edge.dstId -> 1L), 1L)), (edge.dstId, (-1, Map[VertexId, Long](), 0L)))
            }
          }
          else {
            Iterator.empty
          }
        }
        else {
          if (edge.dstAttr._2.equals(typesAndConditions.mapNodeTypes(temp - 1)) & edge.srcAttr._2.equals(typesAndConditions.mapNodeTypes(temp))) {
            if (typesAndConditions.mapNodeHasConditions(temp)){
              if (checkConditions(edge.srcId - mapGlobalIncrease(edge.srcAttr._2), edge.srcAttr._3, typesAndConditions.mapNodeConditions(temp))){
                Iterator((edge.srcId, (edge.dstAttr._1, edge.dstAttr._4, edge.dstAttr._5)), (edge.dstId, (-1, Map[VertexId, Long](), 0L)))
              }
              else{
                Iterator.empty
              }
            }
            else{
              Iterator((edge.srcId, (edge.dstAttr._1, edge.dstAttr._4, edge.dstAttr._5)), (edge.dstId, (-1, Map[VertexId, Long](), 0L)))
            }
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

    def messageCombiner(a: (Int, Map[VertexId, Long], Long),
                        b: (Int, Map[VertexId, Long], Long)): (Int, Map[VertexId, Long], Long) = {

      val (level1, msgMap1, counter1) = a
      val (level2, msgMap2, counter2) = b

      if(level1 == level2){
        if(level1 == -1){
          a
        }
        else {
          (level1,
            (msgMap1.keySet ++ msgMap2.keySet).map { i =>
              val count1Val = msgMap1.getOrElse(i, 0L)
              val count2Val = msgMap2.getOrElse(i, 0L)
              i -> (count1Val + count2Val)
            }(collection.breakOut),
            counter1 + counter2)
        }
      }
      else if(level1 > level2){
        a
      }
      else{
        b
      }
    }

    // The initial message received by all vertices in the preliminaryGraph
    val initialMessage = (0, Map[VertexId, Long](), 0L)

    // Execute a static version of Pregel.
    val vp = {
      (id: VertexId, value: (Int, String, Map[String,String], Map[VertexId, Long], Long), msgSum: (Int, Map[VertexId, Long], Long)) =>
        vertexProgram(id, value, msgSum)
    }

    // We follow the reverse metaPath so as to send the final results to the start-Type vertex of the metaPath
    val metaPathGraph = Pregel(preliminaryGraph, initialMessage, maxIterations = pathLength - 1, activeDirection = EdgeDirection.In)(
      vp, sendMessage, messageCombiner)
      .mapTriplets(edge => edge.attr._2)
      .mapVertices { case (_, attr) =>
        (attr._1, attr._4)
      }

    preliminaryGraph.unpersistVertices()
    preliminaryGraph.edges.unpersist()
    metaPathGraph.edges.unpersist()

    val vertices = metaPathGraph.vertices.filter({
      vertex => vertex._2._1 == pathLength
    }).mapValues{vertex => vertex._2}

    metaPathGraph.unpersistVertices()

    vertices
  }

  /***
   *
   *
   * @param vertices The vertices of the graph produced by createMetaPathGraph or createMetaPathGraphInReverse
   * @param pathLength The length of the metaPath
   * @return a graph much smaller from the original with only start/end Type vertices as src/dst
   *         and the number of different metapaths from the src to the dst following the given metapath
   */
  def createSinglesGraph(vertices: VertexRDD[Map[VertexId, Long]],
                                  pathLength: Int, runReverseTransformation: Boolean): Graph[Long, Long] ={

    // empty List where we will put the edges of the new Graph
    var lstEdges = new ListBuffer[Edge[Long]]()
    // empty Set where we will put the vertices of the new Graph
    var setVertices = mutable.Set[VertexId]()


    val filteredVRDD = vertices.cache()

    val parts = filteredVRDD.partitions
    for(p <- parts){
      val idx = p.index
      val partRDD = filteredVRDD.mapPartitionsWithIndex((index: Int, it: Iterator[(VertexId, Map[VertexId, Long])]) => if(index == idx) it else Iterator(), preservesPartitioning = true)
      val data = partRDD.collect()
      data.foreach(vertex =>{
        // include in the set this start vertex
        setVertices += vertex._1
        for( (k, v) <- vertex._2){
          // include in the set this end vertex
          setVertices += k
          // include in the List this Edge with attribute number of paths, (number of paths from start->end)/(number of paths from start)
          // different edge directions based on the method that has been used
          if (runReverseTransformation){
            lstEdges += Edge(vertex._1, k, v)
          }
          else{
            lstEdges += Edge(k, vertex._1, v)
          }
        }
      })
      partRDD.unpersist()
    }
    filteredVRDD.unpersist()

    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate

    val edgesRDD = spark.sparkContext.parallelize(lstEdges.toList)
    val transformedGraph = Graph.fromEdges(edgesRDD, -1L)
    edgesRDD.unpersist()

    transformedGraph
  }

  /***
   *
   * @param edgeType the type of edge in the graph
   * @param edgeConditions the conditions of the edge in the graph
   * @param conditions the conditions of the edge in the metaPath
   * @return a boolean that explains if this particular edge meets all the conditions we are looking for
   */
  private def checkEdgeConditions(edgeType: String, edgeConditions: Map[String, String], conditions: List[String]): Boolean ={

    var keepAllConditionsExceptType = Set[String]()

    conditions.foreach{ condition =>
      if (condition.trim.toLowerCase.startsWith("type")){
        val checkTypeList = condition.trim.split("'").map(_.trim)
        if (checkTypeList(0).contains("==")){
          if (edgeType != checkTypeList(1)){
            return false
          }
        }
        else if(checkTypeList(0).contains("!=")){
          if (edgeType == checkTypeList(1)){
            return false
          }
        }
        else if(checkTypeList(0).contains(">=")){
          if (!(edgeType >= checkTypeList(1))){
            return false
          }
        }
        else if(checkTypeList(0).contains("<=")){
          if (!(edgeType <= checkTypeList(1))){
            return false
          }
        }
        else if(checkTypeList(0).contains(">")){
          if (!(edgeType > checkTypeList(1))){
            return false
          }
        }
        else if(checkTypeList(0).contains("<")){
          if (!(edgeType < checkTypeList(1))){
            return false
          }
        }
      }
      else{
        keepAllConditionsExceptType += condition
      }
    }

    if (keepAllConditionsExceptType.isEmpty){
      true
    }
    else{
      checkConditions(0, edgeConditions, keepAllConditionsExceptType.toList)
    }
  }

  /***
   *
   * @param id the id of the vertex that will be tested, if the conditions include "id" in them.
   * @param nodeConditions the conditions of the node/edge in the graph
   * @param conditions the conditions of the node/edge in the metaPath
   * @return a boolean that explains if this particular node/edge meets all the conditions we are looking for
   */
  private def checkConditions(id: VertexId, nodeConditions: Map[String, String], conditions: List[String]): Boolean ={
    for (condition <- conditions){
      if (condition.contains("==")){
        val tempList = condition.split("==").map(_.trim)
        if (tempList(1).contains("'")){
          if (nodeConditions.getOrElse(tempList(0), "") != tempList(1).substring(1, tempList(1).length() - 1).trim){
            return false
          }
        }
        else{
          if (tempList(0).toLowerCase == "id"){
            if (id != tempList(1).toLong){
              return false
            }
          }
          else{
            if(nodeConditions.getOrElse(tempList(0), null) != null & nodeConditions.getOrElse(tempList(0), null) != "nan") {
              if (nodeConditions.getOrElse(tempList(0), 0.0).toString.toDouble != tempList(1).toDouble) {
                return false
              }
            }
            else{
              return false
            }
          }
        }
      }
      else if(condition.contains("!=")){
        val tempList = condition.split("!=").map(_.trim)
        if (tempList(1).contains("'")){
          if (nodeConditions.getOrElse(tempList(0), "") == tempList(1).substring(1, tempList(1).length() - 1).trim){
            return false
          }
        }
        else {
          if (tempList(0).toLowerCase == "id") {
            if (id == tempList(1).toLong) {
              return false
            }
          }
          else {
            if(nodeConditions.getOrElse(tempList(0), null) != null & nodeConditions.getOrElse(tempList(0), null) != "nan") {
              if (nodeConditions.getOrElse(tempList(0), 0.0).toString.toDouble == tempList(1).toDouble) {
                return false
              }
            }
            else{
              return false
            }
          }
        }
      }
      else if(condition.contains(">=")){
        val tempList = condition.split(">=").map(_.trim)
        if (tempList(1).contains("'")){
          if (!(nodeConditions.getOrElse(tempList(0), "") >= tempList(1).substring(1, tempList(1).length() - 1).trim)){
            return false
          }
        }
        else {
          if (tempList(0).toLowerCase == "id") {
            if (!(id >= tempList(1).toLong)) {
              return false
            }
          }
          else {
            if(nodeConditions.getOrElse(tempList(0), null) != null & nodeConditions.getOrElse(tempList(0), null) != "nan") {
              if (!(nodeConditions.getOrElse(tempList(0), 0.0).toString.toDouble >= tempList(1).toDouble)) {
                return false
              }
            }
            else{
              return false
            }
          }
        }
      }
      else if(condition.contains("<=")){
        val tempList = condition.split("<=").map(_.trim)
        if (tempList(1).contains("'")){
          if (!(nodeConditions.getOrElse(tempList(0), "") <= tempList(1).substring(1, tempList(1).length() - 1).trim)){
            return false
          }
        }
        else {
          if (tempList(0).toLowerCase == "id") {
            if (!(id <= tempList(1).toLong)) {
              return false
            }
          }
          else {
            if(nodeConditions.getOrElse(tempList(0), null) != null & nodeConditions.getOrElse(tempList(0), null) != "nan") {
              if (!(nodeConditions.getOrElse(tempList(0), 0.0).toString.toDouble <= tempList(1).toDouble)) {
                return false
              }
            }
            else{
              return false
            }
          }
        }
      }
      else if(condition.contains(">")){
        val tempList = condition.split(">").map(_.trim)
        if (tempList(1).contains("'")){
          if (!(nodeConditions.getOrElse(tempList(0), "") > tempList(1).substring(1, tempList(1).length() - 1).trim)){
            return false
          }
        }
        else {
          if (tempList(0).toLowerCase == "id") {
            if (!(id > tempList(1).toLong)) {
              return false
            }
          }
          else {
            if(nodeConditions.getOrElse(tempList(0), null) != null & nodeConditions.getOrElse(tempList(0), null) != "nan") {
              if (!(nodeConditions.getOrElse(tempList(0), 0.0).toString.toDouble > tempList(1).toDouble)) {
                return false
              }
            }
            else{
              return false
            }
          }
        }
      }
      else if(condition.contains("<")){
        val tempList = condition.split("<").map(_.trim)
        if (tempList(1).contains("'")){
          if (!(nodeConditions.getOrElse(tempList(0), "") < tempList(1).substring(1, tempList(1).length() - 1).trim)){
            return false
          }
        }
        else {
          if (tempList(0).toLowerCase == "id") {
            if (!(id < tempList(1).toLong)) {
              return false
            }
          }
          else {
            if(nodeConditions.getOrElse(tempList(0), null) != null & nodeConditions.getOrElse(tempList(0), null) != "nan") {
              if (!(nodeConditions.getOrElse(tempList(0), 0.0).toString.toDouble < tempList(1).toDouble)) {
                return false
              }
            }
            else{
              return false
            }
          }
        }
      }
      else{
        throw new Exception("Conditions can contain only ==, !=, >=, <=, >, <")
      }
    }
    true
  }

  /***
   * This function writes the edges of the transformed Graph to a user-defined output path.
   * The src and dst ids are transformed from their unique id back to their original id.
   *
   * @param graph               the transformed graph
   * @param outputDirectoryPath the output path where we will write the graph vertices and edges
   * @param metaPath            the metaPath that has to be followed, given by the user
   * @param mapGlobalIncrease   how much the id, based on the Type of vertex,
   *                            had to be increased to transform into a unique id
   *
   */
  def writeOutput(graph: Graph[Long, Long],
                  outputDirectoryPath: String, metaPath: String, mapGlobalIncrease: Map[String, VertexId]): Unit ={

    // Take the vertices and the length of the metaPath
    val typesAndConditions = new ParseConditions(metaPath, false)
    val pathLength = typesAndConditions.pathLength

    // Find the start and end vertex of the metaPath
    val startVertex = typesAndConditions.mapNodeTypes(0)
    val endVertex = typesAndConditions.mapNodeTypes(pathLength - 1)
    // the number that each vertex ( start and end ) must decrease to take their original id
    val startTypeDecrease = mapGlobalIncrease(startVertex)
    val endTypeDecrease = mapGlobalIncrease(endVertex)

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    // discard edges where srcId == dstId and create dataFrame from graph
    var df = graph.edges.filter(edge => edge.srcId != edge.dstId).map { case Edge(src, dst, counter) => (src, dst, counter) }
      .toDF("src", "dst", "numberOfPaths").sort("src")

    df = df.withColumn("src", expr("src - " + startTypeDecrease).cast(LongType))
    df = df.withColumn("dst", expr("dst - " + endTypeDecrease).cast(LongType))

    df.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true")
      .option("delimiter", "\t").save(path = outputDirectoryPath)
  }

  /***
   * This function reads the metapath in json form and transforms it into a different form
   * that is later used to extract information about the vertices, the edges and their conditions.
   *
   * @param jsonMetapath the metaPath that has to be followed, given by the json file
   * @return the transformed metaPath to be used in the creation of the transformed graph
   */
  def readFromJson(jsonMetapath: String): String = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    val df = spark.read.json(Seq(jsonMetapath).toDS)
    val mappingSchema = MapType(StringType, StringType)
    val metapathList = df.select("metapath").head.getString(0).toArray[Char]
    var transformedMetapath = ""
    var counter = 0
    if (df.columns.contains("constraints")){
      val constraintsMap = df.select(from_json(to_json($"constraints"), mappingSchema).as("constraints")).collect().head.get(0).asInstanceOf[Map[String, String]]
      for (vType <- metapathList){
        transformedMetapath += vType + "["
        val getConstraints = constraintsMap.getOrElse(vType.toString, "")
        if (getConstraints.nonEmpty){
          if(getConstraints.contains("and")){
            val constraints = getConstraints.split("and").map(_.trim).toList
            val first = constraints.take(1).head
            transformedMetapath += first
            for(constraint <- constraints.takeRight(constraints.length - 1)){
              transformedMetapath += ";" + constraint
            }
          }
          else{
            transformedMetapath += getConstraints
          }
        }

        if (counter < metapathList.length - 1){
          transformedMetapath += "]-()-"
        }
        else{
          transformedMetapath += "]"
        }
        counter += 1
      }
    }
    else{
      for (vType <- metapathList){
        transformedMetapath += vType
        if (counter < metapathList.length - 1){
          transformedMetapath += "-()-"
        }
        counter += 1
      }
    }
    transformedMetapath
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