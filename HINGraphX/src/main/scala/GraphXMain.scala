import Functions.cGraphX._
import Functions.ParseConditions
import org.apache.spark.graphx._

object GraphXMain {
  def main(args: Array[String]): Unit = {

    val jsonString = os.read(os.Path(args(0)))
    val data = ujson.read(jsonString)

    val nodes_dir = data("nodes_dir").str
    val relations_dir = data("relations_dir").str
    val inputCSVDelimiter = data("inputCSVDelimiter").str
    val hin_out = data("hin_out").str
    val query = data("query").toString()

    println("HIN Transformation\t1\tRead and Process Metapath Query")
    // Read Json and Create metapath
    val metaPathWithConstraints = readFromJson(query)

    // Array with the types in the metaPath and the length of the metaPath
    val parseMetaPath = new ParseConditions(metaPathWithConstraints, false)
    val typesInMetaPath = parseMetaPath.mapNodeTypes.values.toSet.toArray
    val length = parseMetaPath.pathLength

    println("HIN Transformation\t2\tInitializing Algorithm")
    // Create the original graph
    val (mapCountVerticesEdges, mapGlobalIncrease, graph) = createInputGraph(nodes_dir, relations_dir, inputCSVDelimiter, typesInMetaPath)
    // Choose between regular or reverse version
    val runReverseTransformation = chooseMethod(metaPathWithConstraints, mapCountVerticesEdges)

    // Create the transformed graph following the given metaPath
    var verticesOfTGraph : VertexRDD[Map[VertexId, Long]] = null
    // The previously computed boolean chooses which to execute
    println("HIN Transformation\t3\tExecute HIN Transformation algorithm")
    if (runReverseTransformation){
      verticesOfTGraph = createMetaPathGraphInReverse(graph, metaPathWithConstraints, mapGlobalIncrease)
    }
    else{
      verticesOfTGraph = createMetaPathGraph(graph, metaPathWithConstraints, mapGlobalIncrease)
    }
    val transformedGraph = createSinglesGraph(verticesOfTGraph, length, runReverseTransformation)

    // Write in the output directory
    writeOutput(transformedGraph, hin_out, metaPathWithConstraints, mapGlobalIncrease)
  }
}
