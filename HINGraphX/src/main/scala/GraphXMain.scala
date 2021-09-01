import org.apache.spark.{SparkConf, SparkContext}
import Functions.cGraphX._
import Functions.ParseConditions
import org.apache.spark.graphx._

object GraphXMain {
  def main(args: Array[String]): Unit = {

    val jsonString = os.read(os.Path(args(0)))
    val data = ujson.read(jsonString)

    val nodes_dir = data("indir").str
    val relations_dir = data("irdir").str
    val inputCSVDelimiter = data("inputCSVDelimiter").str
    val hin_out = data("hin_out").str
    val query = data("query").toString()

    // Read Json and Create metaPath
    val metaPathWithConstraints = readFromJson(query)

    // Array with the types in the metaPath
    val typesInMetaPath = new ParseConditions(metaPathWithConstraints, false).mapNodeTypes.values.toSet.toArray
    // Create the original graph
    val (mapCountVerticesEdges, mapGlobalIncrease, graph) = createInputGraph(nodes_dir, relations_dir, inputCSVDelimiter, typesInMetaPath)
    // Choose between regular or reverse version
    val runReverseTransformation = chooseMethod(metaPathWithConstraints, mapCountVerticesEdges)

    // Create the transformed graph following the given metaPath
    var newGraph: Graph[String, Long] = null
    // The previously computed boolean chooses which to execute
    if (runReverseTransformation){
      newGraph = createMetaPathGraphInReverse(graph, metaPathWithConstraints, mapGlobalIncrease)
    }
    else{
      newGraph = createMetaPathGraph(graph, metaPathWithConstraints, mapGlobalIncrease)
    }

    // Write in the output directory
    writeOutput(newGraph, hin_out, metaPathWithConstraints, mapGlobalIncrease)
  }
}
