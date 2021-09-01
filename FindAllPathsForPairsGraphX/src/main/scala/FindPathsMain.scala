import org.apache.spark.{SparkConf, SparkContext}
import Functions.functions._

object FindPathsMain {
  def main(args: Array[String]): Unit = {

    val jsonString = os.read(os.Path(args(0)))
    val data = ujson.read(jsonString)

    val nodes_dir = data("indir").str
    val relations_dir = data("irdir").str
    val inputCSVDelimiter = data("inputCSVDelimiter").str
    val findAllPaths_out = data("path_searching_out").str
    val findAllForPairs = data("path_searching_pairs").str
    val length = data("path_searching_length").num.toInt

    val (mapGlobalIncrease, graph) = createInputGraph(nodes_dir, relations_dir, inputCSVDelimiter)

    val inputList = createPairList(findAllForPairs, mapGlobalIncrease, inputCSVDelimiter)
    val pathsList = findAllPathsForPairs(graph, inputList, length, mapGlobalIncrease)
    writeResults(pathsList, findAllPaths_out)
  }
}
