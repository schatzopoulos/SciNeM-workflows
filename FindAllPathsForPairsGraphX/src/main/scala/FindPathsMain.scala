import Functions.functions._

object FindPathsMain {
  def main(args: Array[String]): Unit = {

    val jsonString = os.read(os.Path(args(0)))
    val data = ujson.read(jsonString)

    val nodes_dir = data("nodes_dir").str
    val relations_dir = data("relations_dir").str
    val inputCSVDelimiter = data("inputCSVDelimiter").str
    val findAllPaths_out = data("path_searching_out").str
    val findAllForPairs = data("path_searching_pairs").str
    val length = data("path_searching_length").num.toInt

    println("Finding Paths for Pairs of Vertices\t1\tInitializing Algorithm")
    val (mapGlobalIncrease, graph) = createInputGraph(nodes_dir, relations_dir, inputCSVDelimiter)
    val inputList = createPairList(findAllForPairs, mapGlobalIncrease, inputCSVDelimiter)
    println("Finding Paths for Pairs of Vertices\t2\tExecuting FindAllPathsForPairs algorithm")
    val pathsList = findAllPathsForPairs(graph, inputList, length, mapGlobalIncrease)
    writeResults(pathsList, findAllPaths_out)
  }
}
