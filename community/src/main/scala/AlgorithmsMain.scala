import Functions.LPAFunctions._
import Functions.PICFunctions._

object AlgorithmsMain {
  def main(args: Array[String]): Unit = {
    val jsonString = os.read(os.Path(args(0)))
    val data = ujson.read(jsonString)

    val inputDirectoryPath = data("hin_out").str
    val inputCSVDelimiter = data("inputCSVDelimiter").str
    val community_algorithm = data("community_algorithm").str
    val communities_out = data("communities_out").str
    val maxSteps = data("maxSteps").num.toInt

    println("Community Detection\t1\tInitializing Algorithm")
    if (community_algorithm.toUpperCase.equals("LPA")){
      val stopCriterion = data("stopCriterion").num.toInt
      val tGraph = readTGraph(inputDirectoryPath, inputCSVDelimiter)
      println("Community Detection\t2\tExecuting Label Propagation Algorithm")
      val lpaGraph = runLPA(tGraph, maxSteps, stopCriterion)
      writeResultsLPA(lpaGraph.vertices, communities_out)
    }
    else if(community_algorithm.toUpperCase.equals("OLPA")){
      val stopCriterion = data("stopCriterion").num.toInt
      val threshold = data("threshold").num
      val tGraph = readTGraph(inputDirectoryPath, inputCSVDelimiter)
      println("Community Detection\t2\tExecuting Overlapping Label Propagation Algorithm")
      val olpaGraph = runOLPA(tGraph, maxSteps, threshold, stopCriterion)
      writeResultsOLPA(olpaGraph.vertices, communities_out)
    }
    else if(community_algorithm.toUpperCase.equals("PIC")){
      val nOfClusters = data("nOfCommunities").num.toInt
      val tDataFrame = readTDataFrame(inputDirectoryPath, inputCSVDelimiter)
      println("Community Detection\t2\tExecuting Power Iteration Clustering")
      val outputDF = runPIC(tDataFrame, nOfClusters, maxSteps)
      writeResultsPIC(outputDF, communities_out)
    }
    else if(community_algorithm.toUpperCase.equals("HPIC")){
      val ratio = data("ratio").num
      val (tDataFrame, nOfClusters) = readTDataFrameHPIC(inputDirectoryPath,inputCSVDelimiter)
      println("Community Detection\t2\tExecuting Hierarchical Power Iteration Clustering")
      val outputDF = runHPIC(tDataFrame, nOfClusters, ratio, maxSteps)
      writeResultsHPIC(outputDF, communities_out)
    }
    else println("Unknown algorithm has been requested!")
  }
}
