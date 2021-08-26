import org.apache.spark.{SparkConf, SparkContext}
import Functions.LPAFunctions._
import Functions.PICFunctions.{readTDataFrame, readTDataFrameHPIC, runHPIC, runPIC, writeResultsHPIC, writeResultsPIC}


object AlgorithmsMain {
  def main(args: Array[String]): Unit = {
    val jsonString = os.read(os.Path(args(0)))
    val data = ujson.read(jsonString)

    val inputDirectoryPath = data("hin_out").str
    val inputCSVDelimiter = data("inputCSVDelimiter").str
    val community_algorithm = data("community_algorithm").str
    val communities_out = data("communities_out").str
    val maxSteps = data("maxSteps").num.toInt

    //remove master if using spark-submit and add e.g. --master spark://master:7077
    val conf = new SparkConf().setAppName("AlgorithmsGraphX")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /*
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val df = spark.read.option("multiLine", value = true)
      .option("mode", "PERMISSIVE").json(args(0))

    val inputDirectoryPath = df.select("hin_out").collectAsList().get(0).get(0).toString
    val inputCSVDelimiter = df.select("inputCSVDelimiter").collectAsList().get(0).get(0).toString
    val community_algorithm = df.select("community_algorithm").collectAsList().get(0).get(0).toString
    val communities_out = df.select("communities_out").collectAsList().get(0).get(0).toString
    val maxSteps = df.select("maxSteps").collectAsList().get(0).get(0).toString.toInt
*/

    if (community_algorithm.toUpperCase.equals("LPA")){
      //val stopCriterion = df.select("stopCriterion").collectAsList().get(0).get(0).toString.toInt
      val stopCriterion = data("stopCriterion").num.toInt
      val tGraph = readTGraph(inputDirectoryPath, inputCSVDelimiter)
      val lpaGraph = runLPA(tGraph, maxSteps, stopCriterion)
      writeResultsLPA(lpaGraph.vertices, communities_out)
    }
    else if(community_algorithm.toUpperCase.equals("OLPA")){
      //val stopCriterion = df.select("stopCriterion").collectAsList().get(0).get(0).toString.toInt
      //val threshold = df.select("threshold").collectAsList().get(0).get(0).toString.toDouble
      val stopCriterion = data("stopCriterion").num.toInt
      val threshold = data("threshold").num
      val tGraph = readTGraph(inputDirectoryPath, inputCSVDelimiter)
      val olpaGraph = runOLPA(tGraph, maxSteps, threshold, stopCriterion)
      writeResultsOLPA(olpaGraph.vertices, communities_out)
    }
    else if(community_algorithm.toUpperCase.equals("PIC")){
      //val nOfCommunities = df.select("nOfCommunities").collectAsList().get(0).get(0).toString.toInt
      val nOfCommunities = data("nOfCommunities").num.toInt
      val tDataFrame = readTDataFrame(inputDirectoryPath, inputCSVDelimiter)
      val outputDF = runPIC(tDataFrame, nOfCommunities, maxSteps)
      writeResultsPIC(outputDF, communities_out)
    }
    else if(community_algorithm.toUpperCase.equals("HPIC")){
      //val ratio = df.select("ratio").collectAsList().get(0).get(0).toString.toDouble
      val ratio = data("ratio").num
      val (input, numberOfCommunities) = readTDataFrameHPIC(inputDirectoryPath,inputCSVDelimiter)
      val outputDF = runHPIC(input, numberOfCommunities, ratio, maxSteps)
      writeResultsHPIC(outputDF, communities_out)
    }
    else println("Unknown algorithm has been requested!")
  }
}
