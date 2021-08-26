package Functions

import Functions.Common.{getHdfs,deleteDirectory}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.clustering.PowerIterationClustering
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object PICFunctions {
  /** *
   *
   * @param inputDirectoryPath the relations which will be used to read the transformed dataframe of edges
   * @param delimiter          The delimiter which will be used to read the csv files in the folders
   * @return the transformed dataframe with weights to be used in the PIC algorithm
   */
  def readTDataFrame(inputDirectoryPath: String, delimiter: String = "\t"): DataFrame = {

    require(inputDirectoryPath.nonEmpty, s"Path must not be empty")
    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate

    //Build edges DataFrame
    var edgesDF: DataFrame = null
    val hdfsFolder = getHdfs(inputDirectoryPath)
    val outPutPath = new Path(inputDirectoryPath)
    val findStatus = hdfsFolder.getFileStatus(outPutPath)

    if (hdfsFolder.exists(outPutPath) && findStatus.isDirectory) {
      hdfsFolder.listStatus(outPutPath)
        .toList
        .foreach(file => {
          if (file.getPath.getName.endsWith(".csv")) {
            edgesDF = spark.read
              .format("csv")
              .option("header", "true")
              .option("mode", "DROPMALFORMED")
              .option("delimiter", delimiter)
              .option("inferSchema", "true")
              .load(file.getPath.toString)
              .withColumn("src", col("src").cast(LongType))
              .withColumn("dst", col("dst").cast(LongType))
              .withColumn("numberOfPaths", col("numberOfPaths").cast(LongType))
              .withColumnRenamed("numberOfPaths", "weight")
          }
        })
    }
    edgesDF
  }

  /***
   *
   * @param input the transformed dataframe with weights
   * @param nOfCommunities the number of communities given to PIC
   * @param maxSteps the maximum number of iterations that is used in PIC
   * @return a dataframe that every vertex id and its community
   */
  def runPIC(input: DataFrame, nOfCommunities: Int, maxSteps: Int): DataFrame ={

    require(nOfCommunities >= 2, s"The number of starting communities must be greater or equal than 2, but got $nOfCommunities")
    require(maxSteps > 0, s"the maximum number of iterations must be greater than 0, but got $maxSteps")

    val model = new PowerIterationClustering().
      setK(nOfCommunities).
      setMaxIter(maxSteps).
      setInitMode("degree").
      setWeightCol("weight")

    val prediction = model.assignClusters(input)
    val tPrediction = prediction.select("id", "cluster")
    prediction.unpersist()
    tPrediction
  }

  /** *
   *
   * @param inputDirectoryPath the relations which will be used to read the transformed dataframe of edges
   * @param delimiter          The delimiter which will be used to read the csv files in the folders
   * @return the transformed dataframe with weights to be used in the PIC algorithm
   */
  def readTDataFrameHPIC(inputDirectoryPath: String, delimiter: String = "\t"): (DataFrame, Int) = {

    require(inputDirectoryPath.nonEmpty, s"Path must not be empty")
    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate

    //Build edges DataFrame
    var edgesDF: DataFrame = null
    val hdfsFolder = getHdfs(inputDirectoryPath)
    val outPutPath = new Path(inputDirectoryPath)
    val findStatus = hdfsFolder.getFileStatus(outPutPath)

    if (hdfsFolder.exists(outPutPath) && findStatus.isDirectory) {
      hdfsFolder.listStatus(outPutPath)
        .toList
        .foreach(file => {
          if (file.getPath.getName.endsWith(".csv")) {
            edgesDF = spark.read
              .format("csv")
              .option("header", "true")
              .option("mode", "DROPMALFORMED")
              .option("delimiter", delimiter)
              .option("inferSchema", "true")
              .load(file.getPath.toString)
              .withColumn("src", col("src").cast(LongType))
              .withColumn("dst", col("dst").cast(LongType))
              .withColumn("numberOfPaths", col("numberOfPaths").cast(LongType))
              .withColumnRenamed("numberOfPaths", "weight")
          }
        })
    }
    val edgesRDDDistinct = edgesDF.drop("weight").rdd.flatMap(r=>r.toSeq).distinct
    val countDistinct = edgesRDDDistinct.collect.length
    edgesRDDDistinct.unpersist()
    (edgesDF, countDistinct)
  }
  /***
   *
   * @param input the dataframe used as input in the previous execution of PIC
   * @param prediction the dataframe resulting from the previous execution of PIC
   * @return a dataframe to be used as input in the next execution of PIC
   */
  private def createNewLevelDataFrame(input: DataFrame, prediction: DataFrame): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    //first join with src prediction
    val firstJoinDF = input.join(prediction.as("srcDF"), input("src") ===  $"srcDF.id")
      .drop("srcDF.id", "src")
      .withColumnRenamed("cluster", "src")

    input.unpersist()

    //second join with dst prediction
    val secondJoinDF = firstJoinDF.join(prediction.as("dstDF"),input("dst") ===  $"dstDF.id")
      .drop("dstDF.id", "dst")
      .withColumnRenamed("cluster", "dst")

    firstJoinDF.unpersist()

    // select and rename cluster and weight columns
    val intermediateDF = secondJoinDF.select($"src", $"dst", input("weight"))
      .groupBy($"src", $"dst")
      .agg(sum("weight"))
      .withColumnRenamed("srcCluster", "src")
      .withColumnRenamed("dstCluster", "dst")
      .withColumnRenamed("sum(weight)", "weight")

    secondJoinDF.unpersist()

    intermediateDF
  }

  /***
   *
   * @param d a double number which we will round up
   * @return a double number
   */
  private def roundUp(d: Double): Double = math.ceil(d)

  /***
   *
   * @param prediction the dataframe resulting from the first execution of PIC
   * @return a dataframe with the first 2 levels of the hierarchy for each vertex
   */
  private def createOutputDF(prediction: DataFrame): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    prediction.select($"id".as("vid"), $"id".as("level0"), $"cluster".as("level1"))
  }

  /***
   *
   * @param level the level of the hierarchy from the second-to-last execution of PIC
   * @param outputDF the dataframe which contains for each vertex all the communities
   *                 from previous, except the last, levels
   * @param prediction the dataframe resulting from the last execution of PIC
   * @return a dataframe that contains for each vertex all the communities, for each level
   *         of the hierarchy, from all the previous executions of PIC
   */
  private def addColumnInOutputDF(level: Int, outputDF: DataFrame, prediction: DataFrame): DataFrame = {
    //val spark = SparkSession.builder.getOrCreate()
    //import spark.implicits._
    val newDF = outputDF.join(prediction, outputDF("level" + level) === prediction("id"))
      .drop("id")
      .withColumnRenamed("cluster", "level" + (level + 1))
    outputDF.unpersist()
    newDF
  }


  /***
   *
   * @param input the transformed dataframe with weights
   * @param nOfCommunities the number of first level communities
   * @param ratio the number which reduces the number of communities on each level
   * @param maxIter the maximum number of iterations that is used in each execution of PIC
   * @return a dataframe that has vertices as rows and the communities of each level as columns
   */
  def runHPIC(input: DataFrame, nOfCommunities: Int, ratio: Double, maxIter: Int): DataFrame ={

    require(nOfCommunities >= 2, s"The number of starting communities must be greater or equal than 2, but got $nOfCommunities")
    require(ratio > 0 & ratio < 1, s"The ration must be greater than 0 and smaller than 1, but got $ratio")
    require(maxIter > 0, s"the maximum number of iterations must be greater than 0, but got $maxIter")

    var outputDF: DataFrame = null
    var intermediateDF: DataFrame = null
    var counter: Int = 0
    var nNOfCommunities = ratio*nOfCommunities

    while(nNOfCommunities >= 1.0){
      println(nNOfCommunities)
      val model = new PowerIterationClustering().
        setK(roundUp(nNOfCommunities).toInt).
        setMaxIter(maxIter).
        setInitMode("degree").
        setWeightCol("weight")

      if(counter == 0){
        val prediction = model.assignClusters(input)
        intermediateDF = createNewLevelDataFrame(input, prediction)
        input.unpersist()

        outputDF = createOutputDF(prediction)

        //check the current number of communities and if smaller than given number of commnunities then change nNOfCommunities
        val clustersDistinct = prediction.select("cluster").rdd.flatMap(r=>r.toSeq).distinct
        prediction.unpersist()
        val nOfCurrentCommunities = clustersDistinct.collect.length
        if(roundUp(nNOfCommunities).toInt > nOfCurrentCommunities){
          nNOfCommunities = nOfCurrentCommunities
        }
        clustersDistinct.unpersist()
      }
      else{
        val prediction = model.assignClusters(intermediateDF)
        intermediateDF = createNewLevelDataFrame(intermediateDF, prediction)

        outputDF = addColumnInOutputDF(counter, outputDF, prediction)

        //check the current number of communities and if smaller than given number of commnunities then change nNOfCommunities
        val clustersDistinct = prediction.select("cluster").rdd.flatMap(r=>r.toSeq).distinct
        prediction.unpersist()
        val nOfCurrentCommunities = clustersDistinct.collect.length
        if(roundUp(nNOfCommunities).toInt > nOfCurrentCommunities){
          nNOfCommunities = nOfCurrentCommunities
        }
        clustersDistinct.unpersist()
      }
      // number of communities for the next execution of PIC
      nNOfCommunities = ratio*nNOfCommunities
      // if number of communities do not reduce (mainly smaller numbers) reduce again
      while(nNOfCommunities.toInt == (nNOfCommunities/ratio).toInt){
        nNOfCommunities = ratio*nNOfCommunities
        // exit while if previously
        if(nNOfCommunities.toInt == 0){
          nNOfCommunities = -100000
        }
      }
      counter+=1
    }
    intermediateDF.unpersist()

    outputDF
  }


  /***
   *
   * @param results the dataframe produced by PIC
   * @param outputPath the output path where we will write the results of the PIC algorithm
   */
  def writeResultsPIC(results: DataFrame, outputPath: String) :Unit = {
    deleteDirectory(outputPath)
    results.write.format("csv").option("delimiter", "\t").save(path = outputPath)
  }


  /***
   *
   * @param outputDF the dataframe produced by HPIC
   * @param outputPath the output path where we will write the results of the HPIC algorithm
   */
  def writeResultsHPIC(outputDF: DataFrame, outputPath: String) :Unit = {
    deleteDirectory(outputPath)
    outputDF.write.format("csv").option("delimiter", "\t").save(path = outputPath)
  }
}