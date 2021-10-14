package Functions

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.clustering.PowerIterationClustering
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PICFunctions {
  /** *
   * This function reads the .csv file(s) containing the edges of the transformed Graph and creates a new DataFrame for them.
   *
   * @param inputDirectoryPath the relations which will be used to read the transformed dataframe of edges
   * @param delimiter          The delimiter which will be used to read the csv files in the folders
   * @return the dataframe with weights to be used in the PIC algorithm
   */
  def readTDataFrame(inputDirectoryPath: String, delimiter: String = "\t"): DataFrame = {

    require(inputDirectoryPath.nonEmpty, s"Path must not be empty")
    val spark: SparkSession = org.apache.spark.sql
      .SparkSession
      .builder
      .getOrCreate

    // Build edges DataFrame
    var edgesDF: DataFrame = spark.emptyDataFrame
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
              .withColumn("numberOfPaths", col("numberOfPaths").cast(DoubleType))
              .withColumnRenamed("numberOfPaths", "weight")
              .filter(row => row(0) != row(1))

            if(!tempDF.isEmpty){
              if(edgesDF.isEmpty){
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
    edgesDF
  }

  /** *
   * This function reads the .csv file(s) containing the edges of the transformed Graph and creates a new DataFrame for them.
   * Additionally, it returns the number of communities that at least once act as source and/or destination in the edges.
   *
   * @param inputDirectoryPath the relations which will be used to read the transformed dataframe of edges
   * @param delimiter          The delimiter which will be used to read the csv files in the folders
   * @return the transformed dataframe with weights to be used in the HPIC algorithm
   */
  def readTDataFrameHPIC(inputDirectoryPath: String, delimiter: String = "\t"): (DataFrame, Int) = {

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
              .withColumn("numberOfPaths", col("numberOfPaths").cast(DoubleType))
              .withColumnRenamed("numberOfPaths", "weight")
              .filter(row => row(0) != row(1))

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

    val edgesRDDDistinct = edgesDF.drop("weight").rdd.flatMap(r=>r.toSeq).distinct
    val countDistinct = edgesRDDDistinct.count()
    edgesRDDDistinct.unpersist()
    (edgesDF, countDistinct.toInt)
  }

  /***
   * This function executes the Power Iteration Algorithm (PIC) in the DataFrame with the edges of the transformed Graph.
   * PIC runs for a user-defined number of times and for a user-defined number of clusters.
   *
   * @param input the transformed dataframe of edges
   * @param nOfClusters the number of clusters given to PIC
   * @param maxIter the maximum number of iterations that is given to PIC
   * @return a dataframe with two columns where every row contains a VertexId and its cluster.
   */
  def runPIC(input: DataFrame, nOfClusters: Int, maxIter: Int): DataFrame ={

    require(nOfClusters >= 2, s"The number of starting clusters must be greater or equal than 2, but got $nOfClusters")
    require(maxIter > 0, s"the maximum number of iterations must be greater than 0, but got $maxIter")

    val model = new PowerIterationClustering().
      setK(nOfClusters).
      setMaxIter(maxIter).
      setInitMode("degree").
      setWeightCol("weight")

    val prediction = model.assignClusters(input)

    val tPrediction = prediction.select("id", "cluster")
    prediction.unpersist()
    tPrediction
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
    val inputCached = input.cache()
    val predictionCached = prediction.cache()
    val numberOfPartitions = inputCached.rdd.getNumPartitions

    // first join with src prediction
    val firstJoinDF = inputCached.join(predictionCached.as("srcDF"), inputCached("src") ===  $"srcDF.id")
      .drop("srcDF.id", "src")
      .withColumnRenamed("cluster", "src")
      .coalesce(numberOfPartitions)
      .cache()

    inputCached.unpersist()
    // second join with dst prediction
    val secondJoinDF = firstJoinDF.join(predictionCached.as("dstDF"),firstJoinDF("dst") === $"dstDF.id")
      .drop("dstDF.id", "dst")
      .withColumnRenamed("cluster", "dst")
      .coalesce(numberOfPartitions)
      .cache()

    firstJoinDF.unpersist()
    predictionCached.unpersist()
    // select and rename cluster and weight columns
    val intermediateDF = secondJoinDF.select($"src", $"dst", secondJoinDF("weight"))
      .filter(row => row(0) != row(1))
      .groupBy($"src", $"dst")
      .agg(sum("weight"))
      .withColumnRenamed("srcCluster", "src")
      .withColumnRenamed("dstCluster", "dst")
      .withColumnRenamed("sum(weight)", "weight")
      .coalesce(numberOfPartitions)

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
    val outputDFCached = outputDF.cache()
    val newDF = outputDFCached.join(prediction, outputDF("level" + level) === prediction("id"), "left")
      .drop("id")
      .withColumnRenamed("cluster", "level" + (level + 1))
    outputDFCached.unpersist()
    newDF
  }


  /***
   * This function executes the Hierarchical Power Iteration Algorithm (HPIC) to generate a hierarchy of clusters.
   * It runs consecutive executions of the PIC algorithm until the next-level number of clusters is smaller or equal to 1.
   * The first execution of PIC is run on the DataFrame with the edges of the transformed Graph.
   * After each execution of PIC, we create a new DataFrame, which will be used as input in the next iteration,
   * and contains edges that correspond to the resulting clusters as source and destination vertices and the sum
   * of the outer-cluster edges’ edge-weights as new edge-weights.
   * This DataFrame is produced with the following steps:
   * * First, we join the DataFrame used in the previous execution of PIC with the DataFrame that is the output of
   * that execution of PIC on the src column and use the cluster column as the new src column.
   * * Second, we once again join the DataFrame resulting from the first join with the DataFrame that is the output of
   * that execution of PIC on the dst column this time and use the new cluster column as the new dst column.
   * * Finally, we sum the rows with common (src, dst) to produce new edge-weights.
   * The results of each execution are stored in a DataFrame as a new column. Each column corresponds to a different
   * level in the hierarchy.
   *
   * PIC runs for a user-defined number of times and for a user-defined number of clusters.
   *
   * @param input The Dataframe with the edges of the transformed graph.
   * @param nOfClusters the number of first level clusters
   * @param ratio the nOfClusters which reduces the number of clusters on each level
   * @param maxIter the maximum number of iterations that is used in each execution of PIC
   * @return a dataframe that has vertices as rows and the clusters of each level as columns
   */
  def runHPIC(input: DataFrame, nOfClusters: Int, ratio: Double, maxIter: Int): DataFrame ={

    require(nOfClusters >= 2, s"The number of starting clusters must be greater or equal than 2, but got $nOfClusters")
    require(ratio > 0 & ratio < 1, s"The ration must be greater than 0 and smaller than 1, but got $ratio")
    require(maxIter > 0, s"the maximum number of iterations must be greater than 0, but got $maxIter")


    var outputDF: DataFrame = null
    var intermediateDF: DataFrame = null
    var counter: Int = 0
    var nNOfClusters = ratio*nOfClusters
    while(nNOfClusters > 1.0 & !input.isEmpty){
      if(counter == 0){
        val tPrediction = runPIC(input, roundUp(nNOfClusters).toInt, maxIter).cache()
        intermediateDF = createNewLevelDataFrame(input, tPrediction).cache()
        if(intermediateDF.isEmpty){
          return outputDF
        }
        input.unpersist()

        outputDF = createOutputDF(tPrediction)

        // check the current number of clusters and if smaller than given number of clusters then change nNOfClusters
        val clustersDistinct = tPrediction.select("cluster").rdd.flatMap(r=>r.toSeq).distinct
        val nOfCurrentClusters = clustersDistinct.count().toInt
        if(roundUp(nNOfClusters).toInt != nOfCurrentClusters){
          nNOfClusters = nOfCurrentClusters
        }
        clustersDistinct.unpersist()
        tPrediction.unpersist()
      }
      else{
        val tPrediction = runPIC(intermediateDF, roundUp(nNOfClusters).toInt, maxIter).cache()
        intermediateDF = createNewLevelDataFrame(intermediateDF, tPrediction).cache()
        if(intermediateDF.isEmpty){
          return outputDF
        }
        outputDF = addColumnInOutputDF(counter, outputDF, tPrediction)
        // check the current number of clusters and if smaller than given number of clusters then change nNOfClusters
        val clustersDistinct = tPrediction.select("cluster").distinct().rdd.flatMap(r=>r.toSeq).distinct
        val nOfCurrentClusters = clustersDistinct.count().toInt
        if(roundUp(nNOfClusters).toInt != nOfCurrentClusters){
          nNOfClusters = nOfCurrentClusters
        }
        clustersDistinct.unpersist()
        tPrediction.unpersist()
      }
      // number of clusters for the next execution of PIC
      nNOfClusters = ratio*nNOfClusters
      // if number of clusters do not reduce (mainly smaller numbers) reduce again
      while(nNOfClusters.toInt == (nNOfClusters/ratio).toInt){
        nNOfClusters = ratio*nNOfClusters
      }
      counter+=1
    }
    outputDF
  }

  /***
   * This function writes the vertices and their clusters.
   *
   * @param outputDF the dataframe produced by PIC
   * @param outputPath the output path where we will write the results of the PIC algorithm
   */
  def writeResultsPIC(outputDF: DataFrame, outputPath: String) :Unit = {
    outputDF.sort(col("cluster").asc).coalesce(1).write.mode(SaveMode.Overwrite).format("csv")
      .option("delimiter", "\t").save(path = outputPath)
  }


  /***
   * This function writes the ids of the vertices followed by their clusters on each level of the hierarchy.
   *
   * @param outputDF the dataframe produced by HPIC
   * @param outputPath the output path where we will write the results of the HPIC algorithm
   */
  def writeResultsHPIC(outputDF: DataFrame, outputPath: String) :Unit = {
    outputDF.sort(col("level1").asc).coalesce(1).write.mode(SaveMode.Overwrite).format("csv")
      .option("nullValue", "null").option("delimiter", "\t").save(path = outputPath)
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