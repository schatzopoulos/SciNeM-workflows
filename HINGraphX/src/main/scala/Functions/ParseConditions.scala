package Functions

class ParseConditions(private val metaPath: String, private val runReverseTransformation: Boolean) extends Serializable  {
  private val dashRegex = "-(?=([^\']*\'[^\']*\')*[^\']*$)".r
  private val openColumnRegex = "\\[(?=([^\']*\'[^\']*\')*[^\']*$)".r
  private val semiColumnRegex = ";(?=([^\']*\'[^\']*\')*[^\']*$)".r

  require(dashRegex.findFirstIn(metaPath).exists(_.trim.nonEmpty), s"Path must not be empty")
  private var chopPath: Array[String] = Array[String]()
    if (runReverseTransformation){
      chopPath = metaPath.split(dashRegex.regex).map(_.trim).reverse
    }
    else{
      chopPath = metaPath.split(dashRegex.regex).map(_.trim)
    }

  var pathLength: Int = 0

  var mapNodeTypes: Map[Int, String] = Map[Int, String]()
  var mapNodeHasConditions: Map[Int, Boolean] = Map[Int, Boolean]()
  var mapNodeConditions: Map[Int, List[String]] = Map[Int, List[String]]()

  var mapEdgeHasConditions: Map[Int, Boolean] = Map[Int, Boolean]()
  var mapEdgeConditions: Map[Int, List[String]] = Map[Int, List[String]]()

  private var countNodes = 0
  private var countEdges = 0
  for (vPosition <- chopPath.indices){
    val fType = chopPath(vPosition)
    if (fType.startsWith("(")){ // Found an edge
      val edgeConditions = fType.substring(1, fType.length() - 1).trim
      if (!edgeConditions.isEmpty){
        mapEdgeHasConditions += countEdges -> true
        if (semiColumnRegex.findFirstIn(edgeConditions).exists(_.trim.nonEmpty)){
          val conditionsList = edgeConditions.split(semiColumnRegex.regex).map(_.trim).toList
          mapEdgeConditions += countEdges -> conditionsList
        }
        else{
          mapEdgeConditions += countEdges -> List[String](edgeConditions)
        }
      }
      else{
        mapEdgeHasConditions += countEdges -> false
      }
      countEdges += 1
    }
    else{ // Found a node
      if (openColumnRegex.findFirstIn(fType).exists(_.trim.nonEmpty)){
        val vType = fType.split(openColumnRegex.regex).map(_.trim).head
        mapNodeTypes += countNodes -> vType
        mapNodeHasConditions += countNodes -> true

        var vertexConditions = fType.split(openColumnRegex.regex).map(_.trim).last
        vertexConditions = vertexConditions.substring(0, vertexConditions.length() - 1)
        if (semiColumnRegex.findFirstIn(vertexConditions).exists(_.trim.nonEmpty)){
          val conditionsList = vertexConditions.split(semiColumnRegex.regex).map(_.trim).toList
          mapNodeConditions += countNodes -> conditionsList
        }
        else{
          if (vertexConditions.trim != ""){
            mapNodeConditions += countNodes -> List[String](vertexConditions)
          }
          else{
            mapNodeHasConditions += countNodes -> false
          }

        }
      }
      else{
        mapNodeTypes += (countNodes -> fType)
        mapNodeHasConditions += (countNodes -> false)
      }
      countNodes += 1
    }
  }
  pathLength = countNodes
}
