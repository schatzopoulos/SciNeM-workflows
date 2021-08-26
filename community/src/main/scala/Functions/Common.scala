package Functions

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Common {
  /***
   *
   * @param path the path to the hdfs folder/file
   * @return Generate hadoop FileSystem
   */
  def getHdfs(path: String): FileSystem = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }

  /***
   *
   * @param path the path to the output directory
   */
  def deleteDirectory(path: String): Unit = {
    val hdfs = getHdfs(path)
    val outPutPath = new Path(path)
    if (hdfs.exists(outPutPath)) {
      hdfs.delete(outPutPath, true)
    }
    hdfs.close()
  }
}
