package summingbird.proto

import java.io.PrintWriter
import java.util.Date

object DummyBatch extends App {
  import ViewCount._

  val now = System.currentTimeMillis

  var current = now - 3 * batcher.durationMillis
  var currentFile: String = null
  var currentWriter: PrintWriter = null

  while (current < now) {
    val date = new Date(current)

    val pdpView = randomView(date)

    val fileName = dataFileForBatch(batcher.batchOf(date))

    if (fileName != currentFile) {
      if (currentWriter != null) currentWriter.close()

      currentFile = fileName
      currentWriter = new PrintWriter(fileName)
    }

    currentWriter.println(serializeView(pdpView))

    current += 1000
  }

  currentWriter.close()
}
