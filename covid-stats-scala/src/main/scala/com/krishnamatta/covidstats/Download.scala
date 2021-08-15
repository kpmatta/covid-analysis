package main.scala.com.krishnamatta.covidstats

import java.io.{File, FileWriter}
import java.time.temporal.ChronoUnit
import java.util.Date

object Download {

  // get
  def getFileModifiedDays(file: File): Long = {
    val fileModified = file.lastModified()
    ChronoUnit.DAYS.between(new Date().toInstant, new Date(fileModified).toInstant)
  }

  // Download file if the local file is more than a day old
  def downloadFile(url: String, destPath: String): Unit = {
    val file = new File(destPath)
    val download = if (file.exists() && getFileModifiedDays(file) > 0) {
      false
    } else {
      true
    }

    if (download) {
      val src = scala.io.Source.fromURL(url)
      val out = new FileWriter(destPath)
      out.write(src.mkString)
    }
  }
}
