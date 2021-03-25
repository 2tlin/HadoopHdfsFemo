package hadoop.course.homework2
/*
Задача
Требуется написать Scala-приложение, которое будет очищать данные из папки /stage
и складывать их в папку /ods в корне HDFS по следующим правилам:

1) Структура партиций (папок вида date=...) должна сохраниться.
2) Внутри папок должен остаться только один файл,
содержащий все данные файлов из соответствующей партиции в папке /stage.
То есть, если у нас есть папка /stage/date=2020-11-11 с файлами

part-0000.csv
part-0001.csv
то должна получиться папка /ods/date=2020-11-11 с одним файлом
part-0000.csv
содержащим все данные из файлов папки /stage/date=2020-11-11

Подсказки:
1) Файлы конфигурации Hadoop можно найти внутри контейнера namenode в папке /etc/hadoop.
Эти файлы нужно поместить в папку resources вашего проекта.
И тогда пр и создании клиента они найдутся автоматически.
2) Для взаимодействия с HDFS из Scala следует использовать FileSystem API
Для подключения этого API в ваш проект понадобится Hadoop Client
"org.apache.hadoop" % "hadoop-client" % "3.2.1"
*/
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object HadoopHdfsDemo extends App {

  System.setProperty("hadoop.home.dir", "c:/Apache/Hadoop")
  val conf = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf)
  val path: Path = new Path("hdfs://localhost:9000/stage")

  val dst = "hdfs://localhost:9000/ods"
  if (fs.exists(new Path(dst)))
    fs.delete(new Path(dst), true)

  def writeFiles(path: Path): Unit = {
    val listedStatus: Array[FileStatus] = fs.listStatus(path)

    listedStatus.foreach(listStatusRecursively)

    def listStatusRecursively(fileStatus: FileStatus): Unit = fileStatus match {
      case fileStatus if fileStatus.isFile && fileStatus.getPath.getName.contains(".csv") =>
        appendToFile(fileStatus.getPath)
        listStatusRecursively(_)
      case fileStatus if fileStatus.isDirectory  => fs.listStatus(fileStatus.getPath).foreach(listStatusRecursively)
      case _ => listStatusRecursively(_)
    }

    def appendToFile(path: Path): Unit = {
      var out: FSDataOutputStream = null
      val in: FSDataInputStream = fs.open(path)
      val filename: Path = new Path(path.getParent.toString.replace("stage", "ods") + "/part-0000.csv")
      try {
        if ( ! fs.exists(filename)) {
          out = fs.create(filename)
          IOUtils.copy(in, out)
        } else if (fs.exists(filename) &&
                  filename.getParent == new Path(path.getParent.toString.replace("stage", "ods")) &&
                  filename.getName != path.getName)
        {
          out = fs.append(filename)
          IOUtils.copy(in, out)
        }
      } finally {
          if (in != null)   in.close()
          if (out != null)  out.close()
      }
    }
  }
  writeFiles(path)
  fs.deleteOnExit(path)
  fs.close()
}

