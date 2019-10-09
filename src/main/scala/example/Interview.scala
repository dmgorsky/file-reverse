package example

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, PreparedStatement}

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{SparkSession, functions => F}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object Interview extends FixtureAndStuff with App with LazyLogging {

  override def main(args: Array[String]): Unit = {
    super.main(args)

    //
    // look in args / input the fileName
    val fileName = getFilename(args)

    //
    // guard if the file exists
    if (Files.notExists(Paths.get(fileName))) {
      println(s"File doesn't exist: $fileName")
    } else measureTime { // haha, you still want me to work, okay then...

      println(s"about to ingest $fileName")

      //
      // Read words from the file, split by ' ', '\t', '\n'
      ingestReverseWordsToDb(getInputStream(fileName))

      println("data ingested") // logger.debug("data ingested")

      //
      // We are going to let Apache Spark to cope with that words dataframe
      // ingested from a text file into our temp database
      //
      val sparkSession = SparkSession
        .builder()
        .appName("interview")
        .master("local")
        .config("spark.executor.memory", "4g")
        .getOrCreate

      import sparkSession.implicits._


      val src = sparkSession.read
        .format("jdbc")
        .option("url", dbUrl)
        .option("dbtable", wordsTable)
        .option("user", defaultUser).option("password", defaultPassword)
        .load()

      println("You wanted me to flip words, it might look like this:")
      //
      // show source data in reversed order
      src.withColumn("N", F.monotonically_increasing_id())
        .sort(F.desc("N"))
        .drop("N")
        .withColumn("reversed", F.reverse($"word"))
        .drop("word") // leave only reversed words (in reversed order)
        .show(200)

      println("Those words popularity is obvious:")
      //
      // count literals and sort descending
      // show 100 samples, they can be not only words
      //
      src
        .groupBy("word") // count
        .agg(F.count($"word"))
        .orderBy(F.desc("count(word)")) // then order descending
        //        .drop("count(word)") // then get rid of the count column
        .show(100) // finally amaze curious user

    }

  }


  /*
  look in args or input the fileName
   */
  private def getFilename(args: Array[String]) = {
    if (args.nonEmpty && args(0).nonEmpty)
      args(0)
    else
      scala.io.StdIn.readLine("Input filename (i.e. /path_to/file.ext): ")
  }

  /**
    * Register a block (call-by-name) execution time
    */
  def measureTime[T](block: => T): T = {
    println("Measuring execution time...")
    val timeFrom = System.nanoTime()
    val result = block
    val timeTo = System.nanoTime()
    println(s"Elapsed time: ${(timeTo - timeFrom) / 1000000000} s")
    result
  }

  /*
  Get prepared to read a file
   */
  def getInputStream(fileName: String) = new BufferedInputStream(new FileInputStream(fileName))

  //
  // read byte by byte (1-byte encoding),
  // split by delimiters,
  // prepare batch statements,
  // insert to db
  //
  def ingestReverseWordsToDb(bis: BufferedInputStream): Unit = {


    Class.forName(jdbcDriver)

    val conn = DriverManager.getConnection(dbUrl, defaultUser, defaultPassword)

    val stmt = conn.createStatement
    Try {
      stmt.executeUpdate("drop table words")
    }
    stmt.executeUpdate(s"create table words (word varchar($maxWordSize))")

    // delimiters (so far)
    val space = ' '.toByte
    /*
        // in case some day this helps
        val newLine = '\n'.toByte
        val tab = '\t'.toByte
    */


    val word = ArrayBuffer.empty[Char]
    var nextByte: Int = -1
    var batchCounter: Int = 0;

    val insertStmt: PreparedStatement = conn.prepareStatement("insert into words values ?")

    def flush() = {
      insertStmt.executeBatch()
      insertStmt.clearBatch()
      batchCounter = 0
    }

    while ( {
      nextByte = bis.read()
      nextByte
    } != -1) {
      if (nextByte == space /*|| nextByte == newLine ||  nextByte == tab*/ ) {
        if (word.nonEmpty) {
          // todo split long words?
          val wordData = if (word.size <= maxWordSize) word.mkString else word.take(maxWordSize).mkString
          insertStmt.setString(1, wordData)
          insertStmt.addBatch()
          batchCounter += 1
          if (batchCounter >= 300000) flush()
          word.clear()
        }
      } else {
        word.append(nextByte.toChar)
      }
    }
    val wordData = if (word.size <= maxWordSize) word.mkString else word.take(maxWordSize).mkString
    insertStmt.setString(1, wordData)
    insertStmt.addBatch()

    println("about to execute batch inserts")
    insertStmt.executeBatch()
    println("about to commit")
    conn.commit()
    bis.close()


  }

}

trait FixtureAndStuff {
  lazy val greeting: String = "hello"

  val tmpDir: String = System.getProperty("java.io.tmpdir")
  val dbUrl = s"jdbc:h2:$tmpDir/interview"
  val jdbcDriver = "org.h2.Driver"

  val defaultUser = "sa"
  val defaultPassword = ""

  val wordsTable = "words"

  val maxWordSize = 4000

}
