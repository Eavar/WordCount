import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import  java.util.Date

object WordCount {

  def main(args: Array[String]): Unit = {

    // Set hadoop home dir for Windows
    //System.setProperty("hadoop.home.dir", "C:\\Users\\alexander.egorenkov\\Hadoop")

    // Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val textFile = sc.textFile("hdfs:///user/alexander/shakespeare.txt") //Hadoop
    //val textFile = sc.textFile("src/main/resources/shakespeare.txt")  //Windows
    val textFile = sc.textFile("/Users/eavar/GoogleDrive/Programming/Scala/WordCount/src/main/resources/shakespeare.txt") //Mac

    // Word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(_.toLowerCase)
      .map(_.replaceAll("[^a-zA-Z]", ""))
      .filter(word => word.length > 0)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    counts.collect()
    counts.take(10).foreach(System.out.println)
    System.out.println("Total words: " + counts.count())

    val dateFormatter = new SimpleDateFormat("dd.MM.yyyy_hh-mm_aa")
    val submittedDateConvert = new Date()
    val timeNow = dateFormatter.format(submittedDateConvert)
    //counts.saveAsTextFile("file:///home/admin/shakespeareWordCount") //Hadoop
    counts.saveAsTextFile(s"/Users/eavar/GoogleDrive/Programming/Scala/WordCount/src/main/output/count_$timeNow") //Mac

    // Stop SparkContext
    sc.stop()
  }

}
