import org.apache.spark.{SparkConf, SparkContext}

object HelloScala {

  def main(args: Array[String]): Unit = {

    //Set hadoop home dir for Windows
    System.setProperty("hadoop.home.dir", "C:\\Users\\alexander.egorenkov\\Hadoop")

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("src/main/resources/shakespeare.txt")
//    val textFile = sc.textFile("hdfs:///tmp/shakespeare.txt")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(_.toLowerCase)
      .map(_.replaceAll("[^a-zA-Z]", ""))
      .filter(word => word.length > 0)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    counts.collect()
    counts.foreach(System.out.println)
    System.out.println("Total words: " + counts.count())
//    counts.saveAsTextFile("hdfs:///tmp/shakespeareWordCount")
  }

}
