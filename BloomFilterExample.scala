import org.apache.spark.{SparkConf, SparkContext}
import breeze.util.BloomFilter
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender

object BloomFilterExample {
  def main(args: Array[String]): Unit = {


    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val conf = new SparkConf()
      .setAppName("Inverted Index Task")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val DocsRDD  = sc.wholeTextFiles ("data/BigTemp", 8)

    val stopWords = Set("the", "is", "in", "and", "to", "of", "a", "for", "on", "with", "as", "it", "that", "by", "this" , "at" ,"its" , "an" , "was" , "were" , "be")
    val regexSymbols = "[^a-z]+".r

    //take only content .
    val DocsContentRDD = DocsRDD.map{ case(docpath , doccontent)  => doccontent }
    val splittedDocsContentRDD = DocsContentRDD.map(content => content.split("\\s+").map(word => word.toLowerCase()))

    //splittedDocsContentRDD.collect().take(2).foreach{doc => println(doc.mkString(", "))}

    val cleanDocRdd= splittedDocsContentRDD
      .map(content => content
        .map(word => word.replaceAll(regexSymbols.regex, ""))
        .filterNot(word => word.length == 1)
        .filter(_.nonEmpty)
        .filterNot(word => stopWords.contains(word)))

    //cleanDocRdd.collect().take(2).foreach{doc => println(doc.mkString(", "))}

    val finalRDD = cleanDocRdd.flatMap(content => content.map(word => word))
    //finalRDD.collect().take(10).foreach(println)

    //get RDD has unique words
    val uniqueRDD =  finalRDD.distinct()
    uniqueRDD.collect().take(10).foreach(println)


    val countofwords =  uniqueRDD.count()
    println(s"Number of words in RDD : $countofwords")


    // Number of words in RDD => 2064
    val expectedElements = 2064
    val falsePositiveRate = 0.04 // 4% false to save memory

    val bf = BloomFilter.optimallySized[String](expectedElements, falsePositiveRate )
    //add RDD elements to the bf
    uniqueRDD.collect().map(word => bf += word)

    println(bf.contains("pork")) // true
    println(bf.contains("have")) // true
    println(bf.contains("Roaa")) // false

    //Compute the error
    var NumberOfWords = 0

    uniqueRDD.collect().foreach { word =>
      if (bf.contains(word)) {
        NumberOfWords += 1
      }
    }

    println(s"Number Of Words that are In RDD and In BF :$NumberOfWords")
    // Bloom filters not having false negatives


    // Create a list of names
    val namesList = Seq(
      "Roaa", "Bella", "Caleb", "Daisy", "Ethan", "Fiona", "Gabriel", "Hannah",
      "Isaac", "Julia", "Kyle", "Leah", "Mason", "Nora", "Oliver", "Piper",
      "Quinn", "Ryan", "Sophia", "Thomas", "Uma", "Victor", "Willow", "Xavier",
      "Yara", "Zachary", "Amelia", "Benjamin", "Chloe", "Dylan", "Eva", "Finn",
      "Grace", "Henry", "Iris", "Jack", "Kate", "Leo", "Mia", "Noah", "Opal",
      "Paul", "Ruby", "Samuel", "Tessa", "Ulysses", "Veda", "Wyatt", "Xena",
      "Zara", "Adam", "Brianna", "Charlie", "Delilah", "Elijah", "Freya", "George",
      "Hazel", "Ian", "Jada", "Kevin", "Lila", "Max", "Natalie", "Owen", "Paige",
      "Quincy", "Riley", "Seth", "Tiffany", "Uri", "Vanessa", "William", "Xander",
      "Yasmine", "Zoe", "Arthur", "Bethany", "Cody", "Diana", "Edwin", "Felicity",
      "Gideon", "Heidi", "Jonah", "Kira", "Levi", "Melinda", "Nathan", "Olive",
      "Philip", "Quinn", "Regina", "Sienna", "Theo", "Uma", "Vince", "Whitney",
      "Xavier", "Yvette"
    )

    val namesRDD = sc.parallelize(namesList)
    val namesInUniqueRDD = namesRDD.intersection(uniqueRDD).collect()

    if (namesInUniqueRDD.isEmpty)
    {
      println("None of the names are in uniqueRDD.")
    }
    else
    {
      println("Names that are in both RDDs:")
      namesInUniqueRDD.foreach(println)
    }

    //all the above names are not in UniqueRDD
    var falsePositiveCount = 0
    namesRDD.collect().foreach { word =>
      if (bf.contains(word)) {
        falsePositiveCount += 1
      }
    }

    println(s"Number of false positives: $falsePositiveCount Out Of ${namesRDD.count()}")
    /*
    AS I expected , about 4 out of 100 non-existent elements
    to incorrectly appear in the Bloom Filter.
    */

    val errorRate = falsePositiveCount.toDouble / namesRDD.count()
    println(s"Error rate: $errorRate")

    sc.stop()
  }

}
