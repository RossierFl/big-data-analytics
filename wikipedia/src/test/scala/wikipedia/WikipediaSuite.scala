package wikipedia

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import WikipediaRanking._

@RunWith(classOf[JUnitRunner])
class WikipediaSuite extends FunSuite with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("'occurrencesOfLang' should work for (specific) RDD with one element") {
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val res = occurrencesOfLang("Java", rdd)
    assert(res == 1, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
  }
  
  test("'occurrencesOfLang(java)' occurrencesOfLang(java) with javascript in text should be 0") {
    val javardd = sc.parallelize(Seq(WikipediaArticle("title", "Javascript is a very shitty scripting programming language")))
    val secondRes = occurrencesOfLang("Java", javardd)
    assert(secondRes == 0, "occurrencesOfLang(java) with javascript in text should be 0")
  }

  test("'rankLangs' should work for RDD with two elements") {
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great"), WikipediaArticle("2", "Java is OK, but Scala is cooler")))
    val ranked = rankLangs(langs, rdd)
    assert(ranked.head._1 == "Scala")
  }

  test("'makeIndex' creates a simple index with two entries") {
    val langs = List("Scala", "Java","C++")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","Java is very verbose but usefull sometimes")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    //index.foreach(println)
    assert(index.count() == 2)
  }

  test("'rankLangsUsingIndex' should work for a simple RDD with three elements") {
    val langs = List("Scala", "Java","C")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","C is powerfull, powerfull to shoot you in the foot")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    ranked.foreach(println)
    assert(ranked.head._1 == "Scala")
  }

  test("'rankLangsReduceByKey' should work for a simple RDD with four elements") {
    val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","The cool kids like Haskell more than Java"),
        WikipediaArticle("5","Java is for enterprise developers")
      )
    val rdd = sc.parallelize(articles)
    val ranked = rankLangsReduceByKey(langs, rdd)
    assert(ranked.head._1 == "Java")
  }


}

