package com.svds.hashtag

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Time
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._

class BattleSpec extends FlatSpec with BeforeAndAfter with ShouldMatchers with MockitoSugar {

  var sc: SparkContext = null

  before {
    sc = new SparkContext("local", "test")
  }

  after {
    sc.stop()
    sc = null
  }

  "Battle.getSearchTerms" should "return a list with hashtags first" in {
    val options = Options(hashtags = Set("foobar"), terms = Set("bla"))
    Battle.getSearchTerms(options) shouldBe List("#foobar", "bla")
  }

  "Battle.search" should "return an RDD of the words being searched for" in {
    val sampleTweets = Seq(
      "@User1: This is a tweet with a #spark and another spark.",
      "@Spark: This post is not mentioning s-p-a-r-k but its in the name.",
      "@User2: And this is the last #tweet."
    )
    val input = sc.parallelize(sampleTweets)
    val result = Battle.search(List("spark", "#Spark", "Tweet"))(input).collect()
    result.sorted shouldBe Array("#Spark", "Tweet", "Tweet", "spark", "spark")
  }

  "Battle.output" should "output the counts it is given filling in blanks" in {
    val inputCounts = Seq("a" -> 1L, "b" -> 2L)
    val searchTerms = List("a", "b", "c")
    val time = Time(12345000)
    val mockOutput = mock[Output]
    Battle.output(searchTerms, Set(mockOutput))(sc.parallelize(inputCounts), time)
    verify(mockOutput).write("a", 1L, 12345L)
    verify(mockOutput).write("b", 2L, 12345L)
    verify(mockOutput).write("c", 0L, 12345L)
    verify(mockOutput).flush()
  }

  "Output.write" should "format strings correctly" in {
    class MinimalOutput extends Output {
      def write(s: String): Unit = ()

      def close(): Unit = ()

      def flush(): Unit = ()
    }
    val mockOutput = mock[MinimalOutput]
    when(mockOutput.write(anyString(), anyLong(), anyLong())).thenCallRealMethod()
    when(mockOutput.format).thenCallRealMethod()
    mockOutput.write("a", 1L, 12345L)
    verify(mockOutput).write("a 1 12345")
  }
}
