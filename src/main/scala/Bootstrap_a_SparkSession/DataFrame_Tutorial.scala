package Bootstrap_a_SparkSession
package Bootstrap_a_SparkSession

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession

trait Context {

  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.core.max","2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  //Spark session is a unified entry point of a spark application
  // from Spark 2.0.
  // It provides a way to interact with various
  // spark's functionality with a lesser number of constructs.
  // Instead of having a spark context, hive context, SQL context,
  // now all of it is encapsulated in a Spark session.
}


//DataFrame SQL Query:
object DataFrame_Tutorial extends App with Context{

  val dfTags = sparkSession
    .read
    .option("header","true")
    .option("inferSchema","true")
    .csv("src/main/resources/question_tags_10k.csv")
    .toDF("id","tag")

  dfTags.show(10)

  dfTags.printSchema()

  dfTags.select("id","tag").show(10)

  println(s" The number of php tags is ${dfTags.filter("tag=='php'").count()}")

  dfTags.filter("tag like 's%'").show(10)

  dfTags.filter("tag like 's%'").filter("id ==25 or id==108").show(5)

  dfTags.filter("id in (25,100)").show()

  dfTags.groupBy("tag").count().show(5)

  dfTags.groupBy("tag").count().filter("count >5").orderBy("tag").show(5)

  // DataFrame Query: Cast columns to specific data type
  //Although we've passed in the inferSchema option, Spark did not fully match the data type for some of our columns.
 val dfQuestion = sparkSession
   .read
   .option("header","true")
   .option("inferSchema","true")
   .option("dateFormat","yyyy-MM-dd HH:mm:ss")
   .csv("src/main/resources/questions_10K.csv")
   .toDF("Id","CreationDate","CloseDate","DeletionDate","Score","OwnerUserId","AnswerCount")

  dfQuestion.printSchema()

  //You can use the Spark CAST method to convert data frame column data type to required format.
  val dfQuestion_temp = dfQuestion.select(
    dfQuestion("Id").cast("integer"),
    dfQuestion("CreationDate").cast("timestamp"),
    dfQuestion("CloseDate").cast("timestamp"),
    dfQuestion("DeletionDate").cast("date"),
    dfQuestion("Score").cast("integer"),
    dfQuestion("OwnerUserId").cast("integer"),
    dfQuestion("AnswerCount").cast("integer")
  )
  dfQuestion_temp.printSchema()
  dfQuestion_temp.show(5)

  val dfQuestionSubset = dfQuestion_temp.filter("score >400 and score <410").toDF()
  dfQuestionSubset.show(5)

  dfQuestionSubset
    .join(dfTags, dfTags("id") === dfQuestionSubset("Id"))
    .show(10)

  dfQuestionSubset
    .join(dfTags, Seq("id"), "inner")
    .show(5)

  dfQuestionSubset
    .join(dfTags, Seq("id"), "left_outer")
    .show(5)

  dfTags.select("tag").distinct().show(10)


}

