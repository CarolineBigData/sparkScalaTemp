package Bootstrap_a_SparkSession

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context{

  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.core.max","2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

}
object SparkSQL_Tutorial extends App with Context{

  val dfTags = sparkSession
    .read
    .option("header","true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id","tag")

  // we will register it as a temporary table in Spark's catalog and name the table so_tags
  dfTags.createOrReplaceTempView("so_tags")

  // List all tables in Spark's catalog
  sparkSession.catalog.listTables().show()

  // List all tables in Spark's catalog using Spark SQL
  sparkSession.sql("show tables").show()

  sparkSession.sql("select * from so_tags limit 10").show()

  sparkSession.sql("select * from so_tags where tag ='php'").show(5)

  sparkSession.sql(
    """select
      |count(*) as count_php from so_tags
      |where tag='php'
      |""".stripMargin).show(5)

  sparkSession.sql(
    """
      |select *
      |from so_tags
      |where tag like 's%'
      |""".stripMargin
  ).show(5)

  sparkSession.sql(
    """
      |select *
      |from so_tags
      |where tag like 's%'
      |and (id =108 or id = 25)
      |""".stripMargin
  ).show()

  sparkSession.sql(
    """
      |select *
      |from so_tags
      |where tag like 's%'
      |and id in (25,108)
      |""".stripMargin).show(5)

  sparkSession.sql(
    """
      |select tag, count(*)
      |from so_tags
      |group by tag
      |""".stripMargin).show(5)

  sparkSession.sql(
    """
      |select tag, count(*)
      |from so_tags
      |group by tag
      |having count(*) >5
      |""".stripMargin
  ).show(5)

  val dfQuestionCSV = sparkSession.read
    .option("header","true")
    .option("inferSchema","true")
    .option("dataFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("Id","CreationDate","CloseDate","DeletionDate","Score","OwnerUserId","AnswerCount")


  val dfQuestion = dfQuestionCSV.select(
    dfQuestionCSV("Id").cast("integer"),
    dfQuestionCSV("CreationDate").cast("timestamp"),
    dfQuestionCSV("CloseDate").cast("timestamp"),
    dfQuestionCSV("DeletionDate").cast("date"),
    dfQuestionCSV("Score").cast("integer"),
    dfQuestionCSV("OwnerUserId").cast("integer"),
    dfQuestionCSV("AnswerCount").cast("integer")
  )

  val dfQuestionSubset = dfQuestion.filter("score> 400 and score< 410")

  dfQuestionSubset.createOrReplaceTempView("so_question")

  sparkSession.catalog.listTables().show()

  sparkSession.sql(
    """select t.* , q.*
      |from so_tags t
      |join so_question q
      |on t.id = q.Id
      |""".stripMargin).show(5)

  sparkSession.sql(
    """
      |select t.*, q.*
      |from so_tags t
      |left join so_question q
      |on t.id = q.Id
      |""".stripMargin).show(5)

  sparkSession.sql("select distinct tag from so_tags ").show(5)

  // Function to prefix a String with so_ short for StackOverflow
  def prefixStackoverflow(s: String): String = s"so_$s"

  sparkSession.udf.register("prefix_so", prefixStackoverflow _)

  sparkSession.sql("select id, prefix_so(tag) from so_tags").show(3)


}
