package Bootstrap_a_SparkSession

import org.apache.spark.sql.Dataset

object DataFrame_Operations_Introduction extends App with Context{

  val dfTags = sparkSession
    .read
    .option("header",true)
    .option("inferSchema",true)
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id","tag")

  dfTags.show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header",true)
    .option("inferSchema",true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score <410" )
    .join(dfTags,"id")
    .select("owner_userid", "tag", "creation_date", "score")
    .toDF()

  dfQuestions.show(10)

  dfQuestions.printSchema()

  //Convert DataFrame row to Scala case class
  //There is also a new as[U](implicit arg0: Encoder[U]):
  // Dataset[U] which is used to convert a DataFrame to a DataSet of a given type.
  case class Tag(id: Int, tag: String)

  import sparkSession.implicits._
  val dfTagsOfTag: Dataset[Tag] =dfTags.as[Tag]

  dfTagsOfTag.take(10).foreach(t=> println(s"id=${t.id},tag=${t.tag}"))

  //Create DataFrame from collection
  val seqTags = Seq(
    1 -> "so_java",
    2 -> "so_jsp",
    3 -> "so_erlang"
  )

  import sparkSession.implicits._
  val dfMoreTags = seqTags.toDF("id","tag")
  dfMoreTags.show(2)

  //DataFrame Union
  val dfUnionOfTags = dfTags
    .union(dfMoreTags)
    .filter("id in (1,3)")

  dfUnionOfTags.show(2)

  //DataFrame Intersection
  val dfIntersectionTags = dfMoreTags
    .intersect(dfUnionOfTags)
    .show(10)

  //Append column to DataFrame using withColumn()
  import org.apache.spark.sql.functions._
  val dfSplitColumn = dfMoreTags
    .withColumn("tmp",split($"tag","_"))
    .select($"id",$"tag",$"tmp".getItem(0).as("so_prefix"),$"tmp".getItem(1).as("so_tag")).drop("tmp")
  dfSplitColumn.show(10)






}
