package Bootstrap_a_SparkSession

object DataFrameStatistics_Tutorial extends App with Context {

  // Create a dataframe from tags file question_tags_10K.csv
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  // Create a dataframe from questions file questions_10K.csv
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  // cast columns to data types
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  // Average Max Min Mean Sum
  import org.apache.spark.sql.functions._
  dfQuestions.select(avg("score")).show(3)

  dfQuestions.select(max("score")).show(1)

  dfQuestions.select(min("score")).show(1)

  dfQuestions.select(mean("score")).show(1)

  dfQuestions.select(sum("score")).show(1)

  //Group by with statistics
  dfQuestions.filter("id > 400 and id <450")
    .filter("owner_userid is not null")
    .join(dfTags, "id")
    .groupBy("owner_userid")
    .agg(sum("score"), max("answer_count")).show()

  //f you are looking for a quick shortcut to compute the count, mean, standard deviation, min and max values from a DataFrame,
  // then you can use the describe() method

  val dfQuestionsStatistics = dfQuestions.describe()
  dfQuestionsStatistics.show()

  //// Correlation
  val correlation = dfQuestions.stat.corr("score","answer_count")
  println(s"The correlation between score and answer_count is $correlation")

 // covariance
  val covariance = dfQuestions.stat.cov("score","answer_count")
  println(s"The covariance between score and answer_count is $covariance")

  // Frequent Items
  val dfFrequentItems = dfQuestions.stat.freqItems(Seq("answer_count"))
  dfFrequentItems.show()

  // Crosstab
  val dfScoreByUser_id = dfQuestions
    .filter("owner_userid >1 and owner_userid <10")
    .stat
    .crosstab("score","owner_userid")
  dfScoreByUser_id.show()

  //Stratified sampling using sampleBy
  //To start with, we will filter the dataframe dfQuestions to only include rows where answer_count is in (5, 10, 20).
  // We then print the number of rows matching each answer_count
  // so that we get an initial visual representation of the new dataframe dfQuestionsByAnswerCount.

  val dfQuestionsByAnswerCount = dfQuestions
    .filter("owner_userid>0")
    .filter("answer_count in (5,10,20)" )

  dfQuestionsByAnswerCount.groupBy("answer_count").count().show()

  // Create a fraction map where we are only interested:
  // - 50% of the rows that have answer_count = 5
  // - 10% of the rows that have answer_count = 10
  // - 100% of the rows that have answer_count = 20
  // Note also that fractions should be in the range [0, 1]

  val fractionKeyMap = Map(5 -> 0.5, 10-> 0.1, 20->1.0) //设定抽样格式

  // Stratified sample using the fractionKeyMap.
  //Returns a stratified sample without replacement
  //sampleBy(x, col, fractions, seed)

  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count",fractionKeyMap,0 )
    .groupBy("answer_count")
    .count()
    .show()

   //Hive千分位函数percentile()和percentile_approx()
  // percentile(col, p)  传入两个参数，第一个参数类型必须是int，一般是某一列的数据， 返回的是col列的第p分位的值。
  // percentile_approx(col,p,B) 传入三个参数，col列是数值类型都可以，B用来控制内存消耗的精度。实际col中distinct的值<B返回的时精确的值。
  // percentile(col, 0.1)   就相当于col列上第10%的那个值]

  //The first parameter of the approxQuantile() method is the column of your dataframe on which to run the statistics,
  // the second parameter is an Array of quantile probabilities and the third parameter is a precision error factor.

  //In the example below, we will find the minimum, median and maximum from the score column and as such we will pass an Array of probabilities Array(0, 0.5, 1) which represents:
  //0 = minimum
  //0.5 = median
  //1 = maximum
  // where the last parameter is a relative error. The lower the number the more accurate results and more expensive computation.

  // Approximate Quantile
  val quantiles = dfQuestions
    .stat
    .approxQuantile("score",Array(0,0.5,1.0),0.25)
  println(s"Quantiles segments =${quantiles.toSeq}")

  dfQuestions.createOrReplaceTempView("so_questions")
  sparkSession.sql("select min(score), percentile_approx(score, 0.25), max(score) from so_questions").show()


}
