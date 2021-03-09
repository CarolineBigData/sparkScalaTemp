package Bootstrap_a_SparkSession

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions. _

object Spark_Functions extends App with Context {


  val donuts = Seq(("plain donuts",1.5),("vanilla donuts",2.0),("glazed donut",2.5))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("donutName","price")
  df.show()

  val columnNames: Array[String] =df.columns
  columnNames.foreach(name => println(s"$name"))

  val (columnNames1, columnDataTypes) = df.dtypes.unzip
  println(s"DataFrame column names =${columnNames1.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")


  // read Json

  import sparkSession.sqlContext.implicits._
  val tagsDF = sparkSession
    .read
    .option("multiLine",true)
    .option("inferSchema", true)
    .json("src/main/resources/tags_sample.json")

  // explode() function to unwrap the root stackoverflow element.

  val dfTags = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags" )
  dfTags.printSchema()

  dfTags.show(2)

  // tabulate your dataframe by using the select() method.

  val tagDF = dfTags.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  )

  tagDF .select("*")
    .where(array_contains($"frameworks_name","Play Framework")).show()

  //Concatenate DataFrames using join()
  val donuts2 = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))

  val dfDonuts = sparkSession
    .createDataFrame(donuts2)
    .toDF("Id","Donut Name", "Price")
  dfDonuts.show()

  val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
  val dfInventory = sparkSession
    .createDataFrame(inventory)
    .toDF("Id", "Inventory")
  dfInventory.show()

  val dfDonutsInventory = dfDonuts.join(dfInventory, Seq("Id"), "inner")
  dfDonutsInventory.show()

  //Check DataFrame column exists
  val priceColumnExists = dfDonuts.columns.contains("Price")
  println(s"Does price column exist = $priceColumnExists")

  //denormalise an Array columns into separate columns.


  val targets = Seq(("Plain Donut", Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))
  val df_temp1 = sparkSession
    .createDataFrame(targets)
    .toDF("Name", "Prices")

  df_temp1.show()

  val df_temp2 =  df_temp1
    .select($"Name",$"Prices"(0).alias("Low Prices"),$"Prices"(1).alias("High Prices"))
  df_temp2.show()

  //Rename DataFrame column

  val df_temp3 = df_temp2.withColumnRenamed("Name", "DonutName")
  df_temp3.show()

  //Create DataFrame constant column
  val donuts_tmp = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val dfdonuts_tmp = sparkSession.createDataFrame(donuts_tmp).toDF("DonutName","Price")

  val dfdonuts_tmp1= dfdonuts_tmp.withColumn("Tasty",lit(true)).withColumn("Correlation",lit(1))
    .withColumn("Stock_Min_max",typedLit(Seq(100,500)))
  dfdonuts_tmp1.show()

  //DataFrame new column with User Defined Function (UDF)
  val stockMinMax: String => Seq[Int] = (DonutName: String) => DonutName match{
    case "plain donut" => Seq(100,500)
    case "vanilla donut" => Seq(200,400)
    case "glazed donut" => Seq(300,600)
    case _ => Seq(150,150)
  }

  val udfstockMinMax = udf(stockMinMax)
  val donuts_tmp2 = dfdonuts_tmp.withColumn("Stock_Min_max",udfstockMinMax($"DonutName"))
  donuts_tmp2.show()

  //In addition, if you need to return a particular column from the first row of the dataframe, you can also specify the column index:
  // df.first().get(0). Sometimes, though, you may have to refer to a particular column by name as opposed to a column index.
  // In the code snippet below, we make use of the getAs() method to return the value from the dataframe's first row for the Price column:
  // df.first().getAs[Double]("Price").

  val firstRow = donuts_tmp2.first()
  println(s"First row = $firstRow")

  val firstRowColumn1 = donuts_tmp2.first().get(0)
  println(s"First row = $firstRowColumn1")

  val firstRowColumnPrice = donuts_tmp2.first().getAs[Double]("Price")
  println(s"First row column Price = $firstRowColumnPrice")

  val donuts3 = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df_donuts3 = sparkSession
    .createDataFrame(donuts3)
    .toDF("donutName","Price","Purchase_date")
//Format DataFrame column
  df_donuts3
    .withColumn("Price Formatted", format_number($"Price",2))
    .withColumn("Name Formatted",format_string("Awesome %s",$"donutName"))
    .withColumn("Name Upper",upper($"donutName"))
    .withColumn("Name Lower", lower($"donutName"))
    .withColumn("Date Formatted",date_format($"Purchase_date", "yyyyMMdd"))
    .withColumn("Day", dayofmonth($"Purchase_date"))
    .withColumn("Month", month($"Purchase_date"))
    .withColumn("Year", year($"Purchase_date"))
    .show()

  //DataFrame String Functions
  df_donuts3
    .withColumn("Contains donuts",instr($"donutName","donut"))
    .withColumn("Length", length($"donutName"))
    .withColumn("Trim",trim($"donutName"))
    .withColumn("Ltrim", ltrim($"donutName"))
    .withColumn("Rtrim",rtrim($"donutName"))
    .withColumn("Reverse",reverse($"donutName"))
    .withColumn("Substring", substring($"donutName",0,5))
    .withColumn("IsNull",isnull($"donutName"))
    .withColumn("Concat", concat_ws("-", $"donutName",$"Price"))
    .withColumn("InitCap", initcap($"donutName")).show()

  //DataFrame drop null
  val dfWithoutNull = tagDF.na.drop()
  dfWithoutNull.show()

  val firstRow1 = dfWithoutNull.collect().head
  println(firstRow1.getAs[String]("tag_name"))

  val firstRow2 = dfWithoutNull.first()
  println(firstRow2.getAs[String]("tag_name"))





}
