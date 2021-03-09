package Bootstrap_a_SparkSession
import org.apache.spark
import org.apache.spark.sql.{SparkSession, _}
object sample extends App with Context{
  val soql = "select id, name, amount from opportunity"

  val sfDF = sparkSession.
    read.
    format("com.springml.spark.salesforce").
    option("username", "mingzeng0226-1nlz@force.com").
    option("password", "774152mingUJK0JtLi9JAYQKCx6PVWnWMd").//<salesforce login password><security token>
    option("soql", soql).
    option("version", "37.0").
    load()

  sfDF.show(2)
}