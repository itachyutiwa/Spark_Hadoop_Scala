import org.apache.spark.sql._
import org.apache.spark._
object SparkApp {

  def sessionSpark(): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val ss =SparkSession.builder()
      .master(master = "local[*]")
      .appName(name = "MonPremierProject")
      .getOrCreate()

    val rdd1 = ss.sparkContext.parallelize(Seq("Its my first scala application. Im a student of INPHB  "))
    rdd1.foreach(f => println(f))
    val df_1 = ss.read
      .format(source="csv")
      .option("inferSchema", "true")
      .option("delimiter",";")
      .option("header","true")
      .csv(path = "C:\\Users\\akone\\IdeaProjects\\Spark Hadoop Scala\\orders.csv")

    df_1.show(numRows = 10)
    df_1.printSchema()
    df_1.createOrReplaceTempView(viewName = "orders")
    ss.sql(sqlText = "SELECT * FROM orders WHERE city = 'NEWTON' ").show()

  }

  def main(args: Array[String]): Unit ={
    sessionSpark()
  }

}
