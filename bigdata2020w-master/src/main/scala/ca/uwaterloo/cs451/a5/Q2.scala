package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q2 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
	  val args = new Conf(argv)
          log.info("Input: " + args.input())	
	  val conf = new SparkConf().setAppName("Q2")
	  val sc = new SparkContext(conf)
	  val date = args.date()

        if (args.text()) {
       	  val ord=sc.textFile(args.input() + "/orders.tbl")
  			.map(line => (line.split("\\|")(0).toInt, line.split("\\|")(6))) 
          val lineitem=sc.textFile(args.input() + "/lineitem.tbl")
                        .filter(line => line.split("\\|")(10).contains(date))
  			.map(line => (line.split("\\|")(0).toInt, line.split("\\|")(10)))
  			.cogroup(ord)
  			.filter(_._2._1.size != 0)
  			.sortByKey()
  			.take(20)
  			.map(p => (p._2._2.head, p._1))
  			.foreach(println)
       } else if (args.parquet()) {
            val sparkSession=SparkSession.builder.getOrCreate
             val ordf = sparkSession.read.parquet(args.input() + "/orders")
             val ordrdd = ordf.rdd
             val ord = ordrdd
             .map(line => (line.getInt(0), line.getString(6)))      
             val linedf = sparkSession.read.parquet(args.input() + "/lineitem")
             val linerdd = linedf.rdd
             val lineitem = linerdd
                             .filter(line => line.getString(10).contains(date))
                             .map(line => (line.getInt(0), line.getString(10)))
                             .cogroup(ord)
                             .filter(_._2._1.size != 0)
                             .sortByKey()
                             .take(20)
                             .map(p => (p._2._2.head, p._1))
                             .foreach(println)
         }
	}
}
