package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q7 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		
		val conf = new SparkConf().setAppName("Q7")
		val sc = new SparkContext(conf)

               val date = args.date()
		
              if (args.text()) {
                val cust=sc.textFile(args.input() + "/customer.tbl")
               .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
                val bcust=sc.broadcast(cust.collectAsMap())
                val ord=sc.textFile(args.input() + "/orders.tbl")
                         .filter(line => (line.split("\\|")(4)<date))
                         .map(line => (line.split("\\|")(0).toInt,(line.split("\\|")(1).toInt,line.split("\\|")(4),line.split("\\|")(7).toInt)))
                val lineitem=sc.textFile(args.input() + "/lineitem.tbl")
                             .filter(line => line.split("\\|")(10)>date)
                             .map(line => (line.split("\\|")(0).toInt,line.split("\\|")(5).toDouble*(1-line.split("\\|")(6).toDouble)))
                             .reduceByKey(_+_)
                             .cogroup(ord)
                             .filter(p => p._2._1.size!=0&&p._2._2.size!=0)
                             .map(p => (p._2._1.head,(bcust.value.apply(p._2._2.head._1),p._1,p._2._2.head._2,p._2._2.head._3)))
                             .sortByKey(false)
                             .take(10)
                             
                             .foreach(p => println(p._2._1,p._2._2,p._1,p._2._3,p._2._4))
              }  else if (args.parquet()) {
                    val sparkSession = SparkSession.builder.getOrCreate
                    val custdf=sparkSession.read.parquet(args.input() + "/customer")
                    val custrdd=custdf.rdd
                    val cust=custrdd
                             .map(line => (line.getInt(0), line.getString(1)))
                    val bcust=sc.broadcast(cust.collectAsMap())
                    val ordf=sparkSession.read.parquet(args.input() + "/orders")
                    val ordrdd=ordf.rdd
                    val ord=ordrdd
                            .filter(line => (line.getString(4)<date))
                            .map(line => (line.getInt(0),(line.getInt(1),line.getString(4),line.getInt(7))))
                    val lineitemdf=sparkSession.read.parquet(args.input() + "/lineitem")
                    val lineitemrdd=lineitemdf.rdd
                    val lineitem=lineitemrdd
                                .filter(line => line.getString(10)>date)
                                .map(line => (line.getInt(0),line.getDouble(5)*(1-line.getDouble(6))))
                                .reduceByKey(_+_)
                                .cogroup(ord)
                                .filter(p => p._2._1.size!=0&&p._2._2.size!=0)
                                .map(p => (p._2._1.head,(bcust.value.apply(p._2._2.head._1),p._1,p._2._2.head._2,p._2._2.head._3)))
                                .sortByKey(false)
                                .take(10)

                             .foreach(p => println(p._2._1,p._2._2,p._1,p._2._3,p._2._4))

    }
	}
}

