package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q4 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())

		val conf = new SparkConf().setAppName("Q4")
		val sc = new SparkContext(conf)

                val date = args.date()
		
                if (args.text()) {
                  val cust=sc.textFile(args.input() + "/customer.tbl")
                  .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(3).toInt))
                  val bcust=sc.broadcast(cust.collectAsMap())
                  val nat=sc.textFile(args.input() + "/nation.tbl")
                 .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
                  val bnat=sc.broadcast(nat.collectAsMap())
                  val ord=sc.textFile(args.input() + "/orders.tbl")
                  .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1).toInt))

                  val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
                          .filter(line => line.split("\\|")(10).contains(date))
                          .map(line => (line.split("\\|")(0).toInt, 1))
                          .reduceByKey(_+_)

            
                          .cogroup(ord)
                          .filter(_._2._1.size != 0)
                          .map(p => (p._2._1.head,p._2._2.head))
                          .map(p => (bcust.value.apply(p._2),p._1))
                          .reduceByKey(_+_)
      
                          .sortByKey()
                          .collect()
                          .foreach(p => println(p._1, bnat.value.apply(p._1), p._2))
                } else if (args.parquet()) {
                  val sparkSession = SparkSession.builder.getOrCreate
                  val custdf = sparkSession.read.parquet(args.input() + "/customer")
                  val custrdd = custdf.rdd
                  val cust = custrdd
                  .map(line => (line.getInt(0), line.getInt(3)))
                  val bcust = sc.broadcast(cust.collectAsMap())
                  val natdf = sparkSession.read.parquet(args.input() + "/nation")
                  val natrdd = natdf.rdd
                  val nat = natrdd
                 .map(line => (line.getInt(0), line.getString(1)))
                  val bnat = sc.broadcast(nat.collectAsMap())
                  val ordf = sparkSession.read.parquet(args.input() + "/orders")
                  val ordrdd = ordf.rdd
                  val ord = ordrdd
                   .map(line => (line.getInt(0), line.getInt(1)))

                  val lineitemdf = sparkSession.read.parquet(args.input() + "/lineitem")
                   val lineitemrdd = lineitemdf.rdd
                  val lineitem = lineitemrdd
                                 .filter(line => line.getString(10).contains(date))
                                 .map(line => (line.getInt(0), 1))
                                 .reduceByKey(_+_)
                                 .cogroup(ord)
                                 .filter(_._2._1.size != 0)
                                 .map(p => (p._2._1.head,p._2._2.head))
                                 .map(p => (bcust.value.apply(p._2),p._1))
                                 .reduceByKey(_+_)

                                 .sortByKey()
                                 .collect()
                                 .foreach(p => println(p._1, bnat.value.apply(p._1), p._2))
        
    }
	}
}
