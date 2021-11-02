package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q6 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
	    val args = new Conf(argv)

	    log.info("Input: " + args.input())
		

	    val conf = new SparkConf().setAppName("Q6")
	    val sc = new SparkContext(conf)

	    val date = args.date()

            if (args.text()) {
  		val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
  			.filter(line => line.split("\\|")(10).contains(date))
                        .map(line => {
                         val stuff=line.split("\\|")
                         val returnflag=stuff(8)
                         val linestatus=stuff(9)
                         val qty=stuff(4).toDouble
                         val extprice=stuff(5).toDouble
                         val disc=stuff(6).toDouble
                         val tax=stuff(7).toDouble
                         val sdp=extprice*(1-disc)
                         val sc=sdp*(1+tax)
                         ((returnflag,linestatus),(qty,extprice,sdp,sc,disc,1))
                         })
                        .reduceByKey((x, y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6))
                        .collect()
                        .foreach(p => {
                        val count=p._2._6
                        println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1/p._2._6, p._2._2/p._2._6, p._2._5/p._2._6, p._2._6)
                        })
            } else if (args.parquet()) {
                 val sparkSession=SparkSession.builder.getOrCreate
                 val lineitemdf=sparkSession.read.parquet(args.input() + "/lineitem")
                 val lineitemrdd=lineitemdf.rdd
                 val lineitem=lineitemrdd
                                .filter(line => line.getString(10).contains(date))
                                 .map(line => {
                                 
                                 val returnflag=line.getString(8)
                                 val linestatus=line.getString(9)
                                 val qty=line.getDouble(4)
                                 val extprice=line.getDouble(5)
                                 val disc=line.getDouble(6)
                                 val tax=line.getDouble(7)
                                 val sdp=extprice*(1-disc)
                                 val sc=sdp*(1+tax)
                                 ((returnflag,linestatus),(qty,extprice,sdp,sc,disc,1))
                                 })
                                .reduceByKey((x, y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6))
                                .collect()
                               .foreach(p => {
                               val count=p._2._6
                               println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1/p._2._6, p._2._2/p._2._6, p._2._5/p._2._6, p._2._6)
                                })

            }
	}
}
