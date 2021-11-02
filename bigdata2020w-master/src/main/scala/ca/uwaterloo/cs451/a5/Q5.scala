package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q5 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())

		val conf = new SparkConf().setAppName("Q4")
		val sc = new SparkContext(conf)

                
		
                if (args.text()) {
                  val cust=sc.textFile(args.input() + "/customer.tbl")
                  
                  .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(3).toInt))
                  .filter(p => (p._2==3||p._2==24))

                  val bcust=sc.broadcast(cust.collectAsMap())
                  val nat=sc.textFile(args.input() + "/nation.tbl")
                 .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
                  val bnat=sc.broadcast(nat.collectAsMap())
                  val ord=sc.textFile(args.input() + "/orders.tbl")
                  .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1).toInt))

                  val lineitem=sc.textFile(args.input() + "/lineitem.tbl")
                          
                          .map(line => {
                                val key=line.split("\\|")(0).toInt
                                val date=line.split("\\|")(10)
                                ((key, date.substring(0, date.lastIndexOf('-'))),1)
                                })
                          .reduceByKey(_+_)
                          .map(p => (p._1._1,(p._1._2,p._2)))
            
                          .cogroup(ord)
                          .filter(_._2._1.size != 0)
                          .flatMap(p => {
                                var list = scala.collection.mutable.ListBuffer[((String, Int), String)]()
                                 if (bcust.value.contains(p._2._2.head)) {
                                 val natkey=bcust.value(p._2._2.head)
                                 val natname=bnat.value(natkey)
                                 val stuff=p._2._1.iterator
                                 while (stuff.hasNext) {
                                    list+=((stuff.next(), natname))
                                  }
                                  }
                                 list
                                    })
                          .map(p => ((p._1._1,p._2),p._1._2))
                          .reduceByKey(_+_)
                          
                          .sortByKey()
                          
                          .map(p =>(p._1._1,p._1._2,p._2))
                          .collect()
                          .foreach(p => println(p))
                } else if (args.parquet()) {
                  val sparkSession=SparkSession.builder.getOrCreate
                  val custdf=sparkSession.read.parquet(args.input() + "/customer")
                  val custrdd=custdf.rdd
                  val cust=custrdd
                  .map(line => (line.getInt(0), line.getInt(3)))
                  .filter(p => (p._2==3||p._2==24))

                  val bcust=sc.broadcast(cust.collectAsMap())
                  val natdf=sparkSession.read.parquet(args.input() + "/nation")
                  val natrdd=natdf.rdd
                  val nat=natrdd
                 .map(line => (line.getInt(0), line.getString(1)))
                  val bnat=sc.broadcast(nat.collectAsMap())
                  val ordf=sparkSession.read.parquet(args.input() + "/orders")
                  val ordrdd=ordf.rdd
                  val ord=ordrdd
                   .map(line => (line.getInt(0), line.getInt(1)))

                  val lineitemdf=sparkSession.read.parquet(args.input() + "/lineitem")
                   val lineitemrdd=lineitemdf.rdd
                   val lineitem=lineitemrdd

                          .map(line => {
                                val key=line.getInt(0)
                                val date=line.getString(10)
                                ((key, date.substring(0, date.lastIndexOf('-'))),1)
                                })
                          .reduceByKey(_+_)
                          .map(p => (p._1._1,(p._1._2,p._2)))

                          .cogroup(ord)
                          .filter(_._2._1.size != 0)
                          .flatMap(p => {
                                var list = scala.collection.mutable.ListBuffer[((String, Int), String)]()
                                 if (bcust.value.contains(p._2._2.head)) {
                                 val natkey=bcust.value(p._2._2.head)
                                 val natname=bnat.value(natkey)
                                 val stuff=p._2._1.iterator
                                 while (stuff.hasNext) {
                                    list+=((stuff.next(), natname))
                                  }
                                  }
                                 list
                                    })
                          .map(p => ((p._1._1,p._2),p._1._2))
                          .reduceByKey(_+_)

                          .sortByKey()

                          .map(p =>(p._1._1,p._1._2,p._2))
                          .collect()
                          .foreach(p => println(p))
        
    }
	}
}
