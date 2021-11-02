package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q3 {
  val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
	    val args = new Conf(argv)
	    log.info("Input: " + args.input())
	    val conf = new SparkConf().setAppName("Q3")
	    val sc = new SparkContext(conf)
            val date = args.date()
	    if (args.text()) {
              val part=sc.textFile(args.input() + "/part.tbl")
             .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))

              val bpart=sc.broadcast(part.collectAsMap())
              val supplier=sc.textFile(args.input() + "/supplier.tbl")
              .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))

              val bsupplier=sc.broadcast(supplier.collectAsMap())
              val lineitem=sc.textFile(args.input() + "/lineitem.tbl")
               .filter(line => line.split("\\|")(10).contains(date))
               .map(line => {
                     val lines=line.split("\\|")
                     
                     val pk=lines(1).toInt
                     val sk=lines(2).toInt
                     val ok=lines(0).toInt
                     
                     (ok, (bpart.value.apply(pk), bsupplier.value.apply(sk)))
               })
               .sortByKey()
               .take(20)
               .foreach(p => println(p._1, p._2._1, p._2._2))
            } else if (args.parquet()) {
               val sparkSession = SparkSession.builder.getOrCreate
               val partdf = sparkSession.read.parquet(args.input() + "/part")
               val partrdd = partdf.rdd
               val part = partrdd
               .map(line => (line.getInt(0), line.getString(1)))

                val bpart = sc.broadcast(part.collectAsMap())
                 val supplierdf = sparkSession.read.parquet(args.input() + "/supplier")
                val supplierdd = supplierdf.rdd
                 val supplier = supplierdd
                  .map(line => (line.getInt(0), line.getString(1)))

                 val bsupplier = sc.broadcast(supplier.collectAsMap())
                  val lineitemdf = sparkSession.read.parquet(args.input() + "/lineitem")
                  val lineitemrdd = lineitemdf.rdd
                  val lineitem = lineitemrdd
                 .filter(line => line.getString(10).contains(date))
                 .map(line => {
                        
                     val pk=line.getInt(1)
                     val sk=line.getInt(2)
                     val ok=line.getInt(0)

                     (ok, (bpart.value.apply(pk), bsupplier.value.apply(sk)))

                  })
                  .sortByKey()
                  .take(20)
                  .foreach(p => println(p._1, p._2._1, p._2._2))
              }
	}
}
