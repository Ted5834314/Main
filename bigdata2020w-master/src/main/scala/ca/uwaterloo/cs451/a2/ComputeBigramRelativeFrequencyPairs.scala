

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner





class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}


class MyPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = key match {
    case null => 0
    case (k1,k2) => (k1.hashCode & Int.MaxValue) % numPartitions
  }
  override def equals(other: Any): Boolean = other match {
    case h: MyPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
  override def hashCode: Int = numPartitions
}



object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())



  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    

    

    val textFile = sc.textFile(args.input())
    var total=0.0d

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val bigram=tokens.sliding(2).map(p => (p.head,p.last)).toList
          val single=tokens.sliding(2).map(p =>(p.head,"*")).toList
            bigram ++ single    
        
        }
        else List()
      })
      .map(all => (all, 1.0d))
      .reduceByKey(_ + _,args.reducers())
      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
      .map(all => all._1 
        match {

        case (tok,"*") => {
        total = all._2 
        (all._1,all._2)
        
        }
        case (tok1,tok2) => (all._1,all._2 / total)
      })
 
    

    
  


    counts.saveAsTextFile(args.output())
  }
}
