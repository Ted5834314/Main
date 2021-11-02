

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner




class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}






object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

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
          tokens.sliding(2).map(p => (p.head,Map(p.last->1.0d))).toList
          
        
        
        }
        else List()
      })
        .reduceByKey((m1,m2) => {m1 ++ m2.map{
        case (k,v) =>
        k -> (v + m1.getOrElse(k,0.0d)) 
      }},args.reducers()
      )
        .map{
          case (k,amap) => {
        var sum=0.0d
        for((k,v)<-amap) {
        sum=sum+v
        }

        k -> amap.map{
          case (k,v)=>{k -> v/sum}}
        }
  }
 
    

    
  


    counts.saveAsTextFile(args.output())
  }
}
