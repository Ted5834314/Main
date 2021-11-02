

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner





class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(-1))

  verify()
}





object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())



  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("threshold: " + args.threshold())
    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    

    
    val thresh=args.threshold()
    val textFile = sc.textFile(args.input())
    var total=textFile
      .map(line => ("*",1.0d)
        ).reduceByKey(_ + _,args.reducers()).collect().head._2



    val interm =textFile
      .flatMap(line => 
        tokenize(line).take(40).distinct.map(p => (p,1.0d)).toList                          
        ).reduceByKey(_ + _,args.reducers())
      

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(40).distinct
        val outcome= scala.collection.mutable.ListBuffer.empty[Float]
        tokens.flatMap(p =>
          
            tokens.map(q => (p,q)).filter{case(p,q) => !p.equals(q)}
        )
        
        
        
        
      })
      .map(all => (all, 1.0d))
      .reduceByKey(_ + _,args.reducers())
      .filter{case ((m,n),f) => f>=thresh}
      .map{case ((x,y),v) =>(x,((x,y),v))}
      .join(interm)
      .map {case(z,(((x,y),v),t)) => (y,((x,y),v,t)) }
      .join(interm)
      .map {case(z,(((x,y),v,t),s)) => ((x,y),(Math.log10(v*total/t/s),v))}
      

      
 
    

    
  


    counts.saveAsTextFile(args.output())
  }
}
