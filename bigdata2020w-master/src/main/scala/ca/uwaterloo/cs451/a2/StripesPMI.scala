

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner





class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(-1))

  verify()
}



        


object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())



  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("threshold: " + args.threshold())
    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    

    
    val thresh=args.threshold()
    val textFile = sc.textFile(args.input())
    var total=textFile
      .map(line => ("*",1.0d)
        ).reduceByKey(_ + _,args.reducers()).collect().head._2



    val interm =sc.broadcast(textFile
      .flatMap(line => 
        tokenize(line).take(40).distinct.map(p => (p,1.0d)).toList                          
        ).reduceByKey(_ + _,args.reducers()).collectAsMap())
      

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(40).distinct
        
        tokens.map(p =>
          
            (p,tokens.filter(q  => !p.equals(q)).map(q => (q,1.0d)).toMap))
      })
      .reduceByKey((m1,m2) => {m1 ++ m2.map{
        case (k,v) =>
        k -> (v + m1.getOrElse(k,0.0d))
      }},args.reducers()).map{case (p, mp) =>(p,mp.filter((t)=>t._2>=thresh))}
        .map{case (p,mp) =>
        {  
          val x=interm.value.apply(p)
          (p,mp.map{case (a,count) => 
            val y=interm.value.apply(a)
            (a,(Math.log10(count*total/x/y),count))
          }.toMap)

        
        }}
      
      
      

      
 
    

    
  


    counts.saveAsTextFile(args.output())
  }
}
