package ca.uwaterloo.cs451.a6;

import collection.mutable.HashMap
import scala.collection.JavaConverters._
import java.util.StringTokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.exp

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output, method)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  val w1=HashMap[Int, Double]()
  val w2=HashMap[Int, Double]()
  val w3=HashMap[Int, Double]()


  def spamminess(features: Array[Int], w: HashMap[Int, Double]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

  
    val conf = new SparkConf().setAppName("Apply Ensemble Spam Classifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

  
    sc.textFile(args.model()+"/part-00000")
      .map(line => {
             val a = line.split(",")
             (a(0).substring(1),a(1).substring(0,a(1).length-1))
      })
      .collect()
      .foreach(p => {
        w1(p._1.toInt)=p._2.toDouble
      })

  
    sc.textFile(args.model()+"/part-00001")
      .map(line => {
             val a = line.split(",")
             (a(0).substring(1),a(1).substring(0,a(1).length-1))
      })
      .collect()
      .foreach(p => {
        w2(p._1.toInt)=p._2.toDouble
      })

    //load model 3
    sc.textFile(args.model()+"/part-00002")
      .map(line => {
             val a = line.split(",")
             (a(0).substring(1),a(1).substring(0,a(1).length-1))
      })
      .collect()
      .foreach(p => {
        w3(p._1.toInt)=p._2.toDouble
      })

    val bw1=sc.broadcast(w1)
    val bw2=sc.broadcast(w2)
    val bw3=sc.broadcast(w3)
    val method=args.method()

  
    val textFile = sc.textFile(args.input())
    textFile
      .map(line => {
        val tokens=line.split(" ")
        val featuresvec=tokens.slice(2,tokens.length).map(_.toInt)
        val score1=spamminess(featuresvec,bw1.value)
        val score2=spamminess(featuresvec,bw2.value)
        val score3=spamminess(featuresvec,bw3.value)
        var score=(score1+score2+score3)/3
        if(method=="vote") {
          var vote=0
          if(score1>0) vote+=1 else vote-=1
          if(score2>0) vote+=1 else vote-=1
          if(score3>0) vote+=1 else vote-=1
          score=vote
          
          
        }

        var spam="ham"
        if(score>0) {
          spam="spam"
        }
    
        (tokens(0),tokens(1),score,spam)
      })
      .saveAsTextFile(args.output())

  }
}
