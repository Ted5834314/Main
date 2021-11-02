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

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle", required = false)
  verify()
}

object TrainSpamClassifier {
  val log=Logger.getLogger(getClass().getName())
  val w=HashMap[Int, Double]()




  def spamminess(features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }


  def main(argv: Array[String]) {
    val args = new Conf(argv)  
    val conf = new SparkConf().setAppName("Train Spam Classifier")
    val sc = new SparkContext(conf)
    val mdir=new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(mdir, true)
    var textFile = sc.textFile(args.input())
    val delta=0.002


    if (args.shuffle() == true) {
      textFile=textFile
                .map(line => {
                       val r=scala.util.Random
                       (r.nextInt,line)
                     })
                .sortByKey()
                .map(p => p._2)
    }


    textFile
      .map(line => {
             val tokens=line.split(" ")
             var spam=0
             if(tokens(1)=="spam") {
               spam=1
             }     
             (0,(tokens(0),spam,tokens.slice(2,tokens.length).map(_.toInt)))})      
      .groupByKey(1)
      .flatMap(p => {
        p._2.foreach(p1 =>{
          val score=spamminess(p1._3)
          val prob=1.0d/(1+exp(-score))
          p1._3.foreach(f => {
            if(w.contains(f)){
              w(f)+=(p1._2-prob)*delta
            } else{
              w(f)=(p1._2-prob)*delta
            }})
        })
        w
      })
      .saveAsTextFile(args.model())

  }
}
