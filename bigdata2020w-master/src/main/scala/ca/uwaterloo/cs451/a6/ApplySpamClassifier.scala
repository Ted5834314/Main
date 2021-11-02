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

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  val w1=HashMap[Int, Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int], w: HashMap[Int, Double]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("Apply Spam Classifier")
    val sc = new SparkContext(conf)
    val outputDir=new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    sc.textFile(args.model())
      .map(line => {
             val mod=line.split(",")
             (mod(0).substring(1), mod(1).substring(0, mod(1).length-1))
           })
      .collect()
      .foreach(p => {
               w1(p._1.toInt)=p._2.toDouble
      })

    val bw1=sc.broadcast(w1)
    val textFile=sc.textFile(args.input())
    textFile
      .map(line => {
            val tokens=line.split(" ")
            val featurevec=tokens.slice(2,tokens.length).map(_.toInt)
            val score=spamminess(featurevec, bw1.value)
            var spam="ham"
            if (score>0) {
              spam="spam"
            }
            (tokens(0),tokens(1),score,spam)
          })
      .saveAsTextFile(args.output())

  }
}
