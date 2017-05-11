 

import java.lang.String

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
 
 

object  Test {
  def main(args:Array[String]){
    
    if(args.size!=2)println("error")
    val filePath = args(0)
    
    println(filePath)
    val conf = new SparkConf().setAppName("hello");  
    val sc = new SparkContext(conf)
    val consume = new Consume(sc,"file://"+filePath);
    consume.loadData();
    sc.stop()
  }
}