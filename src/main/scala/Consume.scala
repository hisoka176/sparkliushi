 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Consume(sc:SparkContext,filePath:String) {
  

    val sqlContext = new SQLContext(this.sc)
    
    import sqlContext.implicits; 
    
    val consumeData = sqlContext.read.text(filePath)
   
    consumeData.cache();
    def loadData(){
      println("----------------------");
      println(this.consumeData.count());       
    }
}