package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext;

class VipSample(sc:SparkContext,filePath:String="hdfs://WH-BI-NS/user/hive/warehouse/analyst_pub.db/lb_recommend_system") {
  
    val sqlContext = new SQLContext(this.sc);
    import sqlContext.implicits._;
    

    
    def getCount(){
        //  读取数据并且缓存
        val data = sqlContext.read.parquet(this.filePath);
        data.cache();
        val count = data.count();
        println("----------");
        println(count);
    }
  
}