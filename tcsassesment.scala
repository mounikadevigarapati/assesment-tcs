package newstarts
  
  
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object tcsassesment
{
     
  def main(args: Array[String]){
  
    val spark = SparkSession.builder().appName("Example Code").master("local[*]").getOrCreate()

    val df = spark.read.option("header", "False").option("inferSchema","true").csv("f:/exampleOrders.csv")
    .toDF("OrderID", "UserName", "OrderTime", "OrderType", "Quantity", "Price")
    
    import spark.implicits._
    val buyDF = df.filter($"OrderType" === "BUY").withColumnRenamed("Quantity", "Buy_Quantity")
    val sellDF = df.filter($"OrderType" === "SELL").withColumnRenamed("Quantity", "Sell_Quantity").withColumnRenamed("OrderTime","OrderTime_sell").withColumnRenamed("OrderID","OrderID_sell").withColumnRenamed("Price","Price_sell")
      
    val matchesDF = buyDF.join(sellDF, $"Buy_Quantity" === $"Sell_Quantity" ,"inner")
    
    
    val df2=matchesDF.select(
        
        when(buyDF("OrderID") > sellDF("OrderID_sell"), buyDF("OrderID")).otherwise(sellDF("OrderID_sell")).alias("Order_ID"),
       
       when(buyDF("OrderTime") > sellDF("OrderTime_sell"), sellDF("OrderID_sell")).otherwise(buyDF("OrderID")).alias("Order_ID"),
       
       when(buyDF("OrderTime") > sellDF("OrderTime_sell"), buyDF("OrderTime")).otherwise(sellDF("OrderTime_sell")).alias("OrderTime"),
       buyDF("Buy_Quantity").alias("Quantity"),
       when(buyDF("OrderTime") > sellDF("OrderTime_sell"), sellDF("Price_sell")).otherwise(buyDF("Price")).alias("Price"))
       
       df2.show()
       
       df2.write.format("csv").save("F:/outputExampleMatches")
       
       spark.stop()
   
   
  }
}
   
   
   
   
   
    
    
    
       
   

  




  