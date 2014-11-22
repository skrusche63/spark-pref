package de.kp.spark.pref.source

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.pref.model._

import de.kp.spark.pref.spec.Fields

import scala.collection.mutable.ArrayBuffer

class TransactionModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(uid:String,rawset:RDD[Map[String,String]]):RDD[(String,String,List[(Long,List[Int])])] = {
 
    val spec = sc.broadcast(Fields.get(uid))
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val group = data(spec.value("group")._1)

      val item  = data(spec.value("item")._1)
      
      (site,user,group,timestamp,item)
      
    })
    
    buildTransactions(dataset)
    
  }
  
  def buildFile(uid:String,rawset:RDD[String]):RDD[(String,String,List[(Long,List[Int])])] = {
    
    val dataset = rawset.map(valu => {
      
      val Array(site,timestamp,user,group,item) = valu.split("\\|")  
      
      (site,user,group,timestamp.toLong,item)
    
    })
    
    buildTransactions(dataset)
    
  }
  
  def buildJDBC(uid:String,rawset:RDD[Map[String,Any]]):RDD[(String,String,List[(Long,List[Int])])] = {
        
    val fieldspec = Fields.get(uid)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]

      val user = data(spec.value("user")._1).asInstanceOf[String] 
      val group = data(spec.value("group")._1).asInstanceOf[String]
      
      val item  = data(spec.value("item")._1).asInstanceOf[String]
      
      (site,user,group,timestamp,item)
      
    })
    
    buildTransactions(dataset)

  }
    
  def buildPiwik(uid:String,rawset:RDD[Map[String,Any]]):RDD[(String,String,List[(Long,List[Int])])] = {
    
    val dataset = rawset.map(row => {
      
      val site = row("idsite").asInstanceOf[Long]
      val timestamp  = row("server_time").asInstanceOf[Long]

      /* Convert 'idvisitor' into a HEX String representation */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)

      val group = row("idorder").asInstanceOf[String]
      val item  = row("idaction_sku").asInstanceOf[Long]
      
      (site.toString,user,group,timestamp,item.toString)
      
    })
    
    buildTransactions(dataset)
    
  }

  private def buildTransactions(dataset:RDD[(String,String,String,Long,String)]):RDD[(String,String,List[(Long,List[Int])])] = {
    /*
     * Group dataset by site & user and aggregate all items of a
     * certain group and all groups into a time-ordered sequence
     */
    val sequences = dataset.groupBy(v => (v._1,v._2)).map(data => {
     
      val (site,user) = data._1
      /*
       * Aggregate all items of a certain group onto a single
       * line thereby sorting these items in ascending order.
       * 
       * And then, sort these items by timestamp in ascending
       * order.
       */
      val groups = data._2.groupBy(_._3).map(group => {

        val timestamp = group._2.head._4
        val items = group._2.map(_._5.toInt).toList

        (timestamp,items)
        
      }).toList.sortBy(_._1)

      (site,user,groups)     
      
    })
    
    sequences
    
  }
}