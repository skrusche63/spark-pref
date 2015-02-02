package de.kp.spark.pref
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-Pref project
 * (https://github.com/skrusche63/spark-pref).
 * 
 * Spark-Pref is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Spark-Pref is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * Spark-Pref. 
 * 
 * If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.pref.model._
import de.kp.spark.pref.util.EventScoreBuilder

import scala.collection.mutable.Buffer

/**
 * UCol is a data structure to specify the relationship
 * between a certain user and the respective column ix
 */
case class UCol(site:String,user:String,row:Long)
/**
 * ICol is a data structure to specify the relationship
 * between a certain item and the respective column ix
 */
case class ICol(item:String,col:Long)

case class BEntry(col:Long,cat:String,value:Double)
case class VEntry(row:Long,col:Long,cat:String,value:Double)

class EPrefBuilder(@transient ctx:RequestContext) extends Serializable {

  import ctx.sqlCtx.createSchemaRDD
       
  protected val DAY = 24 * 60 * 60 * 1000 // day in milliseconds

  def ratingsExplicit(req:ServiceRequest,rawset:RDD[(String,String,String,Int,Double,Long)]) {
    
    val ratings = rawset.map(x => {
      val (site,user,item,event,score,timestamp) = x
      (site,user,item,score,timestamp,event)
    })
    
    ratingsToParquet(req,ratings)
    
  }
  
  def ratingsImplicit(req:ServiceRequest,rawset:RDD[(String,String,String,Int,Long)]) {
    
    val ratings = buildRatings(rawset)
    ratingsToParquet(req,ratings)    
  
  }
  
  private def ratingsToParquet(req:ServiceRequest,ratings:RDD[(String,String,String,Double,Long,Int)]) {
    
    val uid = req.data(Names.REQ_UID)
    val name = req.data(Names.REQ_NAME)
    
    /*
     * STEP #1: The computed ratings are saved as an (intermediate) 
     * parquet file
     */
    val store1 = String.format("""%s/%s/%s/1""",ctx.base,name,uid)    

    val table1 = ratings.map{
      case(site,user,item,rating,timestamp,event) => EventScoreObject(site,user,item,rating,timestamp,event)
    }
    
    table1.saveAsParquetFile(store1)

    /*
     * STEP #2: Determine the time interval of the observation;
     * this interval is used to describe the timestamp of the
     * last rating in terms of days from the beginning of the
     * observation
     */
    val timestamps = ratings.map(_._5).sortBy(x => x).collect    
    val (begin_ts,end_ts) = (timestamps.head,timestamps.last)
    
    val begin = ctx.sparkContext.broadcast(begin_ts)
    
    /*
     * STEP #3: Determine lookup data structure for users
     * and save as a parquet file
     */
    val users = ratings.groupBy{case(site,user,item,rating,timestamp,event) => (site,user)}.map(_._1)
    val uzipped = users.zipWithIndex
    
    val store2 = String.format("""%s/%s/%s/2""",ctx.base,name,uid)    
    
    val table2 = uzipped.map{case((site,user),col) => UCol(site,user,col)}
    table2.saveAsParquetFile(store2)
    
    val udict = ctx.sparkContext.broadcast(uzipped.collect.toMap)
    /*
     * STEP #4: Determine lookup data structure for items
     * and save as a parquet file
     */    
    val items = ratings.groupBy{case(site,user,item,rating,timestamp,event) => item}.map(_._1)
    val izipped = items.zipWithIndex
     
    val store3 = String.format("""%s/%s/%s/3""",ctx.base,name,uid)    
    
    val table3 = izipped.map{case(item,col) => ICol(item,col)}
    table3.saveAsParquetFile(store3)
    
    val idict = ctx.sparkContext.broadcast(izipped.collect.toMap)
    
    /*
     * STEP #5: Build data structure that is compatible to the factorization
     * machine format (row,col,cat,val). The respective feature vector has the 
     * following architecture:
     * 
     * a) user block describing the active user
     * 
     * b) item block describing the active item
     * 
     * Every information in addition to the active user & item is considered
     * as context that has some influence onto the user-item rating
     * 
     * c) items rated block (value = 1/N where N is the number of items rated so far)
     *         
     * Hypothesis: The items rated so far by the active user do influence the rating
     * of the active item; this requires to have access onto historical rating data
     * of the active user        
     * 
     * d) last items rated before active item
     * 
     * Hypothesis: The items rated before the active item has some influence onto
     * the rating of the active item; this also requires to have access onto some
     * historical data of the active user
     *         
     * e) datetime blocks of user-item-rating determines the number of days with
     *    respect to the start time
     * 
     * Hypothesis: The datetime of the respective user-item-rating influences the
     * rating of the active items
     *         
     * f) event block of user-item-rating (should be a single column)
     * 
     */
    val rows = ratings
      .groupBy{case(site,user,item,rating,timestamp,event) => (site,user)}
      .map(x => {
        
        val a_block_off = 0
        val b_block_off = a_block_off + udict.value.size
        
        val c_block_off = b_block_off + idict.value.size
        val d_block_off = c_block_off + idict.value.size
        
        val e_block_off = d_block_off + idict.value.size
        val f_block_off = e_block_off + 1        
        
        val (site,user) = x._1       

        val items = x._2.map(_._3)
        val weight = 1.toDouble / items.size
      
        val data = x._2.map{case(site,user,item,rating,timestamp,event) => (item,rating,timestamp,event)}
        /*
         * Sort items and build data structure to determine
         * the item rated before the active item
         */
        val latest = data.map{case(item,rating,timestamp,event) => (item,timestamp)}.toList.sortBy(_._2)           
        data.flatMap{case(item,rating,timestamp,event) => {
        
          /*
           * The subsequent process provides a single row of the feature matrix 
           * evaluated by the factorization machines
           */
          val features = Buffer.empty[BEntry]

          // a) USER BLOCK
          features += BEntry(a_block_off + udict.value((site,user)),"user",1)
 
          
          // b) ACTIVE ITEM BLOCK          
          features += BEntry(b_block_off+idict.value(item),"item",1)


          // c) ITEMS RATED BLOCK
          items.foreach(item => features += BEntry(c_block_off + idict.value(item),"context_categorical_set",weight))
         
       
          // d) RATED BEFORE BLOCK
          
          /* 
           * Determine the item that has been rated just 
           * before the active item
           */
          val pos = latest.indexOf((item,timestamp))
          val before = if (pos > 0) latest(pos-1)._1 else null
        
          features += BEntry(d_block_off + idict.value(before),"context_categorical",1)

          
          // e) TIME BLOCK          
          val time = ((timestamp - begin.value) / DAY).toDouble
          features += BEntry(e_block_off,"context_numerical",time)

          
          // f) EVENT BLOCK
           features += BEntry(f_block_off,"context_numerical",event.toDouble)

           
          // g) TARGET
          features += BEntry(f_block_off+1,"label",rating)
       
          features
          
        }}.toSeq
        
      }).zipWithIndex
     
      val store4 = String.format("""%s/%s/%s/4""",ctx.base,name,uid)    
      
      val table4 = rows.flatMap{case(block,row) => block.map(e => VEntry(row,e.col,e.cat,e.value))}
      table4.saveAsParquetFile(store4)
    
  }
  
  private def buildRatings(rawset:RDD[(String,String,String,Int,Long)]):RDD[(String,String,String,Double,Long,Int)] = {
   
    /* Group extracted data by site, user and item */
    val grouped = rawset.groupBy(x => (x._1,x._2,x._3))
    grouped.map(x => {
      
      val (site,user,item) = x._1
      val events = x._2
      
      /*
       * Events may have positive or negative numbers; as an example,
       * the following descriptions may be used:
       * 
       *  1	Purchased item
       *  2	Joined checkout
       *  3	Placed item in shopping cart
       *  4	Placed item in wish list
       *  5	Browsed item from search result
       *  6	Browsed item from recommendation list
       *  7	Browsed item
       * -1	Returned item
       * -2	Left checkout
       * -3	Removed item from shopping cart
       * -4	Removed item from wish list
       * 
       */
      
      /*
       * Split dataset with respect to event type into positives and negatives
       */      
      val positives = events.filter(x => x._4 > 0)
      val negatives = events.filter(x => x._4 < 0)
      
      /*
       * Determine support for positive & negative event types
       */
      val neg_support = negatives.map(x => (x._4,1)).groupBy(_._1).map(x => (x._1,x._2.size))
      val pos_support = positives.map(x => (x._4,1)).groupBy(_._1).map(x => (x._1,x._2.size))
      /*
       * Some events have negative values indicating negative actions on the
       * part of the user e.g., removing an item from the shopping cart. 
       * 
       * For each such negative event, a corresponding positive event is removed 
       * from the event sequence, before calculating rating.
       * 
       */      
      val support = pos_support.map(x => {
        
        val (event,count) = x        
        val diff_count = if (neg_support.contains(-event)) {

          val difference = count - neg_support(-event)
          if (difference > 0) difference else 0
          
        } else count
        
        (event,diff_count)
        
      }).filter(x => x._2 > 0)
      
      val ratings = support.map(x => {
        
        val (event,support) = x
        val rating = getRating(event,support)
        
        (event,rating)
        
      }).toList.sortBy(x => -x._2)
      
      /* Determine maximum engagement event and respective rating */
      val (event,rating) = ratings.head      
      /* Determine latest timestamp for the maximum engagement event */
      val latest = positives.filter(x => x._4 == event).toList.sortBy(x => -x._5).head._5
      
      (site,user,item,rating,latest,event)
      
    })
    
  }
  
  private def getRating(event:Int,support:Int):Int = {
    
    val eventScores = EventScoreBuilder.get().filter(x => x.event == event)
    val eventScore = if (eventScores.size > 0) eventScores(0) else null

    if (eventScore == null) throw new Exception("Event is unknown.")

    val scores = eventScore.scores
    if (support <= scores.length) scores(support-1) else scores(scores.size -1)
     
  }
    
}