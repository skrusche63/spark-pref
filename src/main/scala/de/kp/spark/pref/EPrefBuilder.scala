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

import de.kp.spark.core.io.ParquetWriter
import de.kp.spark.pref.format.{Items,Users,Ratings}

import de.kp.spark.pref.model._

import de.kp.spark.pref.format.EventFormatter
import de.kp.spark.pref.util.EventScoreBuilder

class EPrefBuilder(@transient ctx:RequestContext) extends Serializable {

  def ratingsExplicit(req:ServiceRequest,rawset:RDD[(String,String,String,Int,Double,Long)]) {
    
    val ratings = rawset.map(x => {
      val (site,user,item,event,score,timestamp) = x
      (site,user,item,score,timestamp,event)
    })
    
    req.data(Names.REQ_SINK) match {
  
      case Sinks.FILE    => ratingsToFile(req,ratings)
      case Sinks.PARQUET => ratingsToParquet(req,ratings)
        
      case Sinks.REDIS => ratingsToRedis(req,ratings)
      
      case _ => {/* do nothing */}

    }
    
  }
  
  def ratingsImplicit(req:ServiceRequest,rawset:RDD[(String,String,String,Int,Long)]) {
    
    val ratings = buildRatings(rawset)
    req.data(Names.REQ_SINK) match {
  
      case Sinks.FILE    => ratingsToFile(req,ratings)
      case Sinks.PARQUET => ratingsToParquet(req,ratings)
        
      case Sinks.REDIS => ratingsToRedis(req,ratings)
      
      case _ => {/* do nothing */}

    }
  
  }
  
  /**
   * This method saves event-based ratings as a file on a Hadoop file system; to this
   * end it is distinguished between requests that have specified a certain format,
   * and those where no format is provided.
   */
  private def ratingsToFile(req:ServiceRequest,ratings:RDD[(String,String,String,Double,Long,Int)]) {

    /*
     * Check whether users already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Users.exists(req) == false) {
      val busers = ctx.sparkContext.broadcast(Users)
      ratings.foreach(x => busers.value.put(req,x._2))
    
    } 
    /*
     * Check whether items already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Items.exists(req) == false) {
      val bitems = ctx.sparkContext.broadcast(Items)
      ratings.foreach(x => bitems.value.put(req,x._3.toString))    
    } 

    if (req.data.contains(Names.REQ_FORMAT)) {
       
      val format = req.data(Names.REQ_FORMAT)
      format match {
        
        case Formats.CAR => {
          
          /*
           * STEP #1: Register columns or fields of event-based feature vector in a Redis
           * instance; this field specification is used by the Context-Aware Analysis engine 
           * (later one) 
           */
          EventFormatter.fields(req)
          /*
           * STEP #2: Save ratings in binary feature format as file on a Hadoop file system;
           * the respective path must be configured and, in case of the recommender system
           * shared with the Context-Aware Analysis engine          
           */          
          val formatted = EventFormatter.format(req,ratings)
          val stringified = formatted.map(record => {
          
            val target = record._1
            val features = record._2
            /*
             * This is the string format of a targeted point required by the Context-Aware
             * Analysis engine; the 'rating' variable is set as 'target' variable and all
             * other (feature) variables as predictors
             */
            "" + target + "," + features.mkString(" ")
          
          })
    
          val path = Configuration.output(0)
          stringified.saveAsTextFile(path)
          
        }
        
        case _ => throw new Exception("Format is not supported.")
      }
      
    } else {
      
      val stringified = ratings.map(record => {
        
        val (site,user,item,rating,timestamp,event) = record
        List(site,user,item,rating.toString,timestamp.toString,event.toString).mkString(",")
        
      })
    
      val path = Configuration.output(0)
      stringified.saveAsTextFile(path)

    }
    
  }
  
  /**
   * This method saves event-based ratings to a Redis database; to this end it is 
   * distinguished between requests that have specified a certain format, and those 
   * where no format is provided.
   */
  private def ratingsToRedis(req:ServiceRequest,ratings:RDD[(String,String,String,Double,Long,Int)]) {
    /*
     * Check whether users already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Users.exists(req) == false) {
      val busers = ctx.sparkContext.broadcast(Users)
      ratings.foreach(x => busers.value.put(req,x._2))
    
    } 
    /*
     * Check whether items already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Items.exists(req) == false) {
      val bitems = ctx.sparkContext.broadcast(Items)
      ratings.foreach(x => bitems.value.put(req,x._3.toString))    
    } 

    if (req.data.contains(Names.REQ_FORMAT)) {
       
      val format = req.data(Names.REQ_FORMAT)
       format match {
        
        case Formats.CAR => {
          
          /*
           * STEP #1: Register columns or fields of event-based feature vector in a Redis
           * instance; this field specification is used by the Context-Aware Analysis engine 
           * (later one) 
           */
          EventFormatter.fields(req)
          /*
           * STEP #2: Save ratings in binary feature format as file on a Hadoop file system;
           * the respective path must be configured and, in case of the recommender system
           * shared with the Context-Aware Analysis engine          
           */          
          val formatted = EventFormatter.format(req,ratings)
          
          val bratings = ctx.sparkContext.broadcast(Ratings)
          formatted.foreach(record => {
          
            val target = record._1
            val features = record._2
            /*
             * This is the string format of a targeted point required by the Context-Aware
             * Analysis engine; the 'rating' variable is set as 'target' variable and all
             * other (feature) variables as predictors
             */
            val text = "" + target + "," + features.mkString(" ")
            bratings.value.put(req, text)          
          
          })
          
        }
        
        case _ => throw new Exception("Format is not supported.")

      }
      
    } else {
      
      val bratings = ctx.sparkContext.broadcast(Ratings)
      ratings.foreach(record => {
        
        val (site,user,item,rating,timestamp,event) = record
        val text = List(site,user,item,rating.toString,timestamp.toString,event.toString).mkString(",")
        
        bratings.value.put(req, text)

      })
    
    }
    
  }
  
  private def ratingsToParquet(req:ServiceRequest,ratings:RDD[(String,String,String,Double,Long,Int)]) {
    /*
     * Check whether users already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Users.exists(req) == false) {
      val busers = ctx.sparkContext.broadcast(Users)
      ratings.foreach(x => busers.value.put(req,x._2))
    
    } 
    /*
     * Check whether items already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Items.exists(req) == false) {
      val bitems = ctx.sparkContext.broadcast(Items)
      ratings.foreach(x => bitems.value.put(req,x._3.toString))    
    } 

    if (req.data.contains(Names.REQ_FORMAT)) {
       
      val format = req.data(Names.REQ_FORMAT)
       format match {
        
        case Formats.CAR => {
          
          /*
           * STEP #1: Register columns or fields of event-based feature vector in a Redis
           * instance; this field specification is used by the Context-Aware Analysis engine 
           * (later one) 
           */
          EventFormatter.fields(req)
          /*
           * STEP #2: Save ratings in binary feature format as file on a Hadoop file system;
           * the respective path must be configured and, in case of the recommender system
           * shared with the Context-Aware Analysis engine          
           */          
          val formatted = EventFormatter.format(req,ratings)
          val dataset = formatted.map(record => {
          
            val (target,features) = record
            TargetedPointObject(target,features)
          
          })
      
          val store = Configuration.output(0)
          new ParquetWriter(ctx.sparkContext).writeTargetedPoints(store, dataset)
          
        }
        
        case _ => throw new Exception("Format is not supported.")

      }
      
    } else {
      
      val dataset = ratings.map(record => {
        
        val (site,user,item,rating,timestamp,event) = record
        EventScoreObject(site,user,item,rating,timestamp,event)
 
      })

      val store = Configuration.output(0)
      new ParquetWriter(ctx.sparkContext).writeScoredEvents(store, dataset)
    }
    
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