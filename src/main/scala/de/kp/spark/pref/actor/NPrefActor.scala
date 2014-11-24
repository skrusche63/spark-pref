package de.kp.spark.pref.actor
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.pref.NPrefBuilder

import de.kp.spark.pref.source.TransactionSource
import de.kp.spark.pref.model._

import de.kp.spark.pref.redis.RedisCache

/*
 * The NPrefActor is responsible for normalized preferences;
 * these describe the relationship between users and items
 * without any contextual information taken into account 
 */
class NPrefActor(@transient val sc:SparkContext) extends BaseActor {
 
  def receive = {

    case req:ServiceRequest => {
      
      val missing = (properties(req) == false)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        
        RedisCache.addStatus(req,ResponseStatus.BUILDING_STARTED)
 
        try {
          
          val source = new TransactionSource(sc)
          val dataset = source.transDS(req.data)

          req.data("sink") match {
            
            case Sinks.FILE  => NPrefBuilder.buildToFile(req,dataset)
            case Sinks.REDIS => NPrefBuilder.buildToRedis(req,dataset)
            
            case _ => {/*do not happen*/}
            
          }

          RedisCache.addStatus(req,ResponseStatus.BUILDING_FINISHED)
    
          /* Notify potential listeners */
          notify(req,ResponseStatus.BUILDING_FINISHED)
          
        } catch {
          case e:Exception => RedisCache.addStatus(req,ResponseStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }

  /**
   * Determine whether all required parameters are provided
   * with this request
   */
  private def properties(req:ServiceRequest):Boolean = {
    
    if (req.data.contains("uid") == false) return false
    if (req.data.contains("sink") == false) return false
    
    if (Sinks.isSink(req.data("sink")) == false) return false
    
    true    
    
  }

}