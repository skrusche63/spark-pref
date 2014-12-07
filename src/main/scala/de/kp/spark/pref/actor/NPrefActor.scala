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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.pref.NPrefBuilder

import de.kp.spark.pref.source.TransactionSource
import de.kp.spark.pref.model._

/*
 * The NPrefActor is responsible for normalized preferences;
 * these describe the relationship between users and items
 * without any contextual information taken into account 
 */
class NPrefActor(@transient sc:SparkContext) extends BaseActor {
  
  private val builder = new NPrefBuilder(sc)
  def receive = {

    case req:ServiceRequest => {
      
      val missing = (properties(req) == false)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        
        cache.addStatus(req,ResponseStatus.RATING_BUILDING_STARTED)
 
        try {
          
          val source = new TransactionSource(sc)
          val rating = req.data(Names.REQ_RATING)
          
          rating match {
            
            case "explicit" => {
          
              val dataset = source.explicitDS(req)
              req.data(Names.REQ_SINK) match {
            
                case Sinks.FILE  => builder.ratingsToFileExplicit(req,dataset)
                case Sinks.REDIS => builder.ratingsToRedisExplicit(req,dataset)
            
                case _ => {/*does not happen*/}
            
              }
              
            }
            
            case "implicit" => {
          
              val dataset = source.implicitDS(req)
              req.data(Names.REQ_SINK) match {
            
                case Sinks.FILE  => builder.ratingsToFileImplicit(req,dataset)
                case Sinks.REDIS => builder.ratingsToRedisImplicit(req,dataset)
            
                case _ => {/*does not happen*/}
            
              }
              
            }
            
            case _ => throw new Exception("This rating type is not supported.")
          }

          cache.addStatus(req,ResponseStatus.RATING_BUILDING_FINISHED)
    
          /* Notify potential listeners */
          notify(req,ResponseStatus.RATING_BUILDING_FINISHED)
          
        } catch {
          case e:Exception => cache.addStatus(req,ResponseStatus.FAILURE)          
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
    
    if (req.data.contains(Names.REQ_UID) == false) return false
    if (req.data.contains(Names.REQ_SINK) == false) return false
    
    if (Sinks.isSink(req.data(Names.REQ_SINK)) == false) return false
    
    true    
    
  }

}