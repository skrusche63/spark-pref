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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.pref.{EPrefBuilder,RequestContext}

import de.kp.spark.pref.source.EventSource
import de.kp.spark.pref.model._

/**
 * The EPrefActor is responsible for preferences built from
 * customer engagement events or sequences
 */
class EPrefActor(@transient ctx:RequestContext) extends BaseActor {
  
  private val builder = new EPrefBuilder(ctx)
  def receive = {

    case req:ServiceRequest => {
      
      val missing = (properties(req) == false)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        
        cache.addStatus(req,ResponseStatus.RATING_BUILDING_STARTED)
 
        try {
          
          val source = new EventSource(ctx)
          val rating = req.data(Names.REQ_RATING)
          
          rating match {
            
            case "explicit" => {
          
              val dataset = source.explicitDS(req)
              builder.ratingsExplicit(req,dataset)
              
            }
            
            case "implicit" => {
          
              val dataset = source.implicitDS(req)
              builder.ratingsImplicit(req,dataset)
              
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