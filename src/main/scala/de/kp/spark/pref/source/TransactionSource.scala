package de.kp.spark.pref.source
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
import de.kp.spark.core.source._

import de.kp.spark.pref.RequestContext

import de.kp.spark.pref.spec.Fields

class TransactionSource(@transient ctx:RequestContext) {

  private val config = ctx.config
  private val transactionModel = new TransactionModel(ctx)
  
  def explicitDS(req:ServiceRequest):RDD[(String,String,Int,Double,Long)] = {
    
    val source = req.data(Names.REQ_SOURCE)
    source match {

      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(ctx.sparkContext).connect(config,req)
        transactionModel.buildElasticExplicit(req,rawset)
        
      }

      case Sources.FILE => {

        val rawset = new FileSource(ctx.sparkContext).connect(config.input(1),req)
        transactionModel.buildFileExplicit(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
         
        val rawset = new JdbcSource(ctx.sparkContext).connect(config,req,fields)
        transactionModel.buildJDBCExplicit(req,rawset)
        
      }

      case Sources.PARQUET => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
         
        val rawset = new ParquetSource(ctx.sparkContext).connect(config.input(1),req,fields)
        transactionModel.buildParquetExplicit(req,rawset)
        
      }
      
      case _ => null
      
   }

  }
  
  def implicitDS(req:ServiceRequest):RDD[(String,String,List[(Long,List[Int])])] = {
    
    val source = req.data(Names.REQ_SOURCE)
    source match {

      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(ctx.sparkContext).connect(config,req)
        transactionModel.buildElasticImplicit(req,rawset)
        
      }

      case Sources.FILE => {

        val rawset = new FileSource(ctx.sparkContext).connect(config.input(1),req)
        transactionModel.buildFileImplicit(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
         
        val rawset = new JdbcSource(ctx.sparkContext).connect(config,req,fields)
        transactionModel.buildJDBCImplicit(req,rawset)
        
      }

      case Sources.PARQUET => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
         
        val rawset = new ParquetSource(ctx.sparkContext).connect(config.input(1),req,fields)
        transactionModel.buildParquetImplicit(req,rawset)
        
      }
            
      case _ => null
      
   }

  }

}