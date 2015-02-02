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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class RequestContext(
  /*
   * Reference to the common SparkContext; this context can be used
   * to access HDFS based data sources or leverage the Spark machine
   * learning library or other Spark based functionality
   */
  @transient val sparkContext:SparkContext) extends Serializable {

  val sqlCtx = new SQLContext(sparkContext)
  /**
   * The base directory for all file based IO
   */
  def base = Configuration.input(0)
  
  def config = Configuration

}