package de.kp.spark.pref.sink
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

import de.kp.spark.pref.redis.RedisClient

object RedisSink extends Serializable {

  val client  = RedisClient()

  def addRating(uid:String,rating:String) {
    
    val k = "rating:" + uid
    val v = rating
    
    client.lpush(k,v)

  }
  
}