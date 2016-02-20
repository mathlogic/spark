/*
 * Copyright 2016 MathLogic
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.fnmathlogic.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object analysis {

  def main : String = {
  return "This is Data Science Analysis package but by MathLogic"
  }

   def df2LabeledPoint (indf: org.apache.spark.sql.DataFrame, targetvar: String ) : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = {
var i = -1
val x1 = indf.columns.indexOf(targetvar)
val x2 = indf.rdd.map{row =>  
LabeledPoint(row.toSeq(x1).toString.toDouble,Vectors.dense(row.toSeq.toArray.filter{ _ => i = i+1; i != x1 }.map({
    case s: String => s.toDouble
    case l: Long => l.toDouble
    case d: Double => d
    case i: Int => i*1.0
    case _ => 0.0
  })))
}
return x2
  }
}
