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
import org.apache.spark.sql.functions._

object analysis {

  def main : String = {
  return "This is Data Science Analysis package contributed by MathLogic"
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

   def df2LabeledPointMap (indf: org.apache.spark.sql.DataFrame, targetvar: String ) : Array[String] = {
var i = -1
val x1 = indf.columns.indexOf(targetvar)
val x2 = indf.columns.filter{ _ => i = i+1; i != x1 }
return x2
  }

def model_deciles(indf: org.apache.spark.sql.DataFrame, pr: Any, tv: Any ) : org.apache.spark.sql.DataFrame = {
val prob:String = if (pr.isInstanceOf[Int]) {indf.columns(pr.toString.toInt) }else{pr.toString}
val targetvar:String = if (tv.isInstanceOf[Int]) {indf.columns(tv.toString.toInt) }else{tv.toString}

val temp = indf.select(prob).sort(prob).rdd.map{row => row(0).toString.toDouble}.zipWithIndex.map{case (k,v) => (v,k)}.cache

val l = temp.count
val p = 10
val a = Array.ofDim[Double](p, 2)

var x = 0

for (x <- 0 to p-1) {
a(x)(0) = x
a(x)(1) = temp.lookup(x*l/p)(0).toString.toDouble
}

val deciler: (Double => Double) = (arg: Double) => {
if (arg > a(9)(1)) {a(9)(0)}else{
if (arg > a(8)(1)) {a(8)(0)}else{
if (arg > a(7)(1)) {a(7)(0)}else{
if (arg > a(6)(1)) {a(6)(0)}else{
if (arg > a(5)(1)) {a(5)(0)}else{
if (arg > a(4)(1)) {a(4)(0)}else{
if (arg > a(3)(1)) {a(3)(0)}else{
if (arg > a(2)(1)) {a(2)(0)}else{
if (arg > a(1)(1)) {a(1)(0)}else{
a(0)(0)}}}}}}}}}
}

val sqlfunc = udf(deciler)

val temp2 = indf.withColumn("decile", sqlfunc(col(prob))).sort("decile").groupBy("decile").agg(first("decile").as("Decile"),avg(prob).as("Avg_P"),sum(targetvar).as("Tar"),count("label").as("Total"))
val temp3 = temp2.withColumn("NTar",temp2("Total")-temp2("Tar")).cache

val totTar = temp3.agg(sum("Tar")).map{row => row(0)}.first.toString.toDouble
val totNTar = temp3.agg(sum("NTar")).map{row => row(0)}.first.toString.toDouble
val temp4 = temp3.withColumn("Tar_P",temp3("Tar")/totTar).withColumn("NTar_P",temp3("NTar")/totNTar).select("Decile","Total","Tar","NTar","Tar_p","NTar_p","Avg_P")

return temp4
}
 
 
}
