package com.fnmathlogic.spark

package object analysis {
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vectors

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


