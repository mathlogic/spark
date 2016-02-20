name := "analysis"

version := "1.0.0"

organization := "com.fnmathlogic"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-sql_2.10" % "1.3.0",
"org.apache.spark" % "spark-mllib_2.10" % "1.3.0"
)

ivyXML :=
<dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="3.0.0.v201112011016">
<artifact name="javax.servlet" type="orbit" ext="jar"/>
</dependency>
