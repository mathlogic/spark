# Data Science tool kit for Spark

A library for handy functions that are typically required for any Data Science project using spark

## Requirements

This library requires Spark 1.3+

## Linking
You will be eventualy able to link against this library in your program at the following coordinates:

### Scala 2.10
```
groupId: com.fnmathlogic
artifactId: spark-analysis
version: TBD
```

### Scala 2.11
```
groupId: com.fnmathlogic
artifactId: spark-analysis
version: TBD
```

## Features
This package contains handy tools which would make routine tasks easy for any Data Scientist e.g. conversion of any Data Frame to Labeled Point, treatment of missing values and generate statistical summaries from model output objects in friendly formats.

The following key features are available/ planned:
* `df2LabeledPoint`: Convert any Data Frame to Labeled Point by specifying the taregt variable. It takes two inputs - Name of Data Frame and Nameof target variable and return a RDD of Labeled Point that can then use used with MLlib.
* `missingValue`: Replace missing value with 0/avg/ median.
