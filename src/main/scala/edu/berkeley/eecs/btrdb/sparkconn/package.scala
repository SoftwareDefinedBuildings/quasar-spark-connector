package edu.berkeley.eecs.btrdb

import org.apache.spark.SparkContext

/** Contains [[edu.berkeley.eecs.btrdb.sparkconn]] class that is the main entry point for
  * analyzing Quasar data from Spark. */
package object sparkconn {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)

}
