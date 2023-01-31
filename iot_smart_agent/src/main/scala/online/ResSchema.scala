package online

import org.apache.spark.sql.types._

import scala.collection.mutable

object ResSchema {

  def getSchema(nx:Int): StructType = {

    val schema = mutable.MutableList[StructField]()


    for (i <- 0 to (nx-1)) {

      schema+=StructField("x_"+i, DoubleType, true)

    }

    schema+=StructField("o", DoubleType, true)

    schema+=StructField("p", DoubleType, true)

    schema+=StructField("ae", DoubleType, true)

    schema+=StructField("re", DoubleType, true)

    schema+=StructField("nlp", IntegerType, true)

    schema+=StructField("time", StringType, true)

    schema+=StructField("features", IntegerType, true)

    schema+=StructField("iter", IntegerType, true)

    schema+=StructField("step", DoubleType, true)

    schema+=StructField("batch", DoubleType, true)

    StructType(schema)

  }

}