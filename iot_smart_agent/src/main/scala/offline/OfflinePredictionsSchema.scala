package offline

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import scala.collection.mutable

object OfflinePredictionsSchema {

  def getSchema(nx:Int): StructType = {

    val schema = mutable.MutableList[StructField]()

    schema+=StructField("o", DoubleType, true)

    schema+=StructField("p", DoubleType, true)

    schema+=StructField("ae", DoubleType, true)

    schema+=StructField("re", DoubleType, true)

    for (i <- 1 to nx) {

      schema+=StructField("x_"+i, DoubleType, true)

    }

    StructType(schema)

  }

}
