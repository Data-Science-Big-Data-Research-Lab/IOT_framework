package offline

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import scala.collection.mutable

object OfflineExperimentSchema {

  def getSchema(nx:Int): StructType = {

    val schema = mutable.MutableList[StructField]()

    schema+=StructField("mre", DoubleType, true)

    schema+=StructField("b", DoubleType, true)

    schema+=StructField("w", IntegerType, true)

    schema+=StructField("i", IntegerType, true)

    schema+=StructField("s", DoubleType, true)


    schema+=StructField("a_0", DoubleType, true)

    for (i <- 1 to nx) {

      schema+=StructField("a_"+i, DoubleType, true)

    }

    StructType(schema)

  }

}