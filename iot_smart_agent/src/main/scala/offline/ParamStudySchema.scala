package offline

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import scala.collection.mutable

object ParamStudySchema {

  def getSchema(): StructType = {

    val schema = mutable.MutableList[StructField]()

    schema+=StructField("b", DoubleType, true)

    schema+=StructField("w", IntegerType, true)

    schema+=StructField("i", IntegerType, true)

    schema+=StructField("s", DoubleType, true)

    schema+=StructField("re_1", DoubleType, true)

    schema+=StructField("mre", DoubleType, true)

    StructType(schema)

  }

}
