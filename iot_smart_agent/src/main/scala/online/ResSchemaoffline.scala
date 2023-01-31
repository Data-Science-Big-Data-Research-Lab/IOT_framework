package online

import org.apache.spark.sql.types._

import scala.collection.mutable

object ResSchemaoffline {

  def getSchema(): StructType = {

    val schema = mutable.MutableList[StructField]()



    schema+=StructField("Alpha", DoubleType, true)

    schema+=StructField("Iteration", IntegerType, true)

    schema+=StructField("Windows", IntegerType, true)

    schema+=StructField("Batch", DoubleType, true)

    schema+=StructField("Relative Error", DoubleType, true)



    StructType(schema)

  }

}