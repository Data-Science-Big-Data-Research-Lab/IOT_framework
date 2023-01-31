package online


import java.text.DecimalFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import utils.{SparkSessionUtils, TimingUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable


object StrLinear {


  final val initialWeights = Vectors.zeros(numFeatures)
  final val miniBatchFraction =  1.0

  //#################################
  final val mode="lux"
  final val numFeatures = 90
  final val cero="00"
  final val testFolder = "test_"+numFeatures
  final val stepSize = 0.0000000003
  final val numIterations = 10

  //#################################

  

  //*************************************************************************************
  //final val outputPath = "/outputPath"
  //final val trainingPath = "/trainingPath"
  //final val testPath = "/testPath"
  //*************************************************************************************

  //*************************************************************************************
  final val batchSeconds = SparkSessionUtils.batchSeconds
  final val ssc = SparkSessionUtils.ssc
  final val sc = SparkSessionUtils.sc
  final val sql = SparkSessionUtils.sql
  //*************************************************************************************


  final var nLp = 0
  final var steps = 0



  def main(args: Array[String]): Unit = {

    //###################################################

    val trainingData = ssc.textFileStream(trainingPath).map(LabeledPoint.parse).cache()

    val testData = ssc.textFileStream(testPath).map(LabeledPoint.parse)

    val model = new StreamingLinearRegressionWithSGD().
      setInitialWeights(initialWeights).
      setStepSize(stepSize).
      setNumIterations(numIterations).
      setRegParam(10.0)
    model.algorithm.setIntercept(true)
    model.trainOn(trainingData)

    val res = model.predictOnValues(testData.map(lp => (lp.label, lp.features)))

    //###################################################

    trainingData.foreachRDD { e =>

      if (!e.isEmpty){

        nLp+=e.collect().length
        print("\n"+TimingUtils.getTimeString(steps*batchSeconds)+"\tThe model has been updated with "+nLp+" Labeled Points")

      }
      else{
        print("\n"+TimingUtils.getTimeString(steps*batchSeconds))

      }



    }

    testData.foreachRDD { e =>

      steps+=1

    }


    printPredictions(res,testData)


    ssc.start()
    ssc.awaitTermination()

  }

  def printPredictions(predicted:DStream[(Double,Double)],testData:DStream[LabeledPoint]):Unit={


    val res = predicted.transformWith(testData,(e1:RDD[(Double,Double)],e2:RDD[LabeledPoint]) =>
    {


      val vals = mutable.MutableList[Row]()
      val t1 = e1.collect()
      val t2 = e2.collect()


      if (t1.length!=0){
        print("\tTest done")
      }

      for (i <- 0 to (t1.length-1)){

        val v1 = t1(i)
        val v2 = t2(i)

        val x = v2.features
        val obs = v1._1
        val pre = v1._2
        val ae = math.abs(obs-pre)
        val re = ae/obs

        //        print("\n["+nLp+" LP acquired]\tObserved = "+obs+"\tPredicted = "+pre+"\tAE = "+ae+"\tRE = "+re)

        val rv = mutable.MutableList[Any]()

        for (i <- 0 to (x.size-1)){

          rv+=x(i)

        }

        rv+=obs
        rv+=pre
        rv+=ae
        rv+=re

        rv+=nLp
        rv+=TimingUtils.getTimeString(steps*batchSeconds)
        rv+=numFeatures
        rv+=numIterations
        rv+=stepSize
        rv+=miniBatchFraction

        vals+=Row.fromSeq(rv)

      }

      sc.parallelize(vals)

    })


    res.foreachRDD(e =>{

      if (!e.isEmpty){

        val schema = ResSchema.getSchema(numFeatures)

        val df = sql.createDataFrame(e,schema)

        val fr = new DecimalFormat("0000")

        df.coalesce(1).write.option("header", "true").option("delimiter",";").mode(SaveMode.Overwrite).csv(outputPath+"pred_"+fr.format(steps))

      }

    })

  }

}