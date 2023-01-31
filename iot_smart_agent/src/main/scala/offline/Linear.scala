package offline

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import utils.FormatUtils

import scala.collection.mutable

object Linear {


  def getParamStudy(trainingRDD: RDD[LabeledPoint], testRDD: RDD[LabeledPoint],
                    miniBatchFraction: Double = 1.0, window: Int,
                    iterations: Array[Int], leftValue: Double, rightValue: Double, step: Double): mutable.MutableList[Row] = {


    var nstudy = 1

    val r = mutable.MutableList[Row]()

    for (iter <- iterations) {

      for (stepSize <- leftValue to rightValue by step) {

        val results = trainTestOneLrModel(trainingRDD, testRDD, miniBatchFraction, window, iter, stepSize)

        val r0 = results._1(0)

        val re1 = r0.getDouble(3)

        val mre = results._2.getDouble(0)

        r += Row(miniBatchFraction, window, iter, stepSize, re1, mre)

        print("\n[" + nstudy + "] W = "+window +" numIterations = " + iter + ", stepSize = " + FormatUtils.sfr.format(stepSize) +
          " ==>  re_1 = " + FormatUtils.efr.format(re1) + " mre = " + FormatUtils.efr.format(mre))

        nstudy += 1

      }

    }

    r

  }

  def trainTestOneLrModel(trainingRDD: RDD[LabeledPoint], testRDD: RDD[LabeledPoint],
                          miniBatchFraction: Double = 1.0, window: Int,
                          numIterations: Int, stepSize: Double): Tuple2[mutable.MutableList[Row], Row] = {

    val m: LinearRegressionWithSGD = new LinearRegressionWithSGD()

    m.setIntercept(true)
    m.optimizer.setNumIterations(numIterations)
    m.optimizer.setStepSize(stepSize)
    m.optimizer.setMiniBatchFraction(miniBatchFraction)

    val model = m.run(trainingRDD)

    //    val model = LinearRegressionWithSGD.train(trainingRDD,numIterations,stepSize,miniBatchFraction,Vectors.zeros(window))

    val predictedValues = testRDD.map { point =>
      val prediction = model.predict(point.features)
      (point.features, point.label, prediction)
    }


    val predictionResults = getPredictionResults(testRDD, predictedValues)

    val mre = getMre(predictionResults)

    //#####################################Experiment results

    var rv = mutable.MutableList[Any]()

    rv+=mre
    rv+=miniBatchFraction
    rv+=window
    rv+=numIterations
    rv+=stepSize


    rv+=model.intercept

    for (i <- 0 to (model.weights.size - 1)) {

      rv += model.weights(i)

    }

    val experimentResults = Row.fromSeq(rv)

    //#####################################Experiment results

    return Tuple2(predictionResults,experimentResults)

  }

  def getPredictionResults(testRDD: RDD[LabeledPoint], predictedValues: RDD[(Vector, Double, Double)]): mutable.MutableList[Row] = {

    val predictions = predictedValues.collect()

    val res = mutable.MutableList[Row]()

    predictions.map { el =>

      var rv = mutable.MutableList[Any]()

      val x = el._1
      val o = el._2
      val p = el._3
      val ae = math.abs(o - p)
      val re = ae / o

      rv += o
      rv += p
      rv += ae
      rv += re

      for (i <- 0 to (x.size - 1)) {

        rv += x(i)

      }

      res += Row.fromSeq(rv)

    }

    res

  }

  def getMre(results: mutable.MutableList[Row]): Double = {

    var mre = 0.0

    results.map { el =>

      mre += el.getDouble(3)

    }

    mre / results.length

  }

}