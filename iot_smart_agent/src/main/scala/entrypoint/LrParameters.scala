package entrypoints

import offline.{Linear, OfflineExperimentSchema, OfflinePredictionsSchema, ParamStudySchema}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SaveMode}
import utils.{FormatUtils, SparkSessionUtils, TimingUtils}

import scala.collection.mutable

object LrParameters {


  final val sc = SparkSessionUtils.sc
  final val sql = SparkSessionUtils.sql


  final val singlePath = "/mnt/datos/amfergom/nas/proyectos/iot_smart_agent/fuentes/dataset_experiments/exp_\" + exp + \"/w_\" + ceros+window + \"/paramStudy/study_01"



  def main(args: Array[String]): Unit = {
     val miniBatchFraction = 1.0
     val iterations = Array(10,15,25,50,100)
    //final val iterations = Array(10)
     val leftValue  = 1.00E-13
     val rightValue = 1.00E-04
     val step       = 1.00E-13

    // val windows =Array(3,4,6,12,24,90,180,360,720,1080)
    val windows =Array(3,4,6,12,24,90)
     val exp = "lux"
    var trainingPath = ""
    var testPath = ""
    var ouputPath =""

    var ceros=""
     for (window <-windows) {

       if(window <10){
          ceros="000"
       }else if(window>10 && window<100){
          ceros="00"
       }else if(window>100 && window<1000){
          ceros="0"
       }else{
          ceros=""
       }
       var trainingPath = "/mnt/datos/amfergom/nas/proyectos/iot_smart_agent/fuentes/dataset_experiments/exp_" + exp + "/w_" + ceros + window + "/pre_training"
       var testPath = "/mnt/datos/amfergom/nas/proyectos/iot_smart_agent/fuentes/dataset_experiments/exp_" + exp + "/w_" + ceros + window + "/pre_test"
       var ouputPath = "/mnt/datos/amfergom/nas/proyectos/iot_smart_agent/fuentes/dataset_experiments/exp_" + exp + "/w_" + ceros + window + "/paramStudy"


       //  single (10,0.00000958,singlePath)

       //    multiple()

       //      overFittingCheck (10,1.26E-06)

       TimingUtils.time(paramStudy(trainingPath,testPath,ouputPath,miniBatchFraction,window,iterations,leftValue,rightValue,step))

     }

  }


  def paramStudy(trainingPath:String,testPath:String,ouputPath:String,miniBatchFraction:Double,window:Int, iterations: Array[Int], leftValue:Double,rightValue:Double, step:Double ):Unit={

    val trainingRDD = sc.textFile(trainingPath).map(LabeledPoint.parse).cache()

    val testRDD     = sc.textFile(testPath).map(LabeledPoint.parse).cache()

    val studyResults = Linear.getParamStudy(trainingRDD,testRDD,miniBatchFraction,window,iterations,leftValue,rightValue,step)

    println("\n\nStudy results:")

    studyResults.map{el=>println("i = "+el.getInt(2)+" s = "+FormatUtils.sfr.format(el.getDouble(3))+" re1 = "+FormatUtils.efr.format(el.getDouble(4))+" mre = "+FormatUtils.efr.format(el.getDouble(5)))}

    val aux = sc.parallelize(studyResults)

    sql.createDataFrame(aux,ParamStudySchema.getSchema()).coalesce(1).write.option("header", "true").option("delimiter",";").mode(SaveMode.Overwrite).csv(ouputPath)

  }

  def single (trainingPath:String,testPath:String,ouputPath:String,miniBatchFraction:Double,window:Int,numIterations:Int,stepSize:Double,singlePath:String):Unit={

    val trainingRDD = sc.textFile(trainingPath).map(LabeledPoint.parse).cache()

    val testRDD     = sc.textFile(testPath).map(LabeledPoint.parse).cache()

    val res = Linear.trainTestOneLrModel(trainingRDD,testRDD,miniBatchFraction,window,numIterations,stepSize)
    val predictions = res._1

    val experiment  = res._2

    println("numIterations = "+experiment.getInt(3)+", stepSize = "+FormatUtils.sfr.format(experiment.getDouble(4))+" ==> mre = "+FormatUtils.efr.format(experiment.getDouble(0)*100)+" %")

    val aux1 = sc.parallelize(predictions)

    println("Saving predictions")

    sql.createDataFrame(aux1,OfflinePredictionsSchema.getSchema(window)).coalesce(1).write.option("header", "true").option("delimiter",";").mode(SaveMode.Overwrite).csv(singlePath+"/predictions")

    val aux2 = sc.parallelize(mutable.MutableList[Row]()+=experiment)

    println("Saving experiments")

    sql.createDataFrame(aux2,OfflineExperimentSchema.getSchema(window)).coalesce(1).write.option("header", "true").option("delimiter",";").mode(SaveMode.Overwrite).csv(singlePath+"/experiment")

  }


  def overFittingCheck(trainingPath:String,testPath:String, exp:String,miniBatchFraction:Double,window:Int,numIterations:Int,stepSize:Double): Unit ={

    println("Experiment = "+exp+", w = "+window+", numIterations = "+numIterations+", stepSize = "+FormatUtils.sfr.format(stepSize))

    val trainingRDD = sc.textFile(trainingPath).map(LabeledPoint.parse).cache()

    val testRDD     = sc.textFile(testPath).map(LabeledPoint.parse).cache()

    val resTrainingTraining = Linear.trainTestOneLrModel(trainingRDD,trainingRDD,miniBatchFraction,window,numIterations,stepSize)

    val mre1 = resTrainingTraining._2.getDouble(0)

    println("mre training-training = "+FormatUtils.efr.format(mre1*100)+" %")

    val resTrainingTest = Linear.trainTestOneLrModel(trainingRDD,testRDD,miniBatchFraction,window,numIterations,stepSize)

    val mre2 = resTrainingTest._2.getDouble(0)

    println("mre training-test = "+FormatUtils.efr.format(mre2*100)+" %")

    println("difference = "+FormatUtils.efr.format((mre1*100)-(mre2*100)))

  }

}