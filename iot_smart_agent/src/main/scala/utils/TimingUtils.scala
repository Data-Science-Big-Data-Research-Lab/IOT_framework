package utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
  * Created by davgutavi on 22/03/17.
  */
object TimingUtils {

  def time[T](block: => T): T = {
    val start = System.currentTimeMillis
    val res = block
    val millis = System.currentTimeMillis - start

    var x = millis /1000
    val sec = x % 60
    x = x / 60
    val min = x % 60
    x = x / 60
    val hor = x % 24
    x = x / 24
    val day = x

    println("Elapsed time: "+day+" d, "+hor+" h, "+min+" m, "+sec+" s ("+millis+" ms)" )
    res
  }



  def getTimeString (seconds:Int): String = {

    val millis = seconds*1000

    val df = new java.text.SimpleDateFormat("HH:mm:ss");

    df.getTimeZone.getOffset(millis)

    df.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))

    df.format(millis)


    //    var x = seconds
    //    val sec = x % 60
    //    x = x / 60
    //    val min = x % 60
    //    x = x / 60
    //    val hor = x % 24

    //    hor+" h, "+min+" m, "+sec+" s"
    //    hor+":"+min+":"+sec


  }

}