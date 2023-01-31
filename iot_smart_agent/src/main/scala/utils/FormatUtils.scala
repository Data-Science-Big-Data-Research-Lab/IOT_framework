package utils

import java.text.{DecimalFormat, DecimalFormatSymbols}

object FormatUtils {

  final val otherSymbols = new DecimalFormatSymbols(java.util.Locale.getDefault)

  otherSymbols.setDecimalSeparator('.')

  otherSymbols.setGroupingSeparator(',')

  final val sfr = new DecimalFormat("#.####################", otherSymbols)

  final val efr = new DecimalFormat("#.#####", otherSymbols)

}
