package it.unibo.big.normalized_fact

import java.time.Duration
import java.util.Date

private[normalized_fact] object DateUtils {
  /**
   * Add days to a date
   * @param date the date to add days
   * @param days the number of days to add
   * @return the date with the added days
   */
  def addDays(date: Date, days: Int = 3): Date = {
    import java.util.Calendar
    val cal: Calendar = Calendar.getInstance
    cal.setTime(date)
    cal.add(Calendar.DATE, days)
    cal.getTime
  }

  /**
   *
   * @param d1 start date (excluse)
   * @param d2 end date inclusive
   * @return the diff beteween tow dates including just the second
   */
  def getDaysDiff(d1: Date, d2: Date): Long = Duration.between(d1.toInstant, d2.toInstant).toDays

  /**
   *
   * @param taskDate       the date of the task
   * @param monitoringDate the monitoring date of this task
   * @return if the monitoring date is before task date, returns taskDate + 3 days else monitoring date
   */
  def getMonitoringDate(taskDate: Date, monitoringDate: Date): Date = {
    if (monitoringDate == null) null else if (getDaysDiff(taskDate, monitoringDate) < 0) addDays(taskDate) else monitoringDate
  }

  /**
   *
   * @param d1 the previous date of the difference
   * @param d2 the date next to past date
   * @return the difference of days between d1 and d2, if negative throws IllegalArgumentException
   */
  def getPositiveDateDiff(d1: Option[Date], d2: Date): Long = {
    val diff = getDaysDiff(d1.get, d2)
    if (diff < 0) {
      throw new IllegalArgumentException()
    }
    diff
  }
}
