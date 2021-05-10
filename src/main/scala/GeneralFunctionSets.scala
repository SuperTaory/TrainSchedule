import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

object GeneralFunctionSets {
    def transTimeToTimestamp(timeString: String): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }

    def transTimeToString(timeStamp: Long): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(timeStamp * 1000)
        time
    }

    def dayOfMonth_string(timeString: String): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.DAY_OF_MONTH)
    }

    def dayOfMonth_long(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.DAY_OF_MONTH)
    }

    def hourOfDay(timeString: String): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.HOUR_OF_DAY)
    }

    def hourOfDay_long(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.HOUR_OF_DAY)
    }

    // 获取时间戳对应当天的秒数
    def secondsOfDay(t: Long): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        val H = calendar.get(Calendar.HOUR_OF_DAY)
        val M = calendar.get(Calendar.MINUTE)
        val S = calendar.get(Calendar.SECOND)
        (H * 60 + M) * 60 + S
    }

    // 获取以半小时为单位的时间段序号当月的第几个半小时，0为起点
    def halfHourOfDay(t: Long): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        (calendar.get(Calendar.DAY_OF_MONTH) - 1) * 48 + calendar.get(Calendar.HOUR_OF_DAY) * 2 + calendar.get(Calendar.MINUTE) / 30
    }
}
