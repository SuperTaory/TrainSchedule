import GeneralFunctionSets.{dayOfMonth_long, hourOfDay_long, transTimeToTimestamp, transTimeToString}
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{abs, max}

object ScheduleCount {
    def main(args: Array[String]): Unit = {
        /**
         * 统计站点的不同时刻下AP轨迹点的分布情况
         */
        val spark = SparkSession
            .builder()
            .appName("TrainSchedule")
            .getOrCreate()
        val sc = spark.sparkContext

        val stations = Set("深大", "高新园", "白石洲", "世界之窗")
        val lineMap = Map("260"->2, "261"->3, "262"->4, "263"->5, "268"->1, "241"->11, "265"->7, "267"->9)

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
        val stationFile = sc.textFile(args(0) + "/zlt_hdfs/AllInfo/stationInfo-UTF-8.txt")
        val stationNo2NameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

        val readODTimeInterval = sc.textFile(args(0) + "/zlt_hdfs/UI/AllODTimeInterval/ShortPathTime/part-*").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // 260011,赤湾,260,地铁二号线,22.47947413023911,113.89872986771447
        // 获取各个站点所属线路编号集合
        val lineInfo = sc.textFile(args(0) + "/zlt_hdfs/AllInfo/subway_zdbm_station.txt").map(row => {
            val fields = row.split(",")
            val station = fields(1)
            val lineNo = lineMap(fields(2))
            (station, lineNo)
        }).groupByKey().mapValues(_.toSet).collect().toMap

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        val validPathFile = sc.textFile(args(0) + "/zlt_hdfs/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ').dropRight(5)
            val sou = stationNo2Name.value(fields(0).toInt)
            val des = stationNo2Name.value(fields(fields.length - 1).toInt)
            val path = fields.map(x => stationNo2Name.value(x.toInt))
            ((sou, des), path)
        }).groupByKey().mapValues(x => x.toArray)
        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // 读取AP数据 (9CE82B094191,2019-06-09 20:45:13,福永,147)
        val apFile = sc.textFile(args(0) + "/zlt_hdfs/UI/NormalMacData/part*").map(line => {
            val fields = line.split(',')
            val apId = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            val dur = fields(3).dropRight(1).toLong
            (apId, (time, station, dur))
        })
        val apData = apFile.filter(x => hourOfDay_long(x._2._1) >= 6  & x._1 != "000000000000")
            .groupByKey()
            .mapValues(_.toArray.sortBy(_._1))

        // 划分为出行片段并标记出行日期
        val APSegments = apData.flatMap(line => {
            // 设置出行片段长度阈值
            val m = 2
            val MacId = line._1
            val data = line._2
            val segment = new ListBuffer[(Long, String, Long)]
            val segments = new ListBuffer[List[(Long, String, Long)]]
            for (s <- data) {
                if (segment.isEmpty) {
                    segment.append(s)
                }
                else {
                    if (s._2 == segment.last._2) {
                        if (segment.length > m) {
                            segments.append(segment.toList)
                        }
                        segment.clear()
                    }
                    else {
                        // 设置容忍时间误差
                        var attachInterval = 0
                        val odInterval = ODIntervalMap.value((segment.last._2, s._2))
                        odInterval / 1800 match {
                            case 0 => attachInterval = 600 //10min
                            case 1 => attachInterval = 900 //15min
                            case _ => attachInterval = 1200 // 20min
                        }
                        val realInterval = abs(s._1 - segment.last._1 - segment.last._3)
                        if (realInterval > odInterval + attachInterval) {
                            if (segment.length > m) {
                                segments.append(segment.toList)
                            }
                            segment.clear()
                        }
                    }
                    segment.append(s)
                }
            }
            if (segment.length > m) {
                segments.append(segment.toList)
            }
            for (trip <- segments.toList) yield
                (MacId, trip)
        }).filter(line => {  // 判断起终点之间的时间差是否合适
            val trip = line._2
            val o = trip.head
            val d = trip.last
            val odInterval = ODIntervalMap.value((o._2, d._2))
            val realInterval = abs(o._1 - d._1)
            if (realInterval > odInterval + 1800 | o._2 == d._2)
                false
            else
                true
        })

        // 判断出行片段是否存在匹配的路径
        val filterPath = APSegments.filter(line => {
            val segment = line._2.map(_._2)
            val od = (segment.head, segment.last)
            val paths = validPathMap.value(od)
            var flag = true
            for (path <- paths if flag) {
                var index1 = 0
                var index2 = 0
                while (index1 < segment.length & index2 < path.length) {
                    if (segment(index1) == path(index2)){
                        index1 += 1
                        index2 += 1
                    }
                    else {
                        index2 += 1
                    }
                }
                if (index1 == segment.length)
                    flag = false
            }
            if (flag)
                false
            else
                true
        })


        val filterPosition = filterPath.flatMap(line => {
            // 去掉首尾
            val trip = line._2.drop(1).dropRight(1)
            // 过滤出非换乘站轨迹点集合并且停留时间小于1min
            val positions = trip.filter(x => lineInfo(x._2).size == 1 & x._3 <= 60)
                .map(x => (lineInfo(x._2).head, x._2, x._1, x._3))
            // 处理每个换乘站轨迹点(保留没换乘的轨迹点)
            val trans = new ListBuffer[(Int, String, Long, Long)]
            for (i <- 1.until(trip.length - 1)) {
                val before = lineInfo(trip(i-1)._2)
                val now = lineInfo(trip(i)._2)
                val after = lineInfo(trip(i+1)._2)
                if (now.size > 1){
                    // 取三个站点所属线路的交集并判断是否属同一线路
                    val intersect = before & now & after
                    if (intersect.size == 1 & trip(i)._3 <= 60)
                        trans.append((intersect.head, trip(i)._2, trip(i)._1, trip(i)._3))
                }
            }
            // (线路编号、站点名、采样时间、停留时间, ID)
            for (x <- positions ++ trans.toList) yield
                ((x._1, x._2), x._3, x._4, line._1)
        })

        val watch_station = args(1)

//        // 根据输入的观察时间段和站点，筛选对应的乘客ID集合
//        val from = (args(2).split("_").head.toInt - 6) * 360 + args(1).split("_").last.toInt * 6
//        val to = (args(3).split("_").head.toInt - 6) * 360 + args(2).split("_").last.toInt * 6
//        val day = args(4).toInt
//        val res = filterPosition
//            .filter(x => x._1._2 == watch_station & func(x._2) >= from & func(x._2) <= to & dayOfMonth_long(x._2) == day)
//            .map(_._4).collect().toSet
//
//        // 根据筛选的ID集合过滤出它们的原始数据并保存
//        apFile.filter(x => res.contains(x._1) & dayOfMonth_long(x._2._1) == day & func(x._2._1) > from - 720 & func(x._2._1) < to + 720)
//            .repartition(1)
//            .sortBy(x => (x._1, x._2._1))
//            .map(x => (x._1, transTimeToString(x._2._1), x._2._2, x._2._3))
//            .saveAsTextFile(args(0) + "/zlt_hdfs/TS-2021/watch-" + args(1) + "-" + args(2) + "-" + args(3))



        // 挑选站点进行统计观察
        val for_watch = filterPosition.filter(x => x._1._2 == watch_station)
            .map(line => ((line._1._1, line._1._2, dayOfMonth_long(line._2)), (line._2, line._3)))

        // key: line_number, station, dayOfMonth
        // val: 不同时间段的轨迹点数量分布
        val results = for_watch.groupByKey().mapValues(v => {
            val range = v.toArray.map(y => Array.range(func(y._1), func(y._1 + y._2) + 1))
            val data = range.reduce((x, y) => x ++ y)
            val maxN = data.max
            val res = new Array[Int](maxN+1)
            val grouped = data.groupBy(x=>x).mapValues(_.length)
            for (n <- grouped.keys){
                res(n) = grouped(n)
            }
            res.mkString(",")
        })

        // 保存结果
        results.repartition(1)
            .sortByKey()
            .map(y => (y._1._1, y._1._2, y._1._3, y._2))
            .saveAsTextFile(args(0) + args(2))

        sc.stop()
    }

    def func(t: Long): Int = {
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
        // 距离6点钟的秒数以10s为单位划分
        (((H-6) * 60 + M) * 60 + S) / 10
    }

    def dayOfWeek(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.DAY_OF_WEEK)
    }
}
