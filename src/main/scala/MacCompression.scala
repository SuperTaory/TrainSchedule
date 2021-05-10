import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object MacCompression {
    def main(args: Array[String]): Unit = {
        /**
         * 将mac数据文件格式由parquet转换为普通格式，并对mac数据进行处理压缩；
         * 原Mac数据格式：[000000000140,1559177788,八卦岭,114.09508148721412,22.56193897883047,9]
         * 转换后Mac格式：000000000140,1559177788,八卦岭,停留时间-秒数
         */

        val spark = SparkSession.builder().appName("MacCompression").getOrCreate()
        val sc = spark.sparkContext


        val macFile = spark.read.parquet(args(0))
        macFile.printSchema()


        // 过滤掉数据量超大的数据避免数据倾斜
        val macRDD = macFile.select("mac", "time", "stationName").filter("macCount > 20 and macCount < 100000").rdd

        val transformedMacRDD = macRDD.map(line => {
            val fields = line.toString().split(',')
            val macId = fields(0).drop(1)
            val time = fields(1).toLong
            val station = fields(2).dropRight(1)
            (macId, (time, station))
        })

        val groupAndSort = transformedMacRDD.groupByKey().mapValues(_.toArray.sortBy(_._1))

        // 统计停留时间
        val compressedMacRDD = groupAndSort.flatMap(line => {
            var tempStation = "null"
            var tempTime = 0L
            var interval = 0L
            var preTime = 0L
            val processedData = new ListBuffer[(Long, String, Long)]
            for (x <- line._2) {
                if (x._2.equals(tempStation) && x._1 - preTime < 300) {
                    interval = x._1 - tempTime
                    preTime = x._1
                }
                else {
                    if (tempStation != "null") {
                        processedData.append((tempTime, tempStation, interval))
                        interval = 0
                    }
                    tempStation = x._2
                    tempTime = x._1
                    preTime = x._1
                }
            }
            processedData.append((tempTime, tempStation, interval))
            for (v <- processedData) yield {
                (line._1, v._1, v._2, v._3)
            }
        })

        compressedMacRDD.saveAsTextFile(args(1))

        sc.stop()
    }


    def transTimeToString(time_tamp: Long): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(time_tamp * 1000)
        time
    }
}
