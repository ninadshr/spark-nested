package scala.org.spark.nested

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Success, Failure }
import ExecutionContext.Implicits.global

object ParallelExec {
  def main(args: Array[String]): Unit = {

    //val runLocal = (args.length == 1 && args(0).equals("runlocal"))
    var spark: SparkSession = null
    val warehouseLocation = "/Users/ninad/local_database/"

    if (true) {
      spark = SparkSession
        .builder().master("local[1]")
        .appName("ParallelExecutor")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()

      spark.conf.set("spark.broadcast.compress", "false")
      spark.conf.set("spark.shuffle.compress", "false")
      spark.conf.set("spark.shuffle.spill.compress", "false")
    }

    val df = spark.range(1l, 10l)
    val df2 = spark.range(11l, 2000000000l)

    val f: Future[Long] = Future {
      df.count() //This can be any action you want to execute in parallel
    }

    val f2: Future[Long] = Future {
      df2.count()
    }

    f onComplete {
      case Success(cnt) => println("Df has count of ---------> " + cnt)
      case Failure(t) => println("An error has occured: " + t.getMessage)

    }

    f2 onComplete {
      case Success(cnt) => println("Df2 has a count of --------> " + cnt)
      case Failure(t) => println("An error has occured: " + t.getMessage)

    }

    Await.result(f, 100000 millis);
  }
}