package examples

import com.cloudera.sa.{ExcelFileInputFormat, TextArrayWritable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sabhyankar on 3/16/17.
  */
object ExcelReaderMain {

  val TEST_TABLE = "excel_sample"

  def main(args: Array[String]) {

    val input = args(0).trim
    val conf = new SparkConf().setAppName("Excel-Reader-Example")
    val hadoopConf = new Configuration()
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val dataRdd = sc.newAPIHadoopFile(input,classOf[ExcelFileInputFormat],
      classOf[NullWritable],
      classOf[TextArrayWritable],
      hadoopConf)
    val dataRowRdd = dataRdd.map( x => Row.fromSeq(x._2.toStrings.toSeq))

    val dropTableSql = s"drop table if exists $TEST_TABLE"
    val createTableSql = s"""create table $TEST_TABLE (product_id string, product_sku string, product_price string)
                            stored as parquet
                         """
    hc.sql(dropTableSql)
    hc.sql(createTableSql)
    val schema = hc.table(TEST_TABLE).schema

    val df = hc.createDataFrame(dataRowRdd, schema)

    df.write.mode("overwrite").saveAsTable(TEST_TABLE)
  }

}
