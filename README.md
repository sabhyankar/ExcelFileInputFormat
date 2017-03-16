ExcelFileInputFormat
====================

A Hadoop FileInputFormat implementation for reading Microsoft Excel (xlsx) files.
---------------------------------------------------------------------------------

The ExcelFileInputFormat reads single sheet .xlsx files and returns a Key-Value pair consisting of a NullWritable Key 
and a TextArrayWritable value. i.e It returns every row in the excel sheet as an Array of Text(String) objects. 

The ExcelFileInputFormat can be used for MapReduce as well as for Spark (via newAPIHadoopFile) based access to Excel 
files stored in a Hadoop cluster. The Array[Text] object can be consumed as needed.

The following example shows how the ExcelFileInputFormat can be utilized to read a few .xlsx files in hdfs, parse it and
write the data into a Hive/Impala table. This is a very standard step in an ETL job.

Note that the Array[Text] can be cast to appropriate data types as needed once it is converted into a Spark DataFrame.

### Sample Data (see resources/data) ###
~~~
product_id  product_sku    product_price
----------  -----------    -------------
1001        ABC1001-SKU    987.2923028
1002        ABC1002-SKU    166.8425684
1003        ABC1003-SKU    388.8724633
....        ...........    ...........
~~~

### Copy sample data to hdfs ###
~~~
hdfs dfs -copyFromLocal ~/price_list* /user/sca/excel_data/
hdfs dfs -ls /user/sca/excel_data
Found 3 items
-rw-r--r--   3 sca supergroup      53224 2017-03-16 15:42 /user/sca/excel_data/price_list1.xlsx
-rw-r--r--   3 sca supergroup      53275 2017-03-16 15:42 /user/sca/excel_data/price_list2.xlsx
-rw-r--r--   3 sca supergroup      53362 2017-03-16 15:42 /user/sca/excel_data/price_list3.xlsx
~~~

### Run sample program to process excel files and insert data into an Impala/Hive table ###
~~~
spark-submit --master yarn \
--class examples.ExcelReaderMain \
excel-inputformat-1.0-jar-with-dependencies.jar "hdfs://myHostName:8020/user/sca/excel_data/"
~~~

### The sample program runs through the following steps ###
~~~
// Create a RDD of (NullWritable,TextArrayWritable) objects using the
// ExcelFileInputFormat as the InputFormat
    val dataRdd = sc.newAPIHadoopFile(input,classOf[ExcelFileInputFormat],
      classOf[NullWritable],
      classOf[TextArrayWritable],
      hadoopConf)
// We extract the TextArrayWritable objects from the Tuple and convert them to a Spark Row Object
// This gives us one Row object for every record in every .xlsx files that we processed
    val dataRowRdd = dataRdd.map( x => Row.fromSeq(x._2.toStrings.toSeq))

    val dropTableSql = s"drop table if exists $TEST_TABLE"
// We create a simple table with three columns 
// product_id String
// product_sku String
// product_price String
// This data from the .xlsx file will be inserted into this table.
    val createTableSql = s"""create table $TEST_TABLE (product_id string, product_sku string, product_price string)
                            stored as parquet
                         """
    hc.sql(dropTableSql)
    hc.sql(createTableSql)
 // We extract the schema of the test table that we created. 
 // The schema information will be used to convert the RDD[Row] to a Spark DataFrame
    val schema = hc.table(TEST_TABLE).schema

    val df = hc.createDataFrame(dataRowRdd, schema)

// The DataFrame (df) is written into the test table created earlier
    df.write.mode("overwrite").saveAsTable(TEST_TABLE)
~~~

### Finally we verify that the data is inserted correctly by querying it via Impala ###
~~~
impala-shell -i myHost -q "invalidate metadata excel_sample; select * from excel_sample limit 20"
Starting Impala Shell without Kerberos authentication
Connected to myHost:21000
Query: invalidate metadata excel_sample

Fetched 0 row(s) in 0.04s
Query: select * from excel_sample limit 20
+------------+-------------+--------------------+
| product_id | product_sku | product_price      |
+------------+-------------+--------------------+
| 1001.0     | ABC1001-SKU | 987.2923027637279  |
| 1002.0     | ABC1002-SKU | 166.84256837661704 |
| 1003.0     | ABC1003-SKU | 388.87246332151017 |
| 1004.0     | ABC1004-SKU | 352.71067050318317 |
| 1005.0     | ABC1005-SKU | 908.5760164719924  |
| 1006.0     | ABC1006-SKU | 623.8783947074145  |
| 1007.0     | ABC1007-SKU | 573.8034150329727  |
| 1008.0     | ABC1008-SKU | 156.03917383324506 |
| 1009.0     | ABC1009-SKU | 969.7358765989433  |
| 1010.0     | ABC1010-SKU | 745.2998028248956  |
| 1011.0     | ABC1011-SKU | 678.6687361069905  |
| 1012.0     | ABC1012-SKU | 901.4478881505204  |
| 1013.0     | ABC1013-SKU | 570.9165358023828  |
| 1014.0     | ABC1014-SKU | 814.5112345053641  |
| 1015.0     | ABC1015-SKU | 912.2096456336869  |
| 1016.0     | ABC1016-SKU | 716.8738114063498  |
| 1017.0     | ABC1017-SKU | 635.2177840579085  |
| 1018.0     | ABC1018-SKU | 997.445982906735   |
| 1019.0     | ABC1019-SKU | 849.5802769647676  |
| 1020.0     | ABC1020-SKU | 261.07068348744133 |
+------------+-------------+--------------------+
Fetched 20 row(s) in 2.94s
~~~

### Final Comments
The ExcelFileInputFormat is meant to demonstrate how easy it is to read and consume 
Excel files using [Apache POI](https://poi.apache.org/) and the MapReduce APIs. Microsoft fileformats such as Excel have
a huge numbers of features and functions that Apache POI can access.
