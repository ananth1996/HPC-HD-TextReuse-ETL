// load spark jars
interp.load.cp(os.list(os.root/'usr/'local/'spark/'jars))

@

import $ivy.`sh.almond::almond-spark:0.13.2`
import org.apache.spark.sql._
import scala.collection.JavaConverters._

def getSpark(): SparkSession = {
    import org.apache.spark.sql.almondinternals.NotebookSparkSessionBuilder
    val sparkConf = new org.apache.spark.SparkConf()
    val properties = new java.util.Properties()
    properties.load(scala.io.Source.fromFile("/usr/local/spark/conf/spark-defaults.conf").bufferedReader)
    sparkConf.setAll(properties.asScala)
    val spark = { 
        new NotebookSparkSessionBuilder() {
            override def config(key: String, value: String) = {
                if (key=="spark.repl.class.uri")
                    super.config(key,value+"/")
                else 
                    super.config(key,value)
            }    
        }
        .config(sparkConf)
        .getOrCreate() 
    }
    spark.sparkContext.setLogLevel("WARN")
    spark
}

val spark = getSpark()
import spark.implicits._
val sc = spark.sparkContext

def jdbc_opts(con: DataFrameReader): DataFrameReader = {
    return con
        .format("jdbc")
        .option("driver", "org.mariadb.jdbc.Driver")
        .option("url", "URL")
        .option("user", USERNAME)
        .option("password", "PASSWORD")
        .option("fetchsize","100000")
        .option("batchsize","100000")
}

def jdbc_opts[T](con: DataFrameWriter[T]): DataFrameWriter[T] = {
    return con
        .format("jdbc")
        .option("driver", "org.mariadb.jdbc.Driver")
        .option("url", "URL")
        .option("user", USERNAME)
        .option("password", "PASSWORD")
        .option("fetchsize","100000") 
        .option("batchsize","100000")
}

def get_table(table: String): DataFrameReader = {
    return jdbc_opts(spark.read)
        .option("dbtable", table)
}

def get_limits(table: String, column: String): (Long,Long) = {
    val row = jdbc_opts(spark.read)
      .option("query", "SELECT CAST(MIN("+column+") AS INT), CAST(MAX("+column+") AS INT) FROM "+table)
      .load()
      .collect()(0)
    return (row.getLong(0), row.getLong(1))
}

def get_partitioned(table: String, column: String, numPartitions: Int = 200): DataFrameReader = {
    val (lb, ub) = get_limits(table, column)
    return jdbc_opts(spark.read)
        .option("dbtable", table)
        .option("partitionColumn", column)
        .option("numPartitions", numPartitions)
        .option("lowerBound", lb)
        .option("upperBound", ub)
}

val a3s_path = "s3a://textreuse-processed-data/"

def noop(name: String, df: DataFrame) = df

def register(name: String, df: DataFrame, cache: Boolean = false): DataFrame = {
  if (cache) {
        df.createOrReplaceTempView(name+"_source")
        spark.sql("DROP TABLE IF EXISTS "+name)
        spark.sql("CACHE TABLE "+name+" AS TABLE "+name+"_source")
        return spark.sql("SELECT * FROM "+name)
    } else {
        df.createOrReplaceTempView(name)
        return df
    }
}

def get_local(name: String, cache: Boolean = false): DataFrame = {
    return register(name, spark.read.parquet(name+".parquet"), cache)
}

def materialise_local(name: String, df: DataFrame, cache: Boolean = false): DataFrame = {
    df.write.mode("overwrite").option("compression","zstd").parquet(name+".parquet")
    return get_local(name, cache)
}

val a3s_hfs = org.apache.hadoop.fs.FileSystem.get(
        java.net.URI.create(a3s_path),
        sc.hadoopConfiguration
    )

def a3s_path_exists(path: String): Boolean = {
    return a3s_hfs.exists(new org.apache.hadoop.fs.Path(path))
}

def get_a3s(name: String, cache: Boolean = false): DataFrame = {
    return register(name, spark.read.parquet(a3s_path+name+".parquet"), cache)
}

def materialise_a3s(name: String, df: DataFrame, cache: Boolean = false): DataFrame = {
    df.write.mode("overwrite").option("compression","zstd").parquet(a3s_path+name+".parquet")
    return get_a3s(name, cache)
}

def materialise_a3s_if_not_exists(name: String, df: DataFrame, cache: Boolean = false): DataFrame = {
    if (!a3s_path_exists(a3s_path+name+".parquet"))
        return materialise_a3s(name, df, cache)
    else
        return get_a3s(name, cache)
}

def delete_s3(fname:String){
    val _path = new org.apache.hadoop.fs.Path(a3s_path+fname+".parquet")
    if (a3s_hfs.exists(_path))
        a3s_hfs.delete(_path)
}

def rename_s3(src_fname:String,dst_fname:String){
    val _src_path = new org.apache.hadoop.fs.Path(a3s_path+src_fname+".parquet")
    val _dst_path = new org.apache.hadoop.fs.Path(a3s_path+dst_fname+".parquet")
    if (a3s_hfs.exists(_src_path)){
        a3s_hfs.rename(_src_path,_dst_path)
    }
}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{min, max}

// from https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex
def dfZipWithIndex(
  df: DataFrame,
  offset: Int = 1,
  colName: String = "id",
  inFront: Boolean = true
) : DataFrame = {
  df.sqlContext.createDataFrame(
    df.rdd.zipWithIndex.map(ln =>
      Row.fromSeq(
        (if (inFront) Seq(ln._2 + offset) else Seq())
          ++ ln._1.toSeq ++
        (if (inFront) Seq() else Seq(ln._2 + offset))
      )
    ),
    StructType(
      (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]()) 
        ++ df.schema.fields ++ 
      (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
    )
  ) 
}

def materialise_row_numbers(fname:String,df:DataFrame,col_name:String){
    // materialise the dataframe as temp folder
    var _df = materialise_a3s(fname+"_tmp",df)

    // check if the row_number column already exists
    if (!_df.columns.contains(col_name)){
        //Write dataframe with row numbers to correct location
        _df = materialise_a3s(fname,dfZipWithIndex(_df,1,col_name))
        // then drop old parquet
        delete_s3(fname+"_tmp")
    }
    else{
        // rename tmp as correct location
        rename_s3(fname+"_tmp",fname)
    }
    return _df
}
