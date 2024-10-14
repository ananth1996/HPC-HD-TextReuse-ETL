// load spark jars
// interp.load.cp(os.list(os.root/"Users"/"mahadeva"/"Research"/"etl_textreuse"/".venv"/"lib"/"python3.10"/"site-packages"/"pyspark"/"jars"))
interp.load.cp(os.list(os.root/'usr/'local/'spark/'jars))
import coursierapi.MavenRepository

interp.repositories.update(
  interp.repositories() ::: List(MavenRepository.of("https://repos.spark-packages.org/"))
)

@

import $ivy.`sh.almond::almond-spark:0.13.2`
import $ivy.`graphframes:graphframes:0.8.2-spark3.2-s_2.12`
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
        //.master("local[*]")
        .config(sparkConf)
        .getOrCreate() 
    }
    spark.sparkContext.setLogLevel("WARN")
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",sys.env.getOrElse("AWS_ACCESS_KEY_ID",""))
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",sys.env.getOrElse("AWS_SECRET_KEY",""))
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint",sys.env.getOrElse("AWS_ENDPOINT_URL","https://a3s.fi"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",sys.env("AWS_ACCESS_KEY_ID"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",sys.env("AWS_SECRET_KEY"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint",sys.env("AWS_ENDPOINT_URL"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access","true")
    spark.sparkContext.setCheckpointDir("/home/jovyan/work/spark-checkpoint")    
    //spark.sparkContext.setCheckpointDir("/Users/mahadeva/spark-checkpoint")
    spark
    
}

val spark = getSpark()
import spark.implicits._
val sc = spark.sparkContext

val a3s_url = "s3a://"
// val processed_bucket = sys.env.getOrElse("PROCESSED_BUCKET","textreuse-dagster-rahti2-test")
val processed_bucket = sys.env("PROCESSED_BUCKET")
val a3s_path = a3s_url+processed_bucket+"/"

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
    //Write dataframe with row numbers to correct location
    materialise_a3s(fname,dfZipWithIndex(df,1,col_name))
  
}

