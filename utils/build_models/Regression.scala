/* define imports */
import scala.util.parsing.json._
import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.spark.sql.cassandra._
import com.datastax.driver.core.{Cluster, Session}
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.japi.CassandraJavaUtil._

/* define MLlib imports */
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoderEstimator}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics

object regressor extends App{

	/* function to initialize spark session with hdfs */
	def initSession(): SparkSession = {
		val ss = SparkSession.builder()
					.appName("WeatherForecast")
					.config("spark.cassandra.connection.host", "127.0.0.1")
				    .config("spark.cassandra.connection.port", "9042")
					.master("local[2]")
					.getOrCreate()
		import ss.implicits._
		ss
	}

	/* function to initialize spark context */
	def initContext(session: SparkSession): StreamingContext = {
		val ssc = new StreamingContext(session.sparkContext, Seconds(3))
		ssc.checkpoint("./tmp/checkpoint")
		ssc
	}

	val ss = initSession()
	val ssc = initContext(ss)
	val sql = ss.sqlContext

	var df = sql.read
		.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load("Dataset.csv")
		
	df = df.withColumnRenamed("temp_forecast", "label")

	var weatherIndexer = new StringIndexer()
		.setInputCol("weather")
		.setOutputCol("weatherIndexer")
		.setHandleInvalid("keep")
		
	df = weatherIndexer.fit(df).transform(df)

	var encoder = new OneHotEncoderEstimator()
		.setInputCols(Array(weatherIndexer.getOutputCol))
		.setOutputCols(Array("WeatherEncoded"))
		
	df = encoder.fit(df).transform(df)

	var assembler = new VectorAssembler()
		.setInputCols(Array("lon", "lat", "temp", "tempfelt", "pressure", "humidity", "hour", "WeatherEncoded"))
		.setOutputCol("features")
		
	df = assembler.transform(df)

	var Array(train, test) = df.randomSplit(Array(.8, .2), 13)

	var lr = new LinearRegression()

	var lrmodel = lr.fit(train)

	var lrPredictions = lrmodel.transform(test)

	val VP = test.map( point => {
		val prediction = lrmodel.predict(point.features)
		(prediction, point.label)
	})

	val metrics = new RegressionMetrics(VP)
	
	println(s"R2 = ${metrics.r2}")

}	

		
