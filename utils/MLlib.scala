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
import org.apache.spark.ml.evaluation.RegressionEvaluator

/* define the case class of the parsed data */
case class Record(name: String, ts: Double, lon: Double, lat: Double, temp: Double, tempfelt: Double, pressure: Double, humidity: Double, weather: String)
case class Result(lon: Double, lat: Double, cweather: String, fweather: String, ctemp: Double, ftemp: Double)

/* define class and objects to parse the data coming from the API call */
class PARSER[T] { def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T]) }
object MAP extends PARSER[Map[String, Any]]
object STRING extends PARSER[String]
object DOUBLE extends PARSER[Double]

/*
	** function to parse the data from the API call
	** takes as input the string of the api call
	** gives as output a structured list of Records
	*/
	 def parser(string: String): List[Record] = {
		val parsedData = for {
			
			/* core of the parsing function */
			Some(MAP(message)) <- List(JSON.parseFull(string))
			LIST(relevations) = message("list")
			MAP(relevation) <- relevations
			STRING(cityName) = relevation("name")
			DOUBLE(ts) = relevation("dt")
			MAP(coords) = relevation("coord")
			DOUBLE(longitude) = coords("lon")
			DOUBLE(latitude) = coords("lat")
			MAP(main) = relevation("main")
			DOUBLE(temp) = main("temp")
			DOUBLE(tempfelt) = main("feels_like")
			DOUBLE(pressure) = main("pressure")
			DOUBLE(humidity) = main("humidity")
			LIST(weatherlist) = relevation("weather")
			MAP(weathermap) <- weatherlist
			STRING(weather) = weathermap("main")
			
		} yield {
			
			/* gathering the parsed values for a datapoint into an organized structure */
			Record(cityName, ts, longitude, latitude, temp, tempfelt, pressure, humidity, weather)
		}
	
	
	
	
	
	
	
	
	
	
	
	
	
