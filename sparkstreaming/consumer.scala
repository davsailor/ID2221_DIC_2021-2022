/*
** KAFKA CONSUMER WITH SPARK STREAMING AND MLLIB
** This file contains the code related to the core of the app: receives data, parses data, compute forecasts
*/

/* define package */
package sparkstreaming

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


/* define the case class of the parsed data */
case class Record(name: String, ts: Double, lon: Double, lat: Double, temp: Double, tempfelt: Double, pressure: Double, humidity: Double, weather: String)
case class Result(lon: Double, lat: Double, cweather: String, fweather: String, ctemp: Double, ftemp: Double)

/* define class and objects to parse the data coming from the API call */
class PARSER[T] { def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T]) }
object MAP extends PARSER[Map[String, Any]]
object STRING extends PARSER[String]
object DOUBLE extends PARSER[Double]


object ScalaWeatherForecast extends App {
	
	/* define constants */
	val BROKER_URL = "localhost:9092"
	val RECEIVER_TOPIC = "weather"
	val SENDER_TOPIC = "forecast"
	val CASSANDRA_KS = "weatherks"
	val CASSANDRA_TB = "relevationstb"

	/* change log properties */
	Logger.getLogger("org").setLevel(Level.OFF)
	
	/* function to parse data */
	def parser(string: String): Record = {
		val newmap = string.substring(1, string.length - 1)
				.split(",")
				.map(_.split(":"))
				.map{case Array(k, v) => (k.substring(1, k.length-1), v)}
				.toMap
		Record(newmap("name"), newmap("ts").toDouble, newmap("lon").toDouble, newmap("lat").toDouble, newmap("temp").toDouble, newmap("tempfelt").toDouble, newmap("pressure").toDouble, newmap("humidity").toDouble, newmap("weather"))
	
	}
	
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
	
	/* function to initialize kafka receiver */
	def initKafkaReceiver(context: StreamingContext): InputDStream[(String, String)] = {
		val conf = Map(
			"metadata.broker.list" -> "localhost:9092",
			"zookeeper.connect" -> "localhost:2181",
			"group.id" -> "kafka-spark-streaming",
			"zookeeper.connection.timeout.ms" -> "5000")
		val topics = Set(RECEIVER_TOPIC)
		val input = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, conf, topics)
		input
	}
	
	/* function to initialize kafka producer */
	def initKafkaProducer(brokers: String): KafkaProducer[String, String] = {
		val props = new Properties()
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaForecastProducer")
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
		new KafkaProducer[String, String](props)
	}
	
	/*
	** function to build a message with the relevant data for
	** the app.js client in json format, to simplify the parsing
	*/
	def buildMessage(data: Result): String = {
		val message = """{"lon":""".stripMargin + data.lon.toString +
					  ""","lat":""".stripMargin + data.lat.toString +
					  ""","cweather":""".stripMargin + "\"" + data.cweather.toString + "\"" +
					  ""","fweather":""".stripMargin + "\"" + data.fweather.toString + "\"" +
					  ""","ctemp":""".stripMargin + data.ctemp.toString +
					  ""","ftemp":""".stripMargin + data.ftemp.toString + "}"
		message
	}
	
	/* function to initialize cassandra */
	def initCassandra(): Session = {
		val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
		val cs = cluster.connect()
		val ks_query = new StringBuilder("CREATE KEYSPACE ")
							.append(CASSANDRA_KS)
							.append(" WITH replication={'class':'SimpleStrategy','replication_factor':1};")
		try {
			cs.execute(ks_query.toString)
		} catch {
			case x: com.datastax.driver.core.exceptions.AlreadyExistsException => {
				print("Keyspace " + CASSANDRA_KS + " already exists!")
			}
		}
		val use_ks_query = new StringBuilder("USE ").append(CASSANDRA_KS)
		val tb_query = new StringBuilder("CREATE TABLE ")
							.append(CASSANDRA_TB)
							.append(" (name text, ts double, lon double, lat double, temp double, tempfelt double, pressure double, humidity double, weather text, PRIMARY KEY((name, ts)));")
		val session = cluster.connect(CASSANDRA_KS)
		try {
			cs.execute(use_ks_query.toString)
			cs.execute(tb_query.toString)
		} catch {
			case x: com.datastax.driver.core.exceptions.AlreadyExistsException => {
				print("Table " + CASSANDRA_KS + "." + CASSANDRA_TB + " already exists!")
			}
		}
		cs
	}
	
	/* function to save the RDD to cassandra */
	def storeToCassandra(rdd: RDD[(String, String)]): Unit = {
		val records = rdd.map(w => parser(w._2))
						 .map(r => (r.name, r.ts, r.lon, r.lat, r.temp, r.tempfelt, r.pressure, r.humidity, r.weather));
		records.saveToCassandra(CASSANDRA_KS, CASSANDRA_TB, SomeColumns("name", "ts", "lon", "lat", "temp", "tempfelt", "pressure", "humidity", "weather"))
	}
	
	/* TODO:function to predict stuff
	** IT HAS TO BE CHANGED, THIS IS JUST TO TEST VISUALIZATION
	*/
	def predict(input: Record): Result = {
		val output = Result(input.lon, input.lat, input.weather, "Clear", input.temp, input.temp+10.0)
		output
	}
	
	/* define main function */
	def main() {
		
		val ss = initSession()
		val ssc = initContext(ss)
		val cs = initCassandra()
		
		val input = initKafkaReceiver(ssc)

		input.foreachRDD(rdd => {
			storeToCassandra(rdd)
			rdd.foreach(data => {
				val producer = initKafkaProducer(BROKER_URL)
				val parsedData = parser(data._2)
				val result = predict(parsedData)
				val message = new ProducerRecord[String, String](SENDER_TOPIC, null, buildMessage(result))
				print(message + "\n")
				producer.send(message)
				producer.close()
			})
		})

		ssc.start()
		ssc.awaitTermination()
		cs.close() 
	}
	
	/* calling the main function */
	main()
}
