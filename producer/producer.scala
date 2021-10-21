/*
** KAFKA PRODUCER
** This file contains the code related to the producer of the App
*/

/* define package */
package producer

/*define import */
import scala.util.parsing.json._
import sys.process._
import scala.collection.mutable.ListBuffer
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage
import org.apache.log4j.Logger
import org.apache.log4j.Level

/* define the case class of the parsed data */
case class Record(name: String, ts: Double, lon: Double, lat: Double, temp: Double, tempfelt: Double, pressure: Double, humidity: Double, weather: String)

/* define class and objects to parse the data coming from the API call */
class PARSER[T] { def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T]) }
object MAP extends PARSER[Map[String, Any]]
object LIST extends PARSER[List[Any]]
object STRING extends PARSER[String]
object DOUBLE extends PARSER[Double]
	
/* define object */
object ScalaWeatherProducer extends App {
	
	/* define constants
	** BROKER_URLS: the urls of kafka producers
	** TOEKN_KEY: the key to get access to the API call
	** TOPIC: the topic on which Kafka will produce messages
	** CITIES: list containing all the ids of Italian cities, obtained calling a python function
	** BATCH_SIZE: size of the batch of ids used to perform an API call
	** MAX_DELAY: maximum delay in sending the message, used to simulate an intermittent stream of data
	*/
	val BROKER_URLS = "localhost:9092"
	val TOKEN_KEY = "c1ebde1f788ab3675ef301f7e4ea8855"
	val TOPIC = "weather"
	var CITIES = List[String]()
	val BATCH_SIZE = 20 /* we set this to 20 because it is the maximum imposed by the API call */
	val MAX_DELAY = 200
	
	
	/*
	** change log properties
	*/
  	Logger.getLogger("org").setLevel(Level.OFF)
	
	
	/*
	** call python function extractor.py
	** to extract all the ids of Italian cities
	*/
	def callPythonExtractor(): List[String] = {
		val buffer = ListBuffer.empty[String]
		val output = "python3 ./../utils/italian_cities_extractor/extractor.py" ! ProcessLogger(buffer append _, stderr append _)
		Random.shuffle(buffer.toList)
	}
	
	
	/*
	** set the properties of kafka producer
	** instantiate a new kafka producer
	*/
	def initProducer(brokers: String): KafkaProducer[String, String] = {
		val props = new Properties()
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaWeatherProducer")
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
		new KafkaProducer[String, String](props)
	}
	
	
	/*
	** function to call the API on a defined set of strings and for a given token
	*/
	def getDataFromAPI(token: String, cities: String): String = {
		val APIurl = "http://api.openweathermap.org/data/2.5/group?id=" + cities + "&APPID=" + token
		val data = scala.io.Source.fromURL(APIurl).mkString
		data
	}
	
	
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
		
		/* returning all the datapoints correctly parsed */
		parsedData
	}
	
	
	/*
	** function to build a message with the relevant data for the consumer
	** in json format, to simplify the parsing
	** Record(name, ts, lon, lat, temp, tempfelt, pressure, humidity, weather)
	*/
	def buildMessage(data: Record): String = {
		val message = """{"name":""".stripMargin + data.name.toString +
					  ""","ts":""".stripMargin + data.ts.toString +
					  ""","lon":""".stripMargin + data.lon.toString +
					  ""","lat":""".stripMargin + data.lat.toString +
					  ""","temp":""".stripMargin + data.temp.toString +
					  ""","tempfelt":""".stripMargin + data.tempfelt.toString +
					  ""","pressure": """.stripMargin + data.pressure.toString +
					  ""","humidity":""".stripMargin + data.humidity.toString +
					  ""","weather":""".stripMargin + data.weather.toString + "}"
		message
	}


	/* define main function */
	def main() {
		
		/* initialize the producer */
		val producer = initProducer(BROKER_URLS)
		
		/* call the python script to extract all the ids of Italian cities */
		CITIES = callPythonExtractor()
		
		/* define the batches of cities for which we will perform a grouped API call */
		val batches = CITIES.grouped(BATCH_SIZE).toList.map(batch => batch.mkString(",")) 
		
		/* main cycle of the class */
		while(true) {
		
			/*
			** for each batch we performe an API call
			** we will introduce delays to respect the constraints
			** of the free plan of the API we are using
			*/
			for(batch <- batches) {
				
				/* perform the API call */
				val data = parser(getDataFromAPI(TOKEN_KEY, batch))
				
				/*
				** for each datapoint retrieved from the API call, 
				** we will produce an event on the specified topic channel
				*/
				for(datapoint <- data) {

					/* create the message */
					val message = new ProducerRecord[String, String](TOPIC, null, buildMessage(datapoint))

					/* send the message */
					producer.send(message)

					/* print the message for debugging */
					print(message + "\n")
					
					/* simulate an intermittent stream of data with a random delay */
					Thread.sleep(Random.nextInt(MAX_DELAY))
				}
				/* insert a pause to respect the free usage constraint of the API */
				Thread.sleep(1000)
			}
		}
		
		/* close the producer */
		producer.close()
	}
	
	/* calling the main function */
	main()
}
