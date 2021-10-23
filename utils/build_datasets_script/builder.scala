/*define import */
import scala.util.parsing.json._
import sys.process._
import scala.collection.mutable.ListBuffer
import java.util.{Date, Properties}
import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

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

	/*
	** change log properties
	*/
  	//Logger.getLogger("org").setLevel(Level.OFF)


	/*
	** function to parse the data from the API call
	** takes as input the string of the api call
	** gives as output a structured list of Records
	*/
	def parser(string: String, name: String): Dataset[Record] = {
		val parsedData = for {
			Some(MAP(message)) <- List(JSON.parseFull(string))
			DOUBLE(longitude) = message("lon")
			DOUBLE(latitude) = message("lat")
			LIST(relevations) = message("hourly")
			MAP(relevation) <- relevations
			DOUBLE(ts) = relevation("dt")
			DOUBLE(temp) = relevation("temp")
			DOUBLE(tempfelt) = relevation("feels_like")
			DOUBLE(pressure) = relevation("pressure")
			DOUBLE(humidity) = relevation("humidity")
			LIST(weatherlist) = relevation("weather")
			MAP(weathermap) <- weatherlist
			STRING(weather) = weathermap("main")
			
		} yield {
			
			/* gathering the parsed values for a datapoint into an organized structure */
			Record(name, ts, longitude, latitude, temp, tempfelt, pressure, humidity, weather)
		}
		
		/* returning all the datapoints correctly parsed */
		val spark = SparkSession.builder().master("local").getOrCreate()
		import spark.implicits._
		parsedData.toDF
	}


	/* define main function */
	def main() { 
		
		val cities = List("Milano", "Torino", "Genova", "Aosta", "Trento", "Trieste", "Venezia", "Bologna", "Firenze", "Perugia", "Ancona", "Roma", "Aquila", "Campobasso", "Napoli", "Bari", "Potenza", "Catanzaro", "Palermo", "Cagliari")
		
		for(city <- cities) {
			
			val input = "./datasets/"+city+".json"
			val output = "./parsed_datasets/3_init/"+city+".json"
			
			val content = scala.io.Source.fromFile(input).mkString
			
			val data: Dataset[Record] = parser(content, city)
			
			data.write.format("json").save(output)
			//data.show()
		}

	}
	
	/* calling the main function */
	main()
}
