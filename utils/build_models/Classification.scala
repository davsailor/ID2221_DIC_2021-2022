/* define imports */
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.{Date, Properties}

/* define MLlib imports */
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoderEstimator, IndexToString, VectorIndexer}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, DecisionTreeClassificationModel, LinearSVC}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline


object Classifier extends App{

	/* function to initialize spark session with hdfs */
	def initSession(): SparkSession = {
		val ss = SparkSession.builder().appName("Regressor").master("local[2]").getOrCreate()
		import ss.implicits._
		ss
	}
	
	Logger.getLogger("org").setLevel(Level.OFF)

	val spark = initSession()
	val sql = spark.sqlContext

	var data = sql.read.format("csv").option("header", "true").option("inferSchema", "true").load("Dataset.csv")
	
	data = data.withColumnRenamed("weather_forecast", "label")
	
	val labelIndexer = new StringIndexer()
		.setInputCol("label")
		.setOutputCol("indexedLabel")
		.fit(data)
	
	data = labelIndexer.transform(data)
	
	val weatherIndexer = new StringIndexer()
		.setInputCol("weather")
		.setOutputCol("indexedWeather")
		.fit(data)
	
	weatherIndexer.save("weatherIndexer.model")
		
	data = weatherIndexer.transform(data)
	
	var encoder = new OneHotEncoderEstimator()
		.setInputCols(Array(weatherIndexer.getOutputCol))
		.setOutputCols(Array("WeatherEncoded"))
		
	data = encoder.fit(data).transform(data)

	var assembler = new VectorAssembler()
		.setInputCols(Array("lon", "lat", "hour", "temp", "tempfelt", "pressure", "humidity", "temp_forecast", "WeatherEncoded"))
		.setOutputCol("features")
	
	data = assembler.transform(data)
		
	val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))
	
	val dt = new DecisionTreeClassifier()
		.setLabelCol("indexedLabel")
		.setFeaturesCol("features")
	
	val x = weatherIndexer.labels

	val labelConverter = new IndexToString()
		.setInputCol("prediction")
		.setOutputCol("predictedLabel")
		.setLabels(Array("Clear", "Clouds", "Fog", "Rain", "Snow"))
	
	labelConverter.save("labelConverter.model")
		
	val model1 = dt.fit(trainingData)
	
	val predictions1 = labelConverter.transform(model1.transform(testData))
	
	predictions1.select("predictedLabel", "label", "features").show(5)
	
	val evaluator = new MulticlassClassificationEvaluator()
		.setLabelCol("indexedLabel")
		.setPredictionCol("prediction")
		.setMetricName("accuracy")

	val accuracy1 = evaluator.evaluate(predictions1)

	println(s"Test Error = ${(1.0 - accuracy1)}")
	
	model1.save("dt_weather.model")

	spark.close()
}		
