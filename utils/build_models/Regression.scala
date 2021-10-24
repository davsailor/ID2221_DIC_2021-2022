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
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoderEstimator}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, GBTRegressor, GBTRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object Regressor extends App{

	/* function to initialize spark session with hdfs */
	def initSession(): SparkSession = {
		val ss = SparkSession.builder().appName("Regressor").master("local[2]").getOrCreate()
		import ss.implicits._
		ss
	}
	
	Logger.getLogger("org").setLevel(Level.OFF)

	val spark = initSession()
	val sql = spark.sqlContext

	var df = sql.read.format("csv").option("header", "true").option("inferSchema", "true").load("Dataset.csv")
		
	df = df.withColumnRenamed("temp_forecast", "label")

	var weatherIndexer = new StringIndexer()
		.setInputCol("weather")
		.setOutputCol("weatherIndexer")
		
	df = weatherIndexer.fit(df).transform(df)

	var encoder = new OneHotEncoderEstimator()
		.setInputCols(Array(weatherIndexer.getOutputCol))
		.setOutputCols(Array("WeatherEncoded"))
		
	df = encoder.fit(df).transform(df)
	
	var assembler = new VectorAssembler()
		.setInputCols(Array("lon", "lat", "hour", "temp", "tempfelt", "pressure", "humidity", "WeatherEncoded"))
		.setOutputCol("features")
		
	df = assembler.transform(df)
	
	var preprocessed = df.select("features", "label")

	var Array(train, test) = preprocessed.randomSplit(Array(.8, .2), 13)

	var lr = new LinearRegression()
	var lrmodel = lr.fit(train)
	val results = lrmodel.evaluate(test)
	
	var gbt = new GBTRegressor()
	var gbtmodel = gbt.fit(train)
	val predictions = gbtmodel.transform(test)
	//print(predictions.select(col("prediction")).first.getDouble(0))
	
	
	val evaluator = new RegressionEvaluator()
		.setLabelCol("label")
		.setPredictionCol("prediction")
		.setMetricName("rmse")
	val rmse = evaluator.evaluate(predictions)
	
	print("lr R2 Error: " + results.rootMeanSquaredError + "\n")
	print("gbt R2 Error: " + rmse + "\n")
	
	gbtmodel.save("gbt_temp.model")
/*	lrmodel.save("lr_temp.model")
*/
	spark.close()
}		
