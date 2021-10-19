/* needed imports */
import http from 'http';
import {WebSocketServer} from 'ws';
import express from 'express';
import kafkaNode from 'kafka-node';

/* define the constants to be used */
const PORT = 14520;
const KAFKA_TOPIC = "forecast";
const KAFKA_HOST = "localhost:9092";

/* create the app and define the static resources */
const app = express();
app.use(express.static("./static"));

/* create the server */
const server = http.createServer(app);
const socketServer = new WebSocketServer({server: server});

/* create and setup the kafka client */
const kafkaClient = new kafkaNode.KafkaClient({
										kafkaHost: KAFKA_HOST,
										connectTimeout: 10000,
										requestTimeout: 10000,
										autoConnect: true});

kafkaClient.on("connect", (error) => {
										if(error) {
											console.log("cannot establish connection");
											throw error;
										} else {
											console.log("connected");
										}
									});

kafkaClient.on("error", (error) => {throw error;});

/* refresh the metadata before sending the first message
** refreshing needed to avoid the error BrokerNotAvailableError: Could not find the leader
** reference at: https://www.npmjs.com/package/kafka-node#highlevelproducer-with-keyedpartitioner-errors-on-first-send
*/
kafkaClient.refreshMetadata([KAFKA_TOPIC], (error) => { 
														if(error) {
															console.log(error);
															throw error;
														} else {
															console.log("successfully refreshed metadata");
														}
													  });

/* create and setup the kafka consumer */
const kafkaConsumer = new kafkaNode.Consumer(kafkaClient, [{topic: KAFKA_TOPIC, partition: 0, offset: 0}], {autoCommit: false});
const kafkaOffset = new kafkaNode.Offset(kafkaClient);

kafkaConsumer.on("message", (message) => { 
										socketServer.clients.forEach(client => client.send(message));
										Log.insert({message})
											.then(() => {
															kafkaConsumer.commit((error, data) => console.log(error, data));
														})
									});

kafkaConsumer.on("error", (error) => {
									console.log(error);
									throw error;
								});
								
kafkaConsumer.on("offsetOutOfRange", (topic) => {
												kafkaOffset.fetchLatestOffsets([topic], (error, offsets) => {
																												const latestOffset = offsets[topic][0];
																												kafkaConsumer.setOffset(topic, 0, latestOffset);
																											})
										   });

/* start the server */
server.listen(PORT);
console.log("server is started");
