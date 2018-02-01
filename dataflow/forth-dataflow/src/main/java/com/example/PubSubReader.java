/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class PubSubReader {

	private static final Logger LOG = LoggerFactory.getLogger(PubSubReader.class);
	 

	public interface PubSubOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
		String getInputFile();

		void setInputFile(String value);

		@Description("PubSub Topic to read data from")
		@Default.String("projects/iot-demo-psteiner-2018/topics/iot-topic")
		String getPubSubTopic();
		void setPubSubTopic(String pubsubTopic);
		
		@Description("Path of the file to write to")
		@Required
		String getOutput();
		void setOutput(String value);
	}	
	
	@SuppressWarnings("serial")
	static class FormatMessageAsKV extends DoFn<PubsubMessage, KV<String, String>> {
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			
			String key   = c.element().getAttribute("deviceId");
			String value = new String( c.element().getPayload() );
			
			c.output(KV.of(key, value));
		}
		
	}
    

	@SuppressWarnings("serial")
	static class LogKVMessage extends DoFn<KV<String, Double>, KV<String, Double>> {
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			
			LOG.info(String.format("Message with key (%s) has content (%f) ...", c.element().getKey(), c.element().getValue()));
			
			c.output(KV.of(c.element().getKey(), c.element().getValue()));
		}
		
	}
    
	
	@SuppressWarnings("serial")
	static class FormatKVAsTableRowFn extends DoFn<KV<String,String>, TableRow> {
    
		@ProcessElement
		public void processElement(ProcessContext c) {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonMessage = null;
			TableRow row = null;
			
			try {
				
				// Parse the context as a JSON object:
				jsonMessage = (JSONObject) jsonParser.parse( new String( c.element().getValue() ) );	
				Number hum = (Number)jsonMessage.get("hum");
				Number temp = (Number)jsonMessage.get("temp");
				String deviceID = c.element().getKey();

				// Make a BigQuery row from the JSON object:
				row = new TableRow()
						.set("timestamp", c.timestamp().getMillis()/1000 )
						.set("deviceID", deviceID)
						.set("humidity", hum.doubleValue() )
						.set("temp", temp.doubleValue() );
				
				LOG.info(String.format("Message (%s) ...", row.toString()));

			} catch (ParseException e) {
				LOG.warn(String.format("Exception encountered parsing JSON (%s) ...", e));

			} catch (Exception e) {
				LOG.warn(String.format("Exception: %s", e));
			} finally {
				// Output the row:
				c.output(row);
			}
		}
	}
	
	public static void main(String[] args) {
		PipelineOptionsFactory.register(PubSubOptions.class);
		PubSubOptions options = PipelineOptionsFactory.fromArgs(args)
			  										  .withValidation()
			  										  .as(PubSubOptions.class);
		Pipeline p = Pipeline.create(options);
	  
		String tableSpec = new StringBuilder()
		        .append("iot-demo-psteiner-2018:")
		        .append("iot_data.")
		        .append("raw_data")
		        .toString();		
			
		PCollection<PubsubMessage> items = p.apply(PubsubIO.readMessagesWithAttributes().fromTopic(options.getPubSubTopic()));
		
		PCollection<KV<String,String>> sliding_windowed_items = items
				.apply(Window.<PubsubMessage>into(SlidingWindows.of(Duration.standardMinutes(5)).every(Duration.standardSeconds(5))))
				.apply(ParDo.of(new FormatMessageAsKV()));
		
		final TupleTag<KV<String, Double>> temperatureTag = new TupleTag<KV<String, Double>>(){};
		final TupleTag<KV<String, Double>> humidityTag = new TupleTag<KV<String, Double>>(){};
		
		PCollectionTuple results =
				sliding_windowed_items.apply(ParDo
			          .of(new DoFn<KV<String, String>, KV<String, Double>>() {
			        	  
			        	@SuppressWarnings("unused")
			        	@ProcessElement
						public void processElement(ProcessContext c) {
			        		  String key   = c.element().getKey();
			        	  
			        		  JSONParser jsonParser = new JSONParser();
			      			  JSONObject jsonMessage = null;
			      			  
			      			  Number hum=0;
			      			  Number temp=0;
			      			  
			      			  try {
			      				  // Parse the context as a JSON object:
			      				  jsonMessage = (JSONObject) jsonParser.parse( new String( c.element().getValue() ) );	
			      				  hum = (Number)jsonMessage.get("hum");
			      				  temp = (Number)jsonMessage.get("temp");

			      			  } catch (ParseException e) {
			      				  LOG.warn(String.format("Exception encountered parsing JSON (%s) ...", e));
			      			  } catch (Exception e) {
			      				  LOG.warn(String.format("Exception: %s", e));
			      			  } finally {
			      				  // Output to PCollections:
			    				c.output(KV.of(key, temp.doubleValue()));
			    				c.output(humidityTag, KV.of(key, hum.doubleValue()));
			    			}
			        	  } 
			          })
			          .withOutputTags(temperatureTag,    // Specify the tag for the main output.
			                          TupleTagList.of(humidityTag))); // Specify the tags for the two additional outputs as a TupleTagList.
			                                      
		PCollection tempPCollection = results.get(temperatureTag)
				.apply(ParDo.of(new LogKVMessage()));
		
		PCollection humPCollection = results.get(humidityTag)
				.apply(ParDo.of(new LogKVMessage()));
		
		
		
//		 		.apply(ParDo.of(new FormatKVAsTableRowFn()))
//		 		.apply(BigQueryIO.writeTableRows().to(tableSpec.toString())
//		          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//		          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));   		
		
//		p.apply(PubsubIO.readMessagesWithAttributes().fromTopic(options.getPubSubTopic()))
//		 .apply(ParDo.of(new FormatMessageAsKV()))
//		 .apply(ParDo.of(new FormatKVAsTableRowFn()))
//		 .apply(BigQueryIO.writeTableRows().to(tableSpec.toString())
//		          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//		          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));   		
		
		p.run();
	}
}
