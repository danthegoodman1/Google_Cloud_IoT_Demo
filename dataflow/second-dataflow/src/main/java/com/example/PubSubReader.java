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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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
	
	
	static class ComputeWordLengthFn extends DoFn<String, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			// Get the input element from ProcessContext.
		    TableRow row;
//		    row.
		    
		    //= c.element();
		    // Use ProcessContext.output to emit the output element.
//		    c.output(row.length());
		 }
	}
	
	// A DoFn that converts a logging-message into a BigQuery table row:
	@SuppressWarnings("serial")
	static class FormatAsTableRowFn extends DoFn<String, TableRow> {
    
		@ProcessElement
		public void processElement(ProcessContext c) {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonMessage = null;
			TableRow row = null;

			try {
				LOG.info(String.format("Message as read from PubSub (%s) ...", c.element()));
				
				// Parse the context as a JSON object:
				jsonMessage = (JSONObject) jsonParser.parse(c.element());	
				Number hum = (Number)jsonMessage.get("hum");
				Number temp = (Number)jsonMessage.get("temp");

				// Make a BigQuery row from the JSON object:
				row = new TableRow()
//						.set("timestamp",)
						.set("timestamp", c.timestamp().getMillis()/1000 )
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
		
		p.apply(PubsubIO.readStrings().fromTopic(options.getPubSubTopic()))
		 .apply(ParDo.of(new FormatAsTableRowFn()))
		 .apply(BigQueryIO.writeTableRows().to(tableSpec.toString())
		          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
		          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));   		
	
		p.run();
	}
}
