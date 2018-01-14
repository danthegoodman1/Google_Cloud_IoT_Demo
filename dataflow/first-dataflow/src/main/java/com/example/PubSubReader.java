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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class PubSubReader {


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
	
	
  public static void main(String[] args) {
	  PubSubOptions options = PipelineOptionsFactory.fromArgs(args)
			  										  .withValidation()
			  										  .as(PubSubOptions.class);
	  Pipeline p = Pipeline.create(options);

	 p.apply(PubsubIO.readStrings().fromTopic(options.getPubSubTopic()))
	  .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
	  .apply(TextIO.write().to(options.getOutput()).withWindowedWrites().withNumShards(1));

	
	  p.run();
  }
}
