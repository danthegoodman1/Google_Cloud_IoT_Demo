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

import com.example.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class MinimalWordCount {

  public static void main(String[] args) {
     PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);

     p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))

     .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                       @ProcessElement
                       public void processElement(ProcessContext c) {
                         for (String word : c.element().split(ExampleUtils.TOKENIZER_PATTERN)) {
                           if (!word.isEmpty()) {
                             c.output(word);
                           }
                         }
                       }
                     }))

     .apply(Count.<String>perElement())

     .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                       @Override
                       public String apply(KV<String, Long> input) {
                         return input.getKey() + ": " + input.getValue();
                       }
                     }))

     .apply(TextIO.write().to("wordcounts"));

    // Run the pipeline.
    p.run().waitUntilFinish();
  }
}
