/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sa;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Sameer Abhyankar
 */
public class ExcelFileInputFormat extends FileInputFormat<NullWritable, TextArrayWritable> {


  @Override
  protected boolean isSplitable(JobContext context, Path filename)
  {
    return false;
  }

  @Override
  public RecordReader<NullWritable, TextArrayWritable> createRecordReader(InputSplit split,
                                                                          TaskAttemptContext context)
      throws IOException, InterruptedException {
    ExcelRecordReader reader = new ExcelRecordReader();
    reader.initialize(split, context);
    return reader;
  }
}
