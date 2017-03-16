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
 * Created by sabhyankar on 3/15/17.
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
