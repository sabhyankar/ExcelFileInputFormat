package com.cloudera.sa;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by sabhyankar on 3/15/17.
 */
public class TextArrayWritable extends ArrayWritable {

  public TextArrayWritable() {
    super(Text.class);
  }
  public TextArrayWritable(Text[] texts) {
    super(Text.class, texts);
  }
}
