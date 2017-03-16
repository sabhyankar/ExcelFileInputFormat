package com.cloudera.sa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by sabhyankar on 3/15/17.
 */
public class ExcelRecordReader
    extends RecordReader<NullWritable, TextArrayWritable> {



  private TextArrayWritable value = new TextArrayWritable();
  private FSDataInputStream in;
  private Iterator<Row> rowIterator;
  private int totalRows;
  private float processedRows;


  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    Configuration conf = context.getConfiguration();
    Path file = fileSplit.getPath();
    FileSystem fs = file.getFileSystem(conf);
    this.in = fs.open(file);
    XSSFWorkbook workbook = new XSSFWorkbook(this.in);
    XSSFSheet sheet = workbook.getSheetAt(0);
    this.totalRows = sheet.getPhysicalNumberOfRows();
    this.processedRows = 0;
    this.rowIterator = sheet.rowIterator();
  }


  @Override
  public boolean nextKeyValue()
      throws IOException, InterruptedException {

    Boolean ret = false;
    if (rowIterator.hasNext()) {
      ret = true;
      Row row = rowIterator.next();
      Iterator<Cell> cellIterator = row.cellIterator();
      ArrayList<Text> textArrayList = new ArrayList<Text>();
      while(cellIterator.hasNext()) {
        Cell cell = cellIterator.next();
        textArrayList.add(getCellValue(cell));
      }
      Text[] texts = new Text[textArrayList.size()];
      texts = textArrayList.toArray(texts);
      value.set(texts);
      processedRows++;
    }
    return ret;
  }

  private Text getCellValue(Cell cell) {
    Text out = new Text();
     CellType cellType =  cell.getCellTypeEnum();

    if (cellType == CellType.STRING) {
      out.set(cell.getStringCellValue());
    } else if (cellType == CellType.NUMERIC) {
      out.set(String.valueOf(cell.getNumericCellValue()));
    } else if (cellType == CellType.FORMULA) {
      out.set(cell.getCellFormula());
    } else if (cellType == CellType.ERROR) {
      out.set(String.valueOf(cell.getErrorCellValue()));
    } else if (cellType == CellType.BOOLEAN) {
      out.set(String.valueOf(cell.getBooleanCellValue()));
    } else {
      out.set("");
    }

    return out;
  }

  @Override
  public NullWritable getCurrentKey()
      throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public TextArrayWritable getCurrentValue()
      throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress()
      throws IOException, InterruptedException {
    return processedRows/totalRows;
  }

  @Override
  public void close()
      throws IOException {
      IOUtils.closeStream(in);
  }
}
