/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
 package twitter.rio;

/**
 *
 * @author ec305
 */
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.text.*;
import java.lang.Object;


import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.time.ZoneOffset;
import java.time.OffsetDateTime ;

public class partB2mapper extends Mapper<Object, Text, NullWritable, TextIntPair> {
  private  Text key = new Text();
  TextIntPair pair = new TextIntPair();
  public void map(Object ob, Text value, Context context) throws IOException, InterruptedException {
    String fields [] =value.toString().split("\t");
    pair.set(fields[0], Integer.parseInt(fields[1]));
    context.write(NullWritable.get(), pair);
  }
}
