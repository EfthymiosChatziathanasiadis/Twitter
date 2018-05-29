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

public class partBmapper extends Mapper<Object, Text, IntWritable, IntWritable> {
 private final IntWritable key = new IntWritable(1);
 private final IntWritable val = new IntWritable(1);

 public void map(Object ob, Text value, Context context) throws IOException, InterruptedException {
     String [] fields = value.toString().split(";");
     if(fields.length == 4){
          try{
                LocalDateTime dateTime = LocalDateTime.ofEpochSecond(Long.parseLong(fields[0])/1000,0, ZoneOffset.of("-02:00"));
                int hour = dateTime.getHour();
                key.set(hour);
                context.write(key, val);
          }catch(NumberFormatException e){}
      }
    }
}
