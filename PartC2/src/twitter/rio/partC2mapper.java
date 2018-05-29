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
 import java.io.BufferedReader;
 import java.io.IOException;
 import java.io.InputStreamReader;
 import java.net.URI;
 import java.util.Calendar;
 import java.util.Hashtable;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.NullWritable;


 import org.apache.hadoop.fs.FSDataInputStream;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;
 import java.util.StringTokenizer;



public class partC2mapper extends Mapper<Object, Text, NullWritable, TextIntPair> {
  private  Text key = new Text();
  private  IntWritable val = new IntWritable(1);
  public void map(Object ob, Text value, Context context) throws IOException, InterruptedException {
   String fields [] = value.toString().split("\t");
   TextIntPair pair = new TextIntPair();
   pair.set(fields[0], Integer.parseInt(fields[1]));
   context.write(NullWritable.get(), pair);
  }
}
