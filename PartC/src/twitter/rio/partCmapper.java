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


 import org.apache.hadoop.fs.FSDataInputStream;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;
 import java.util.StringTokenizer;
 import java.util.*;



public class partCmapper extends Mapper<Object, Text, Text, IntWritable> {
  private  Text key ;
  private final IntWritable val = new IntWritable(1);
  private Hashtable<String, String> athleteSports;
  public void map(Object ob, Text value, Context context) throws IOException, InterruptedException {
    String [] fields =value.toString().split(";");
    if(fields.length == 4){
      String tweet = fields[2];
      singleName(context, tweet);
      fullName(context, tweet);
      fullName_withMiddle(context, tweet);
    }
  }

  private void singleName(Context context, String tweet)throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(tweet.toString(), "-- \t\n\r\f,.:;?![]'\"");
    while(itr.hasMoreTokens()){
      String current = itr.nextToken();
      if(athleteSports.containsKey(current)){
        key = new Text(current);
        context.write(key, val);
      }
    }
  }
  private void fullName(Context context, String tweet)throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(tweet.toString(), "-- \t\n\r\f,.:;?![]'\"");
    String previous = null;
    while(itr.hasMoreTokens()){
      String current = itr.nextToken();
      if(previous != null){
        String fullName = previous+" "+current;
        if(athleteSports.containsKey(fullName)){
          key = new Text(fullName);
          context.write(key, val);
        }
      }
      previous = current;
    }
  }
  private void fullName_withMiddle( Context context, String tweet)throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(tweet.toString(), "-- \t\n\r\f,.:;?![]'\"");
      String first = null;
      String middle = null;
      if(itr.hasMoreTokens()){
        first = itr.nextToken();
        if(itr.hasMoreTokens()){
          middle = itr.nextToken();
        }
      }
      if(first != null && middle != null){
        while(itr.hasMoreTokens()){
          String last = itr.nextToken();
          String fullName_middle = first + " " + middle + " " + last;
          if(athleteSports.containsKey(fullName_middle)){
            key = new Text(fullName_middle);
            context.write(key, val);
          }
          first = middle;
          middle = last;
        }
       }

  }
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    athleteSports = new Hashtable<String, String>();
    URI fileUri = context.getCacheFiles()[0];
    FileSystem fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream in = fs.open(new Path(fileUri));
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line = null;
    try {
          br.readLine();
          while ((line = br.readLine()) != null) {
            String[] fields = line.toString().split(",");
            if (fields.length == 11)athleteSports.put(fields[1], fields[7]);
          }
          br.close();
        } catch (IOException e1) {}
    super.setup(context);
  }
}
