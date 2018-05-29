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



public class partCmapper extends Mapper<Object, Text, Text, IntWritable> {
  private  Text key ;
  private final IntWritable val = new IntWritable(1);
  private Hashtable<String, String> athleteSports;
  public void map(Object ob, Text value, Context context) throws IOException, InterruptedException {
   String [] fields =value.toString().split("\t");
   if(fields.length == 2){
    if(athleteSports.containsKey(fields[0])){
     key = new Text(athleteSports.get(fields[0]));
     val.set(Integer.parseInt(fields[1]));
     context.write(key, val);
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
