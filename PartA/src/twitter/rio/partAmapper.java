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

public class partAmapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable key = new IntWritable(1);
    private final IntWritable val = new IntWritable(1);

    public void map(Object ob, Text value, Context context) throws IOException, InterruptedException {
              String [] fields =value.toString().split(";");
              if(fields.length == 4){
                  if(fields[2].length() <= 140){
                      int length = fields[2].length();
                      float category = (float) length/5;
                      int cat = (int) Math.ceil(category);
                      key.set(cat);
                      context.write(key, val);
                  }
              }
    }
}
