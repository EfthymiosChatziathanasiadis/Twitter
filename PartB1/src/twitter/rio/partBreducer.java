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
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;


public class partBreducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
  private Text key;
  private final IntWritable val = new IntWritable(1);

    public void reduce(IntWritable k, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable i : values)sum=sum+i.get();
        val.set(sum);
        key = new Text(k.get()+"");
        context.write(key, val);
    }
}
