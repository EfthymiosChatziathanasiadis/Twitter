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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;


public class partB2reducer extends Reducer<NullWritable, TextIntPair, NullWritable, Text> {
  private Text key ;
  public void reduce(NullWritable k, Iterable<TextIntPair> values, Context context)
      throws IOException, InterruptedException {
      TreeMap<Integer, String> tweetFreqs =new TreeMap<Integer, String>();
      for(TextIntPair i : values){
        String tag = i.getLeft();
        Integer freq = i.getRight();
        tag = tag + " " + freq;
        tweetFreqs.put(freq, tag);
        if(tweetFreqs.size() > 10)tweetFreqs.remove(tweetFreqs.firstKey());
      }
      for(String st : tweetFreqs.descendingMap().values()){
        key = new Text(st);
        context.write(NullWritable.get(), key);
      }
  }
}
