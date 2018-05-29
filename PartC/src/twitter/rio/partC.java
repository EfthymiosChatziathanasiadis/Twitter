package twitter.rio;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;


public class partC {
    public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance();

        job.setJarByClass(partC.class);

        job.setReducerClass(partCreducer.class);
        job.setMapperClass(partCmapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path("/data/medalistsrio.csv").toUri());

        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }
    public static void runJob2(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance();

        job.setJarByClass(partC.class);

        job.setReducerClass(partC2reducer.class);
        job.setMapperClass(partC2mapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(TextIntPair.class);
        job.addCacheFile(new Path("/data/medalistsrio.csv").toUri());

        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
        String inputJob1 = args[args.length - 1];
        String inputJob2 [] = {inputJob1};
        runJob2(inputJob2, "Top30Medalists");
    }
}
