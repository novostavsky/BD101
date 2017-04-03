package com.epam.cdp;

/**
 * Used tutorial from
 * http://hadoop.apache.org/docs/r3.0.0-alpha2/hadoop-mapreduce-client/
 *                             hadoop-mapreduce-client-core/MapReduceTutorial.html
 **/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LongWord {
//    private static IntWritable maxLength = new IntWritable(0);
//    private static Text longestWord = new Text ("");

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            IntWritable maxLength = new IntWritable(0);
            Text longestWord = new Text ("");

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(word.getLength() > maxLength.get()){
                    maxLength.set(word.getLength());
                    longestWord.set(word.toString());
                }
            }
            context.write(one, longestWord);//single key to get one reducer
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, Text,IntWritable, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            IntWritable maxLength = new IntWritable(0);
            Text longestWord = new Text ("");

            while(values.iterator().hasNext()){
                Text word = values.iterator().next();
                if(word.getLength() > maxLength.get()){
                    maxLength.set(word.getLength());
                    longestWord = word;
                }
            }
            context.write(maxLength, longestWord);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //Job job = Job.getInstance(conf, "word count"); //doesn't work, used constructor
        Job job = new Job(conf);
        job.setJarByClass(LongWord.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);


        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}
