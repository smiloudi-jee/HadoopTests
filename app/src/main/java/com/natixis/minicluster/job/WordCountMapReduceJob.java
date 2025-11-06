package com.natixis.minicluster.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Job MapReduce type "wordcount".
 */
public final class WordCountMapReduceJob {

    private WordCountMapReduceJob() {}

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private final Text WORD = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                WORD.set(itr.nextToken());
                context.write(WORD, ONE);
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key,
                              Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * Lance le job MR sur un cluster MapReduce déjà démarré.
     */
    public static boolean run(Configuration conf,
                              String inputPath,
                              String outputPath) throws Exception {

        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCountMapReduceJob.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inputPath));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }
}
