package inputformat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountJob extends Configured implements Tool {
    public class WordCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final LongWritable one = new LongWritable(1);
        private final Pattern wordRegPattern = Pattern.compile("\\p{L}+");
        private HashSet<String> words = new HashSet<String>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            words.clear();

            Matcher matcher = wordRegPattern.matcher(value.toString());

            while (matcher.find()) {
                String word = matcher.group().toLowerCase();
                if (word.length() > 0) {
                    words.add(word);
                }
            }

            for (String word : words) {
                context.write(new Text(word), one);
            }
        }
    }

    public static class WordCounterReducer extends Reducer<Text, LongWritable, Text, IntWritable> {
        protected void reduce(Text word, Iterable<IntWritable> nums, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable ignored : nums) {
                sum += 1;
            }
            // produce pairs of "word" <-> amount
            context.write(word, new IntWritable(sum));
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(WordCountJob.class);
        job.setJobName(WordCountJob.class.getCanonicalName());

        job.setInputFormatClass(DocumentInpFormat2.class);
        DocumentInpFormat2.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(WordCounterMapper.class);
        job.setReducerClass(WordCounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WordCountJob(), args);
        System.exit(ret);
    }
}
