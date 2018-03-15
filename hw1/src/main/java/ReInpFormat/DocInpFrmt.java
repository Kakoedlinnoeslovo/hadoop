package ReInpFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

from java.utils

import java.io.IOException;
import java.util.List;

public class DocInpFrmt extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        DocReader reader = new DocReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {


        List<InputSplit> splits = ArrayList();


        return super.getSplits(job);
    }
}

