package ReInpFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;

public class DocReader extends RecordReader<LongWritable, Text> {
    @Override
    public void initialize (InputSplit split, TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() {
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return null;
    }

    @Override
    public Text getCurrentValue() {
        return null;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void close() {

    }
}


