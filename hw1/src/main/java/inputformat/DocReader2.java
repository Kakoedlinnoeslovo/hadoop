package inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

public class DocReader2 extends RecordReader<LongWritable, Text>{

    private FSDataInputStream inputIndex;
    private Text value;
    private long documentNumber;
    private long filesNumber;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSplit fsplit = (FileSplit)split;
        Path path = fsplit.getPath();
        FileSystem fs = path.getFileSystem(conf);
        inputIndex = fs.open(path);
        System.out.println(inputIndex);


    };

    public LongWritable getCurrentKey() throws IOException, InterruptedException{
        return new LongWritable(0);

    };

    public Text getCurrentValue() throws IOException {
//            System.out.println(value);
//            throw new IOException(value.toString());
        return value;
    };

    public float getProgress() throws IOException, InterruptedException{
        return (float) (documentNumber) / filesNumber;
    };



    public boolean nextKeyValue() throws IOException, InterruptedException{

        return true;
    };


    public void close() throws IOException{
        IOUtils.closeStream(inputIndex);

    };
}
