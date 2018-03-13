package inputformat;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DocumentInpFormat2 extends FileInputFormat<LongWritable, Text> {


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException{
        System.out.println("Here RecordReader I AM HERE!!!");
        DocReader2 reader = new DocReader2();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        System.out.println("Here getSplits I AM HERE!!!");
        for (FileStatus status : listStatus(context)) {
            Path path = status.getPath();
            splits.add(new FileSplit(path, 0, 0, null));
        }

        //splits.add(new FileSplit(new Path(1), 0, 0, null));
        return splits;
    };
}
