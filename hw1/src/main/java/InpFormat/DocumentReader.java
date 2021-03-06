package InpFormat;

import com.google.common.io.LittleEndianDataInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DocumentReader  extends RecordReader<LongWritable, Text> {
    FSDataInputStream input_file;
    Text value;
    List<Integer> indexArray;
    int pos;
    long end;
    long start_file;
    long maxDocLength = 5000000;
    byte[] inputArray = new byte[(int) maxDocLength];
    byte[] result = new byte[(int) maxDocLength ];
    static long maxBufferSize = 150000*10;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        context.getConfiguration();
        FileSplit fsplit = (FileSplit) split;
        Path path = fsplit.getPath();

        String index_file = path.getName();
        index_file = index_file + ".idx";

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FSDataInputStream input_index = fs.open(new Path(path.getParent(), index_file));

        IndexReader reader = new IndexReader();

        indexArray = reader.ReadIndex(input_index);
        start_file = fsplit.getStart();

        long offset = 0;
        while (pos < start_file) {
            offset += indexArray.get(pos);
            pos++;
        }
        end = fsplit.getLength();


        input_file = fs.open(path);
        input_file.seek(offset);


    }


    @Override
    public boolean nextKeyValue() throws IOException {
        if (pos >= end)
            return false;
        try {

            input_file.readFully(inputArray, 0, indexArray.get(pos));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Inflater decompresser = new Inflater();
        decompresser.setInput(inputArray, 0, indexArray.get(pos));
        int res_len = 0;
        try {
            if ((res_len = decompresser.inflate(result)) > maxBufferSize)
                System.out.println("decompress error");
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
        decompresser.end();
        value = new Text (new String(result, 0, res_len, "UTF-8"));
        pos++;
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {

        return new LongWritable(indexArray.get(pos-1));
    }

    @Override
    public Text getCurrentValue() throws IOException {
        return value;
    }

    @Override
    public float getProgress() {

        return (float) pos / end;
    }

    @Override
    public void close() {
        IOUtils.closeStream(input_file);
    }

}
