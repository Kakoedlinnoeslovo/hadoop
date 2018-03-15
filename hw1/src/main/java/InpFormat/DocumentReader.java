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
    List<Integer> index_array;
    byte[] input_arr;
    byte[] result;
    int doc_num;
    long n_files;
    long start_file;
    long max_doc = 50000;


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        context.getConfiguration();
        FileSplit fsplit = (FileSplit) split;
        Path path = fsplit.getPath();

        String index_file = path.getName();
        index_file = index_file + ".idx";

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FSDataInputStream input_index = fs.open(new Path(path.getParent(), index_file));

        prepare_index(fsplit, path, fs, input_index);

    }

    private void prepare_index(FileSplit fsplit, Path path, FileSystem fs, FSDataInputStream input_index) throws IOException {
        index_array = read_index(input_index);
        start_file = fsplit.getStart();

        long offset = 0;
        while (doc_num < start_file) {
            offset += index_array.get(doc_num);
            doc_num++;
        }
        n_files = fsplit.getLength();

        if (max_doc < 0)
            throw new IOException("max doc error");

        input_file = fs.open(path);
        input_file.seek(offset);

        input_arr = new byte[(int) max_doc];
        result = new byte[(int) max_doc * 20];//?
    }

    private static List<Integer> read_index(FSDataInputStream index_file) throws IOException {
        int max_doc = 0;
        LittleEndianDataInputStream in = new LittleEndianDataInputStream(index_file);
        List<Integer> al = new ArrayList<>();
        try {
            while (true){
                int val = in.readInt();
                if (val > max_doc)
                    max_doc = val;
                al.add(val);
            }
        } catch (EOFException ignored) {
        }
        return al;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (doc_num >= n_files)
            return false;

//            System.out.println("Doc num:" + doc_num + ", N files:" + n_files);
        try {

            input_file.readFully(input_arr, 0, index_array.get(doc_num));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Inflater decompresser = new Inflater();
        decompresser.setInput(input_arr, 0, index_array.get(doc_num));
        int res_len = 0;
        try {
            if ((res_len = decompresser.inflate(result)) > 150000 * 5)
                System.out.println("decompress error");
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
        decompresser.end();
        value = new Text (new String(result, 0, res_len, "UTF-8"));
        doc_num++;
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return new LongWritable(index_array.get(doc_num - 1));
    }

    @Override
    public Text getCurrentValue() throws IOException {
//            System.out.println(value);
//            throw new IOException(value.toString());
        return value;
    }

    @Override
    public float getProgress() {
        return (float) (doc_num ) / n_files;
    }

    @Override
    public void close() {
        IOUtils.closeStream(input_file);
    }

}
