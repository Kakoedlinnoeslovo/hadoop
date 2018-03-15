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

public class DocumentInpFormat extends FileInputFormat<LongWritable, Text> {
    private long max_doc = 5000000;

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        DocumentReader reader = new DocumentReader();
        reader.initialize(split, context);
//        System.out.println("Here?");
        return reader;
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
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();

        for (FileStatus status : listStatus(context)) {
            Path path = status.getPath();
            String index_file = path.getName();
            if (index_file.substring(index_file.length() - 4).equals(".idx")) {
                continue;
            } else {
                index_file = index_file + ".idx";
            }
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            FSDataInputStream input_index = fs.open(new Path(path.getParent(), index_file));
            List<Integer> al = read_index(input_index);

            int cur_split = 0;
            long split_size = 0;
            long offset = 0;
            for (Integer cur : al) {
                split_size += cur;
                if (cur > max_doc)
                    max_doc = cur;
                cur_split++;
                long bytes_num_for_split = getNumBytesPerSplit (context.getConfiguration());
                if (split_size > bytes_num_for_split) {
                    splits.add(new FileSplit(path, offset, cur_split, null));
                    offset += cur_split;
                    split_size = 0;
                    cur_split = 0;
                }
            }
            splits.add(new FileSplit(path, offset, cur_split, null));
        }
        return splits;
    }
    public static final String BYTES_PER_MAP = "mapreduce.input.doc.bytes_per_map";

    public static long getNumBytesPerSplit(Configuration conf) {
        return  conf.getLong(BYTES_PER_MAP, 134217728);
    }


}