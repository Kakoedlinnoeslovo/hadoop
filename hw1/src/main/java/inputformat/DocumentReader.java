//package inputformat;
//
//
//import com.google.common.io.LittleEndianDataInputStream;
////import org.apache.commons.configuration.Configuration;
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IOUtils;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//
//import java.io.IOException;
//import java.util.List;
//
//
//public class DocumentReader extends RecordReader<LongWritable, Text> {
//
//
//    private static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";
//    FSDataInputStream input_file;
//    Text value;
//    List<Integer> index_array;
//    byte[] input_arr;
//    byte[] result;
//    int doc_num;
//    long n_files;
//    long start_file;
//    long NumBytesPerSplit;
//    boolean isInit;
//    long whereAmI;
//
//
//    @Override
//    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException{
//        Configuration conf = context.getConfiguration();
//        FileSplit fsplit = (FileSplit) split;
//        Path path = fsplit.getPath();
//
//        String index_file = path.getName();
//        index_file = index_file + ".idx";
//
//        FileSystem fs = path.getFileSystem(context.getConfiguration());
//        FSDataInputStream inputIndex = fs.open(new Path(path.getParent(), index_file));
//        System.out.println(inputIndex);
//        NumBytesPerSplit = conf.getLong(BYTES_PER_MAP, 134217728);
//        isInit = true;
//
//        //prepare_index(fsplit, path, fs, input_index);
//    }
//
//    public long getNumBytesPerSplit(){
//        if (isInit) {
//            return NumBytesPerSplit;
//        }
//        else{
//            System.out.println("First you need init");
//            return 0;
//        }
//
//    }
//
//
//    @Override
//    public boolean nextKeyValue() throws IOException {
//        return true;
//    }
//
//    @Override
//    public LongWritable getCurrentKey() {
//        return new LongWritable(index_array.get(doc_num - 1));
//    }
//
//    @Override
//    public Text getCurrentValue() throws IOException {
////            System.out.println(value);
////            throw new IOException(value.toString());
//        return value;
//    }
//
//    @Override
//    public float getProgress() {
//        return (float) (doc_num ) / n_files;
//    }
//
//    @Override
//    public void close() {
//        IOUtils.closeStream(input_file);
//    }
//}