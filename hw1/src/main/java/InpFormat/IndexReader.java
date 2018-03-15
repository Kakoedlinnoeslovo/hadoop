package InpFormat;

import com.google.common.io.LittleEndianDataInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;



public class IndexReader {

    public List<Integer> ReadIndex(FSDataInputStream indexFile, long size) throws IOException {
        LittleEndianDataInputStream in = new LittleEndianDataInputStream(indexFile);
        List<Integer> al = new ArrayList<>((int)size);

        for (int i = 0; i < size; i++) {
            al.add(indexFile.readInt());
        }
        return al;
    }


}
