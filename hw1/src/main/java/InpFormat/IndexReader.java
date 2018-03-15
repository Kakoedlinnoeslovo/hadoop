package InpFormat;

import com.google.common.io.LittleEndianDataInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



public class IndexReader {

    public  List<Integer> ReadIndex(FSDataInputStream indexFile) throws IOException {
        int max_doc = 0;
        LittleEndianDataInputStream in = new LittleEndianDataInputStream(indexFile);
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


}
