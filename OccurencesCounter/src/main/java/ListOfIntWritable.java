import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ListOfIntWritable implements Writable {
    List<IntWritable> lst = new ArrayList<IntWritable>();
    public ListOfIntWritable(List<IntWritable> lst){
        this.lst = lst;
    }
    public void write(DataOutput dataOutput) throws IOException {
        for(IntWritable i : lst){
            dataOutput.write(i.get());
        }
    }

    public void readFields(DataInput dataInput) throws IOException {

    }


}
