import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;

public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable(Writable[] values) {
        super(IntWritable.class, values);
    }

    public IntWritable[] get(){
        return get();
    }

    @Override
    public void write(DataOutput out) throws IOException { //todo check
//        super.write(out);
        IntWritable[] arr = get();
        String strOut = "";
        for(IntWritable i : arr){
            strOut += i.get() + " ";
        }
        out.write(strOut.getBytes());
    }


}
