package uk.ac.ncl.cs.zequnli.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: Li Zequn
 * Date: 11/12/13
 */
public class OrderReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double total = 0.0;
        int count = 0;
        for(DoubleWritable d:values){
            total+=Double.valueOf(d.toString());
            count++;
        }
        double average = total/count;
        context.write(key,new DoubleWritable(average));
    }
}
