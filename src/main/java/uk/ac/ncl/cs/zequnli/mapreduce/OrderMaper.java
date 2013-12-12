package uk.ac.ncl.cs.zequnli.mapreduce;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: Li Zequn
 * Date: 11/12/13
 */
public class OrderMaper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser csvParser = new CSVParser();
        String [] data = csvParser.parseLine(value.toString());
        context.write(new Text(data[0]),new DoubleWritable(Double.valueOf(data[3])));
    }
}
