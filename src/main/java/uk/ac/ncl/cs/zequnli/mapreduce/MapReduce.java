package uk.ac.ncl.cs.zequnli.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * @Auther: Li Zequn
 * Date: 11/12/13
 */
public class MapReduce {
    public static void main(String []args) throws Exception{
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("error need 2 args");
            System.exit(2);
        }
        Job job = new Job(conf,"Average Cost");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(OrderMaper.class);
        job.setCombinerClass(OrderReducer.class);
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
