//###################################################################
//For COMP9313 Assignment 1
//Hao Chen z5102446
//2018. Mar
//###################################################################

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EdgeAvgLen1 
{
    /* The mapper function for EdgeAvgLen1, and it mainly realize the function to pass 
     * the key-value pair(key, Pair(weight, 1)) to the combiner later on.
     * In this case key is the inNode, (e.g. 1), and 1 stands for the count number.
     * 
     * Here pair(weight, 1) stands for that this node receive the weight from this line with 1 time.
     * */
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Pair> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {   
            //split the term and use the list to store it
            String[] termList = value.toString().split(" ");

            //construct the pair and send it to the combiner 
            Pair new_p = new Pair();
            new_p.set(Double.parseDouble(termList[3]), 1);
            IntWritable k1 = new IntWritable();
            k1.set(Integer.parseInt(termList[2]));
            //write the key-value pair to the context
            context.write(k1, new_p);
        }
    }
        
        
    /* This is the combiner function to get the key-value pair, and get the sum and count times for each pair
     * Then we construct the new Pair and pass it to the reducer later.
     * 
     * Here the key-value pair is (key, Pair(sum, time))
     * key is also the inNode, and sum stands for the 
     * */
    public static class Combiner extends Reducer<IntWritable, Pair, IntWritable, Pair>
    {
        public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
        {
            double sum = 0.0;
            int time = 0;
            //for every token, we add it's sum and the time(in line)
            while(values.iterator().hasNext())
            {
                Pair p = values.iterator().next();
                sum = sum + p.getSum();
                time = time + p.getTimes();
            }
            //pass this to the reducer, by key-value pair of (key, pair)
            context.write(key, new Pair(sum, time));
        }   
    }


    /* This is the reducer function to get the key-value pair, and get the sum and count times for each pair
     * Then we compute the mean value by sum and count.
     * 
     * The key-value pair we receive here can show the sum and count time(lines this node receive).
     * */
    public static class MeanReducer extends Reducer<IntWritable, Pair, IntWritable, DoubleWritable> 
    {
        public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException 
        {
            //Initialize the sum and count for computing
            Double sum = 0.0;
            int count = 0;
            //for every Pair in the sent values, add the sum and compute the mean number
            for (Pair val : values) 
            {
                sum += val.getSum();
                count += val.getTimes();
            }
            DoubleWritable mean = new DoubleWritable(sum/count);
            //write into the context
            context.write(key, mean);
        }
    }
    
    //The class for Pair, given by teacher from the lecture
    public static class Pair implements Writable
    {
        private double sum;
        private int times;
            public Pair()
            {}
            public Pair(double sum,int times)
            {set(sum, times);}
            public void set(double left,int right)
            {
                sum =left;
                times = right;
            }
            public double getSum()
            {return sum;}
            public int getTimes()
            {return times;}
            public void write(DataOutput out)throws IOException
            {
                out.writeDouble(sum);
                out.writeInt(times);
            }
            public void readFields(DataInput in) throws IOException
            {
                sum = in.readDouble();
                times = in.readInt();
            }
        }
    
    //main function
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EdgeAvgLen1");
        job.setJarByClass(EdgeAvgLen2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(MeanReducer.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Pair.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}