public class EdgeAvgLen2 
{
    /* The mapper function for EdgeAvgLen2, and it mainly realize the function to pass 
     * the key-value pair(key, Pair(weight, count)) to the reducer later on.
     * In this case key is the inNode, (e.g. 1), and 1 stands for the count number.
     * 
     * Here pair(weight, count) mens that there are "count" lines in and their sum is weight.
     * */
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Pair> 
    {
        private IntWritable count = new IntWritable(1);
        private static HashMap<Integer, Pair> map = new HashMap<Integer, Pair>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {   
            //split the term and use the list to store it
            String[] termList = value.toString().split(" ");
            //if the map contains the key 
            if(map.containsKey(Integer.parseInt(termList[2])))
            {
                //get the sum value from the map if this key already exists
                double s1 = map.get(Integer.parseInt(termList[2])).getSum();
                double s2 = Double.parseDouble(termList[3]);
                //get the times value from the map if this key already exists
                int t = map.get(Integer.parseInt(termList[2])).getTimes();
                //construct the pair to store the value by (sum, times)
                Pair new_p = new Pair();
                new_p.set(s1+s2, t+1);
                //put the new generated pair back into the map 
                map.put(Integer.parseInt(termList[2]), new_p);
            }
            else
            {
                //construct the pair and put into the map 
                Pair new_p = new Pair();
                new_p.set(Double.parseDouble(termList[3]), 1);
                map.put(Integer.parseInt(termList[2]), new_p);
            }
        }
        
        
        //this is the cleanup function to get the key-value pair, and put the token to into the hash map,
        //which is for pass key-value pairs.
        public void cleanup(Context context) throws IOException, InterruptedException 
        {
            Set<Entry<Integer, Pair>> sets = map.entrySet();
            //for every key-value pair(Integer-Pair) pair, put it into the context and send to the reducer
            for(Entry<Integer, Pair> entry: sets)
            {
                Integer key = entry.getKey();
                Pair Value = entry.getValue();
                count.set(key); 
                context.write(count, Value);
            }
        }
    }
    
    /* This is the reducer function to get the key-value pair, and get the sum and count times for each pair
     * Then we compute the mean value by sum and count.
     * 
     * The key-value pair we receive here can show the sum and count time(lines this node receive).
     * */
    public static class MeanReducer extends Reducer<IntWritable, Pair, IntWritable, DoubleWritable> 
    {
        public void reduce(IntWritable key, Iterable<Pair> values, Context context)throws IOException, InterruptedException 
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
        Job job = Job.getInstance(conf, "EdgeAvgLen2");
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
