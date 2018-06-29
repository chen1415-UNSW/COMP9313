package comp9313.ass2;

import java.io.IOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*	Project 2 for COMP9313
 * 	2018 S1
 *  Hao Chen
 *  z5102446
 *  
 *  Main Idea is to use Three Map/Reduce to compute the final result.
 *  1. First Map/Reduce is to set all node information to a table, which contains all nodes and it's neighbors' information.  
 *  2. Second Map/Reduce is to find the shortest path, this one follows the lecture teacher given.
 *  3. Third Map/Reduce is to output the result as required.
 *  
 * */

public class SingleTargetSP 
{
    public static String OUT = "output";
    public static String IN = "input";
    public static int queryNodeID;
	
	// create a counter.
	public static enum eInf 
	{
		COUNTER; // used to record the update time
	};
    
    //First Mapper we use.
	public static class PreMapper extends Mapper<Object, Text, IntWritable, Text>
	{	
		//create the HashTable to store all nodes in case of some nodes does not in List of toNode. 
		private Map<String, String> HMap = new HashMap<>();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException
		{
			String targetID = String.valueOf(context.getConfiguration().getInt("queryNodeID", 0));
			
			String[] sp = value.toString().split(" ");			
			
			//sp should be like 0 0 1 10.0
			//So sp[2] is toNdoe, if it does not in HMap, add it's key and value
			if(!HMap.containsKey(sp[2]))
			{
				HMap.put(sp[2], sp[1]+":"+ sp[3]);
			}
			//if HMap already has sp[2] as key, then judge whether it contains "inf"
			else
			{
				//if have, put in fromNode and Distance
				if(HMap.get(sp[2]).equals(targetID + "inf"))
				{
					HMap.put(sp[2], sp[1]+":"+ sp[3]);
				}
				//if no, put in it's own key and value
				else
				{
					HMap.put(sp[2], HMap.get(sp[2]) + "," + sp[1] + ":" + sp[3]);
				}
					
			}
			//At least, check whether it has Node which does not appear in toNode list. Give it a value 0.
			if(!HMap.containsKey(sp[1]))
			{
				HMap.put(sp[1], targetID + ":0");
				
			}
			
//			System.out.println("HMap: " + HMap);
//			System.out.println("-----------------");

			//Write into the reducer.

		}
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for(Map.Entry<String, String> mEntry: HMap.entrySet())
			{
//				System.out.println("key: " + mEntry.getKey());
//				System.out.println("value: " + mEntry.getValue());
				
				context.write(new IntWritable(Integer.parseInt(mEntry.getKey())), new Text(mEntry.getValue()));
			}			
		}
	}
	
	//First reducer we use
	public static class PreReducer extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//Get queryNode
			Configuration config = context.getConfiguration();
			queryNodeID = config.getInt("queryNodeID", 0);
					
			//Prepare the Node information like below format:
			//  key: Node
			//Value: Inf	1:1.0,2:9.0,4:6.0R3
			//
			//In Value, Inf means the current distance we get is Inf.
			//Followed by the adjacency list information of this node.
			//R means the current route we have.
			String sb = "";
			for(Text val: values)
			{
				//if more than one Node:Distance pair in the val, split it and add each one.
				if(val.toString().contains(","))
				{
					String[] sub_val = val.toString().split(",");
					for(String x: sub_val)
					{
						if(sb.contains(x))
						{
							continue;
						}
						
						if(sb.length() == 0 && !sb.contains(x))
						{
							sb += x;
						}
						else
						{
							sb += ",";
							sb += x;
						}
					}
				}
				//just one Node:Distance pair, add it.
				else
				{
					if(sb.contains(val.toString()))
					{
						continue;
					}
					
					if(sb.length() == 0 && !sb.contains(val.toString()))
					{
						sb += val.toString();
					}
					else
					{
						sb += ",";
						sb += val.toString();
					}
				}

			}
			
			
			//Check whether this node is the query Node, and change the current distance of this node to 0.
			String inf = "Inf";
			
			if(key.get() == queryNodeID)
			{
				inf = "0";
			}
			
//			System.out.println("------reduce------");
//			System.out.println("queryNodeID: " + queryNodeID);
//			System.out.println("key: " + key.get());
//			System.out.println(inf + "\t" + sb.toString() + "R" + key.get());
			
			IntWritable k1 = new IntWritable();
			k1.set(key.get());
			
			//write key, value to the next
			context.write(k1, new Text(inf + "\t" + sb.toString() + "R" + key.get()));
		}
		
	}	


	//Second Mapper we have, this mapper just compute the distance and send it to next reducer
    public static class STMapper extends Mapper<Object, Text, LongWritable, Text> 
    {

		@Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
        	String line = value.toString();
        	String[] sp = line.split("[\tR]");

//        	System.out.println("key: " + key);
//        	System.out.println("Value: " + line);
//        	 
//        	System.out.println("sp[0]: " + sp[0]);
//        	System.out.println("sp[1]: " + sp[1]);
//        	System.out.println("sp[2]: " + sp[2]);
//        	System.out.println("sp[3]: " + sp[3]);
//        	System.out.println("Length: " + sp.length);
//        	
//        	System.out.println("---Then get Value---");
        	
        	//sp[0] is toNode, sp[1] is distance
        	context.write(new LongWritable(Integer.parseInt(sp[0])), new Text(sp[1] + "\t" + sp[2] + "R" + sp[3]));
        	
        	String AddDistance = sp[1];
        	if(!AddDistance.equals("Inf"))//it means that this node need to calculate its value
        	{
//            	System.out.println("Value: " + line);
//            	System.out.println("sp[0]: " + sp[0]);
//            	System.out.println("sp[1]: " + sp[1]);
//            	System.out.println("sp[2]: " + sp[2]);
        		
        		if(sp[2].contains(","))// more than 1 node connected to this node
        		{
        			String[] nodes = sp[2].split(",");
        			for(String node: nodes)
        			{
        				String[] pos_dis = node.split(":");
        				
//            			System.out.println("pos_dis[0]: "+ pos_dis[0]);
//            			System.out.println("AddDistance: " + AddDistance);
//            			System.out.println("pos_dis[1]: "+ pos_dis[1]);
            			
        				Double distance = Double.parseDouble(AddDistance) + Double.parseDouble(pos_dis[1]);
        				
//            			System.out.println("Total distance: "+ distance);
        				//Record the Route we pass here in the end
        				context.write(new LongWritable(Integer. parseInt(pos_dis[0])), new Text(String.valueOf(distance) + "R" + sp[3]));
        			}
        		}
        		else //only one node connected to this code
        		{
        			String[] pos_dis = sp[2].split(":");
        			
//        			System.out.println("pos_dis[0]: "+ pos_dis[0]);
//        			System.out.println("AddDistance: " + AddDistance);
//        			System.out.println("pos_dis[1]: "+ pos_dis[1]);
        			
        			Double distance = Double.parseDouble(AddDistance) + Double.parseDouble(pos_dis[1]);
        			
//        			System.out.println("distance: "+ distance);
        			
        			//Record the Route we pass here in the end
        			context.write(new LongWritable(Integer.parseInt(pos_dis[0])), new Text(String.valueOf(distance) + "R" + sp[3] ));
        		}
        	}
//        	System.out.println("---------------------");
        }

    }

    //Second reducer we use
    public static class STReducer extends Reducer<LongWritable, Text, LongWritable, Text> 
    {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
        	ArrayList<String> al = new  ArrayList<String>();
        	
        	//Add all text to an Arraylist
        	for(Text val :values)
        	{
        		al.add(val.toString());
        	}
        	Collections.sort(al);
        	Collections.sort(al);
        	String head = al.get(0);
        	
//        	System.out.println("======Reducer Old Line=========");
//        	System.out.println("key " + key);
//        	System.out.println("al: " + al);
//        	System.out.println("head: " + head);
//        	System.out.println("al.length: " + al.size());
        	
        	//Define the useful var
        	String current_distance, current_route;
        	String min;
        	String route;
        	String AdList; 
        	boolean update = false;
        	
        	//The al size should just be 1 or 2. If this length >3, it means some node are added before, and it is added again now.
        	//Then we just only preserve the one with "\t"(this one contains the adjacency information)
        	//So we first cut the unnecessary elements, and the element we want should always be in the first two. (The Nodes that Mapped sends here should appear in the first.)
        	//Example:
        	// al: [0	0:10.0,2:3.0R1, 10.0R01, 6.0R21] --->  al: [0	0:10.0,2:3.0R1], here 10.0R01, 6.0R21 are already computed.
        	//or al: [11.0R120, 15.0R340, 4.0	1:3.0R2040, 4.0R40, 5.0R0] --> [11.0R120, 15.0R340, 4.0	1:3.0R2040], here 4.0R40, 5.0R0 are unnecessary
        	
        	if(al.size() > 2)
        	{
        		boolean Flag = false;
        		ArrayList<String> al2 = new  ArrayList<String>();
        		al2.add(al.get(0));
        		for(int i=0;i<al.size();i++)
        		{
        			if(al.get(i).contains("\t") && !al2.contains(al.get(i)))
        			{
        				al2.add(al.get(i));
        				break;
        			}
        		}
        		al = al2;
        	}
        	//When the al.length == 3, we could get the first element and the element with"\t",
        	//which is our needs to cmpare.
        	//For example:
        	//[11.0R120, 15.0R340, 4.0	1:3.0R2040] --> [11.0R120, 4.0	1:3.0R2040]
        	
        	
        	if(al.size() == 3 && !al.get(2).contains("\t"))
        	{
        		ArrayList<String> al2 = new  ArrayList<String>();
        		al2.add(al.get(0));
        		al2.add(al.get(1));
        		al = al2;
        	}
        	else if(al.size() == 3 && al.get(2).contains("\t"))
        	{
        		ArrayList<String> al2 = new  ArrayList<String>();
        		al2.add(al.get(0));
        		al2.add(al.get(2));
        		al = al2;
        	}
        	
//        	System.out.println("======Reducer New Line=========");
//        	
//        	System.out.println("key " + key);
//        	System.out.println("al: " + al);
//        	System.out.println("head: " + head);
//        	System.out.println("al.length: " + al.size());
        	
        	//Then the al.size should be 2 we split the string to get the current min and the current distance
        	//For example:
        	//[11.0R120, 4.0	1:3.0R2040] --> current distance == 11 and min ==4
        	// Change the min if current distance is lower, and add the route 
        	if(al.size() == 2)
        	{
        		if(!head.contains("\t"))
        		{
        			current_distance = al.get(0).split("R")[0];
        			current_route = al.get(0).split("R")[1];

        			String Main_chain[] = al.get(1).split("[\tR]");
        			
            		min = Main_chain[0];// INF or recorded current distance
            		AdList = Main_chain[1];//info from adlist
            		route = Main_chain[2]; //routes      		
      
            		if(min.equals("Inf") || Double.parseDouble(min)>Double.parseDouble(current_distance))
            		{
//            			System.out.println("OUTPUT: " + current_distance + "\t" + AdList + "R" + route +"|"+ current_route);
            			
            			//write back with new distance and routes
            			context.write(key, new Text(current_distance + "\t" + AdList + "R" + route +"|"+ current_route));
            			//change the update to get the counter updated.
            			update = true;
            		}
            		else
            		{
            			//no change,just write back
            			context.write(key, new Text(al.get(1)));
            		}
        		}
        		else if(head.contains("\t"))
        		{
        			current_distance = al.get(1).split("R")[0];
        			current_route = al.get(1).split("R")[1];

        			String Main_chain[] = al.get(0).split("[\tR]");
        			
            		min = Main_chain[0];// INF or recorded current distance
            		AdList = Main_chain[1];//info from adlist
            		route = Main_chain[2]; //routes      		
      
            		if(min.equals("Inf") || Double.parseDouble(min)>Double.parseDouble(current_distance))
            		{
//            			System.out.println("OUTPUT: " + current_distance + "\t" + AdList + "R" + route +"|"+ current_route);
            			//write back with new distance and routes
            			context.write(key, new Text(current_distance + "\t" + AdList + "R" + route + "|"+ current_route));
            			//change the update to get the counter updated.
            			update = true;
            		}
            		else
            		{//no change,just write back
            			context.write(key, new Text(al.get(0)));
            		}
        		}

        		
        	}
        	//if al.size = 1, it means no nodes come in at all, just write it back
        	else if(al.size()==1)
        	{
        		context.write(key, new Text(al.get(0)));
        	}
        	
        	//if updated, make the counter count.
        	if(update)
        	{
        		context.getCounter(eInf.COUNTER).increment(1L);
        	}

        }
    }
    
    //The third Mapper we use.
    public static class ResultMapper extends Mapper<Object, Text, IntWritable, Text>
    {
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    	{
    		String line = value.toString();
    		
    		//split the value, and sp[3] is the routes recorded.
    		String[] sp = line.split("[\tR]");
    		
//    		System.out.println("sp[0]" + sp[0]);
//    		System.out.println("sp[1]" + sp[1]);
//    		System.out.println("sp[2]" + sp[2]);
//    		System.out.println("sp[3]" + sp[3]);
    		
    		//The last char should be the targetID
			Configuration config = context.getConfiguration();
			queryNodeID = config.getInt("queryNodeID", 0);
			
//    		System.out.println("queryNodeID: " + queryNodeID);
    		
    		if(!sp[1].equals("Inf"))
    		{
    			//We need to preserve the last char and distinct the char before 
    			ArrayList<Integer> route_result = new ArrayList<Integer>();
    			ArrayList<Integer> route_result_final = new ArrayList<Integer>();
    			String route = sp[3];

    			System.out.println("route2: " + route);
    			
    			String x = "";
    			for(int i=0;i<route.length();i++)
    			{
    				if(!String.valueOf(route.charAt(i)).equals("|"))
    				{
    					x+=String.valueOf(route.charAt(i));	
    				}
    				else
    				{
    					route_result.add(Integer.parseInt(x));
    					x="";
    				}
    			}
    			if(!x.equals(""))
    			{
    				route_result.add(Integer.parseInt(x));
    			}
//    			System.out.println("route_result: " + route_result);

    			
    			for(int i=0; i<route_result.size(); i++)
    			{
    				
					if(queryNodeID!=0 && i!=0 && route_result.get(i)==0)
					{
						break;
					}
    				else
    				{

        				if(route_result.get(i) != queryNodeID && i!=route_result.size()-1 && !route_result_final.contains(route_result.get(i)))
        				{
        					route_result_final.add(route_result.get(i));
        					continue;
        				}
        				if(i == route_result.size()-1)
        				{
        					route_result_final.add(route_result.get(i));
        				}
    				}
    			}
    			
        		//After all process, we connect all elements in route_result with "->"
//        		System.out.println(route_result_final);
//        		System.out.println("----------------");
        		
    			
    			
    			String r = "";
    			for(int i=0;i<route_result_final.size();i++)
    			{
    				r+=String.valueOf(route_result_final.get(i));
    				if(!(i==route_result_final.size()-1))
    				{
    					r+="->";
    				}
    			}
    			String sr[] = r.split("->");
    			if(Integer.parseInt(sr[sr.length-1]) == queryNodeID)
    			{
    				context.write(new IntWritable(Integer.parseInt(sp[0])), new Text(sp[1] + "\t" + r));
    			}
    			
        		//Write back to the reducer
    			
    		}
    		
    	}
    }
    
    //The third reducer we use.
    public static class ResultReducer extends Reducer<IntWritable, Text, IntWritable, Text>
    {
    	
    	public void reduce(IntWritable key, Text values, Context context) throws IOException, InterruptedException
    	{    		
    		//output the context.
			context.write(key, values);
    		
    	}
    }
    
    

    


    public static void main(String[] args) throws Exception 
    {  
    	IN = args[0];
		OUT = args[1];
		
		int queryNodeID = Integer.parseInt(args[2]);
		
		int iteration = 0;

		// Initialize the input and output file
		String input = IN;
		String output = OUT + iteration;

		// YOUR JOB: Convert the input file to the desired format for iteration,
		// Transform to the input format.
		// Input Format: SourceNode Distance (ToNode: distance; ToNode:
		// distance; ...)
		Configuration conf = new Configuration();
		conf.setInt("queryNodeID", queryNodeID);
		
		Job job1 = Job.getInstance(conf, "PreProcess");
		job1.setJarByClass(SingleTargetSP.class);
		// Set the corresponding class for map and reduce.
		job1.setMapperClass(PreMapper.class);
		job1.setReducerClass(PreReducer.class);
		

		// Set the Data Type for reducer's output.
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setNumReduceTasks(1);;
		
		// Set the input path and output path.
		FileInputFormat.addInputPath(job1, new Path(input));
		FileOutputFormat.setOutputPath(job1, new Path(output));
		job1.waitForCompletion(true);
		
		//Repeat the second Map/Reduce
		boolean isdone = false;
		while(isdone == false)
		{
			Job job2 = Job.getInstance(conf, "ST Mapper");
			job2.setJarByClass(SingleTargetSP.class);
			
			job2.setMapperClass(STMapper.class);
			job2.setReducerClass(STReducer.class);
		
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setNumReduceTasks(1);
			
			input = output;
			iteration++;
			output = OUT + iteration;
			
			// Set the input path and output path.
			FileInputFormat.addInputPath(job2, new Path(input));
			FileOutputFormat.setOutputPath(job2, new Path(output));
			boolean flag = job2.waitForCompletion(true);
			
			if(flag && job2.getCounters().findCounter(eInf.COUNTER).getValue() == 0)
			{
				isdone = true;
			}
		}
		
		//Deal with the third Map/Reduce
		while(isdone == true)
		{
			Job job3 = Job.getInstance(conf, "Result");
			job3.setJarByClass(SingleTargetSP.class);
			
			job3.setMapperClass(ResultMapper.class);
			job3.setReducerClass(ResultReducer.class);
			
			job3.setOutputKeyClass(IntWritable.class);
			job3.setOutputValueClass(Text.class);
			
			job3.setNumReduceTasks(1);
			
			input = output;
			output = OUT;
			
			FileInputFormat.addInputPath(job3, new Path(input));
			FileOutputFormat.setOutputPath(job3, new Path(output));
			
			System.exit(job3.waitForCompletion(true)? 0 : 1);
			
			
		}
    }
    
}


