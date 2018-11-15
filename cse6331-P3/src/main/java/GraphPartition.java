import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                  // the vertex ID
    public long size;
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;  // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */
    Vertex(){}
    Vertex (Vertex A)
    {
    	id=A.id;
    	size=A.size;
    	adjacent=A.adjacent;
    	centroid=A.centroid;
    	depth=A.depth;
    }
    
    Vertex (long Id,long Centroid, short Depth,long Size, Vector<Long> Adjacent)
    {
    	id=Id;
    	size=Size;
    	adjacent=Adjacent;
    	centroid=Centroid;
    	depth=Depth;
    }
    public void write ( DataOutput out ) throws IOException 
	{
		out.writeLong(id);
		//writeVector(adjacent,out);
		out.writeLong(centroid);
		out.writeShort(depth);
		out.writeLong(size);
		for(long l:adjacent)
		{
			out.writeLong(l);
		}
		
	 
	}
 public void readFields ( DataInput in ) throws IOException 
	{	
		id = in. readLong ();
		centroid = in. readLong ();
		depth = in. readShort ();
		size = in.readLong();
		adjacent=new Vector<Long>();
		for (int i=0;i<size;i++)
		{
			adjacent.add(in.readLong());
		}

	}


}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static int counter=0;
    static short BFS_depth = 0;
    
    public static class firstMapper extends Mapper<Object,Text,LongWritable,Vertex> 
   	{
    	@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException 
				{	//int counter=1;
					long temp=-1;
					short temp2=0;
    				Scanner input = new Scanner(value.toString()).useDelimiter(",");
    				long vertexid = input.nextLong();
    				//centroids.add(vertexid);
    				Vector<Long> ADJACENT=new Vector<Long>();
    				long centroid=-1;
    				Vertex Final=new Vertex(vertexid,centroid,temp2,ADJACENT.size(),ADJACENT);
    				while(input.hasNextLong())
    				{
    					long VERtex = input.nextLong();
    					ADJACENT.addElement(VERtex);
    					if (counter<10)
    					{
    						centroid=vertexid;
    						//unter=counter+1;
    						Final= new Vertex(vertexid,centroid,temp2,ADJACENT.size(),ADJACENT);
    					}
    					else
    					{
    						
    						//counter=counter+1;
    						Final= new Vertex(vertexid,centroid,temp2,ADJACENT.size(),ADJACENT);
    					}
    				
    				}
    				centroids.addElement(centroid);
					counter=counter+1;
    				input.close();
    				context.write(new LongWritable(vertexid), new Vertex(Final));
    				//System.out.println(Final.centroid);
    				
    				
				}
   	}
    public static class firstReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
    {
			@Override
			public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
							 throws IOException, InterruptedException
			{	
				Vertex output=new Vertex();
				for (Vertex V:values)
				{
					output=V;
				}
				context.write(key,new Vertex(output));
			}
			
    }
    public static class secondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex> 
   	{
    	@Override
		public void map ( LongWritable key, Vertex value, Context context )
				throws IOException, InterruptedException 
				{	
		    	
    				context.write(new LongWritable(value.id), value);
    				if (value.centroid>(long)0)
    				{
    					for(long l:value.adjacent)
    					{
    						context.write(new LongWritable(l), new Vertex(l,value.centroid,BFS_depth,value.adjacent.size(),value.adjacent));
    						
    					}
    				}
				}

    /* ... */
   	}
    public static class secondReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
    {
			@Override
			public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
							 throws IOException, InterruptedException
			{
				short  min_depth = 1000;
				Vector<Long> trrt =new Vector<Long>();
				//long centroider=-1;
				short DFSer=0;
				long hh=0;
				long hhjj=trrt.size();
				for (Vertex v:values)
				{
					hh=v.id;
					//System.out.println("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"+v.centroid);
					
				}
				
				Vertex m = new Vertex();
				m.id=Long.valueOf(key.get());
				m.centroid=(long)-1;
				m.depth=(short)0;
				for (Vertex v:values)
				{
					//hh=v.id;
					//System.out.println("gzvHVXJbscyooooooooooooooooooooooooooooooooooooooooooooooo"+v.centroid);
					
				}

				 //Debug//System.out.println("vsdbc xh hxgcvn zjh dscvh d vcjsdbvh dvjhsd vjdsvb dvkbdkjv hnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
				for (Vertex e:values)
					System.out.println("10");
				for (Vertex v:values)
				{	System.out.println("IIIIIIIIIIIIIIIIIIIIIIIIIIIIII"+v.centroid+" "+v.depth);
					if(!(v.adjacent.isEmpty()))
					{
						m.adjacent=v.adjacent;
					}
					
					if(v.centroid>0 && v.depth<min_depth)
					{
						min_depth = v.depth;
					    m.centroid = v.centroid;
					}
				}
				m.depth=min_depth;
				context.write(key,new Vertex(m));
				//System.out.println("yooooooooooooooooooooooooooooooooooooooooooooooo"+m.centroid);
			
				
			}
    
    }
    public static class lastMapper extends Mapper<LongWritable,Vertex,LongWritable,IntWritable> 
   	{
    	@Override
		public void map ( LongWritable key, Vertex value, Context context )
				throws IOException, InterruptedException 
				{	
    				context.write(new LongWritable(value.centroid), new IntWritable(1));
				}
   	}
    public static class lastReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable>
    {
			@Override
			public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context )
							 throws IOException, InterruptedException
			{	
				int m=0;
				for(IntWritable v:values)
				{
					long j =Long.valueOf(v.get());
					m=m+(int)j;
				}
				context.write(key, new IntWritable(m));
			}
				
    }		
    public static void main ( String[] args ) throws Exception {
    	
        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
        job1.setJarByClass(GraphPartition.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);        
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,firstMapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/i0"));
        /* ... First Map-Reduce job to read the graph */
        job1.waitForCompletion(true);
        Path inpath =new Path(args[1]);
		Path outpath =null;
        for ( short i = 0; i < max_depth; i++ )
        {
            BFS_depth++;
    		Job job2 = Job.getInstance();
    		//outpath = new Path(args[1]+"/f"+i);
    		job2.setJobName("job2");
    		job2.setJarByClass(GraphPartition.class);
    		job2.setOutputKeyClass(LongWritable.class);
    		job2.setOutputValueClass(Vertex.class);
    		job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
    		job2.setReducerClass(secondReducer.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
    		MultipleInputs.addInputPath(job2,new Path(args[1]+"/i"+i),SequenceFileInputFormat.class,secondMapper.class);
    		FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/i"+(i+1)));
    		inpath=outpath;
    		job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
		job3.setJobName("3");
		job3.setJarByClass(GraphPartition.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
		job3.setReducerClass(lastReducer.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
		MultipleInputs.addInputPath(job3,new Path(args[1]+"/i8"),SequenceFileInputFormat.class,lastMapper.class);
		FileOutputFormat.setOutputPath(job3,new Path(args[2]));
		job3.waitForCompletion(true);
		for(long yy:centroids)
		{
			//System.out.println(yy);
		}
    }
}
/*
References:

1) To understand how to build map setup:
https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
2) How to read each line for centroid.txt:
https://stackoverflow.com/questions/5868369/how-to-read-a-large-text-file-line-by-line-using-java
3) Build Compareto method in class Point:
https://hadoop.apache.org/docs/r2.6.4/api/org/apache/hadoop/io/WritableComparable.html
4) A lot of other references were used to understand very basic functionalities of "Hadoop" and "Java".
5) https://github.com/fahadfurniturewala/Implementation_of_graph-MapReduce-Java/blob/master/Graph.java


Problems encountered and resolved:

1) Improper definition of constructor with Point P which led to reading all values of points as (0.0,0.0)
2) Comparing only x values in the compareTo method which caused only 10 Centroids in the reduce result. 
3) Couldnot locate URI as Java.net.URI wasn't imported.

*/