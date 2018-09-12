//to display the count of no of drivers based on hours and miles
//drivers.csv
import java.io.*;
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

	//data - student
	public class driverscount{
		
		public static class FruitMap extends Mapper<LongWritable,Text,Text,IntWritable>
		   {
		     public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		      {	    	  
		       	final IntWritable  i =new  IntWritable(1);
       	    	 String [] data = value.toString().split(","); 
       	    	 Text basis=new Text(data[5]);
       	    	 con.write(basis,i);
		         }
		        
		      
		   }
		
		  public static class FruitReduce extends Reducer<Text,IntWritable,Text,IntWritable>
		   {
			    
			  public void reduce(Text key, Iterable<IntWritable> val,Context context) throws IOException, InterruptedException 
			    	{
			    	 int count=0;
			         for(IntWritable i :val)
			    		{
			        	  count=count+i.get();
			    		}
			    		
			    	 IntWritable a=new IntWritable(count);
			    		context.write(key,a);
			    		
			    		
			 
			    	}
		   }
		
			    
			    
			    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
			    Configuration conf = new Configuration();
				Job job = Job.getInstance(conf, "Total Marks");
			    job.setJarByClass(driverscount.class);
			    job.setMapperClass(FruitMap.class);
			    job.setReducerClass(FruitReduce.class);
			    job.setNumReduceTasks(1);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(IntWritable.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
	}
	
	
	
	