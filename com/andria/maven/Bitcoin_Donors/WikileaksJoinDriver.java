package com.andria.maven.Bitcoin_Donors;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikileaksJoinDriver {
	

	public static final String TOUT_FILENAME_CONF_KEY = "Wikileaks_Don";


		//transaction out is held in Dictionary in the forma of a hashmap : scope to integrate spark RDD in future

		//The key in this hashmap will be all of the unique hashes relating to outgoing transactions, the value will be the actual writable 
        //object storing other fields which dont need to be Keys but may need to be dispayed togethetr with the value etc to give context to the record	 

	
	public static void setupJob( String voutFile, String vinFile, String outPutOfJoin) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException
	{
		
        Path voutFiltered = new Path(voutFile);
		
		Path vinUnfiltered = new Path(vinFile);
		
        Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Bitcoin Wikileaks Repartition Join");
		
		job.setJarByClass(WikileaksJoinDriver.class);
		
		
		Path outputPath = new Path(outPutOfJoin);

		// Create cache file and set path in configuration to be retrieved later by the mapper
		//job.addCacheFile(voutFiltered.toUri());
		
		job.getConfiguration().set(TOUT_FILENAME_CONF_KEY , voutFiltered.getName());

		// Mapper configuration
		
		job.setMapperClass(transactionMapper.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
	
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(Text.class);

		// Only one reduce task needed , in order to sort output of join based on bitcoin amount
		job.setNumReduceTasks(1);
		
		//t input and output destination for the job

		FileInputFormat.setInputPaths(job, vinUnfiltered);
		
		job.addCacheFile(new URI (voutFile));
		
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
		

	public static void main(String[] args) throws Exception {
		
		if ( args.length != 3) {
			
			System.err.println("You need to enter three arguments: 1: TransactionIn, 2:TransactionOut, 3: Output destination for joined records");
		
		}
		
		setupJob(args[0], args[1], args[2]);
	}

		

	}