package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("CountryRequestCount".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, CountryRequestCount.UserMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, CountryRequestCount.MessageMapper.class );

		job.setReducerClass(CountryRequestCount.JoinReducer.class);

		job.setOutputKeyClass(CountryRequestCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryRequestCount.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("URLCountryCount".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, URLCountryCount.UserMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, URLCountryCount.MessageMapper.class );

		job.setReducerClass(URLCountryCount.JoinReducer.class);

		job.setOutputKeyClass(URLCountryCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(URLCountryCount.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("URLCountrySorter".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(URLCountrySorter.ReducerImpl.class);
		job.setMapperClass(URLCountrySorter.MapperImpl.class);

		job.setPartitionerClass(URLCountrySorter.PartitionerImpl.class);
		job.setGroupingComparatorClass(URLCountrySorter.GroupingComparator.class);
		job.setSortComparatorClass(URLCountrySorter.SortComparator.class);

		job.setOutputKeyClass(URLCountrySorter.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(URLCountrySorter.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("URLListCountry".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(URLListCountry.ReducerImpl.class);
		job.setMapperClass(URLListCountry.MapperImpl.class);
		job.setOutputKeyClass(URLListCountry.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(URLListCountry.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog2".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(AccessLog2.ReducerImpl.class);
		job.setMapperClass(AccessLog2.MapperImpl.class);
		job.setOutputKeyClass(AccessLog2.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(AccessLog2.OUTPUT_VALUE_CLASS);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("SumCountry".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(SumCountry.ReducerImpl.class);
		job.setMapperClass(SumCountry.MapperImpl.class);
		job.setOutputKeyClass(SumCountry.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(SumCountry.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
