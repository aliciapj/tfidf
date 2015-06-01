package es.mbit.tf_idf.proc2wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
* Grupo1: Alicia, Flor, Javier Muñoz, Javier Finez
*/

public class WordCountDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
	
		Job job = Job.getInstance();

		FileSystem fs = FileSystem.get(job.getConfiguration());

		// Elimina el directorio de salida (si existe)
		Path outputPath = new Path(args[1]);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setJarByClass(WordCountDriver.class);
		job.setJobName("Words Counts");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
//		job.setCombinerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new WordCountDriver(), args);
		System.exit(res);
	}

}
