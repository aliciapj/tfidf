package es.mbit.tf_idf.proc1tf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
* Grupo1: Alicia, Flor, Javier Muñoz, Javier Finez
*/

public class WordFreqDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Usage: WordFreqDriver <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = Job.getInstance();

		FileSystem fs = FileSystem.get(job.getConfiguration());

		// Elimina el directorio de salida (si existe)
		Path outputPath = new Path(args[1]);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setJarByClass(WordFreqDriver.class);
		job.setJobName("Word Frequency in Doc");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(WordFreqMapper.class);
		job.setReducerClass(WordFreqReducer.class);
		job.setCombinerClass(WordFreqReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordFreqDriver(),
				args);
		System.exit(res);
	}

}
