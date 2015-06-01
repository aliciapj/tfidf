package es.mbit.tf_idf.proc3idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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

public class IdfDriver extends Configured implements Tool {

	public static final String NUMBEROFDOCS = "numberOfDocsInCorpus";

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance();

		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);

		// Elimina el directorio de salida (si existe)
		Path userOutputPath = new Path(args[1]);
		if (fs.exists(userOutputPath)) {
			fs.delete(userOutputPath, true);
		}

		// Obtiene el número total de ficheros que hay en la ruta de entrada
		Path userInputPath = new Path(args[2]);
		FileStatus[] userFilesStatusList = fs.listStatus(userInputPath);
		final int numberOfUserInputFiles = userFilesStatusList.length;
		conf.setInt(NUMBEROFDOCS, numberOfUserInputFiles);


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(IdfDriver.class);
		job.setJobName("Idf Calc");
		job.setMapperClass(IdfMapper.class);
	    job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf("Usage: IdfDriver <input dir> <output dir> <originalinput dir> \n");
			System.exit(-1);
		}

		int res = ToolRunner.run(new Configuration(), new IdfDriver(),
				args);
		System.exit(res);
	}
}