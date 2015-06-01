package es.mbit.tf_idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import es.mbit.tf_idf.proc1tf.WordFreqDriver;
import es.mbit.tf_idf.proc2wc.WordCountDriver;
import es.mbit.tf_idf.proc3idf.IdfDriver;

/*
* Grupo1: Alicia, Flor, Javier Muñoz, Javier Finez
*/

public class TfIdfDriver {

	// sufijo que añadirá a la ruta de salida para el resultado parcial del
	// proceso 1
	private static final String OUTPUT_PATH1 = "_p1";

	// sufijo que añadirá a la ruta de salida para el resultado parcial del
	// proceso 2
	private static final String OUTPUT_PATH2 = "_p2";

	public static void main(String[] args) throws Exception {

		int res1 = 1;
		int res2 = 1;
		int res3 = 1;

		if (args.length != 2) {
			System.out.printf("Usage: TfIdfDriver <input dir> <output dir>\n");
			System.exit(-1);
		}

		String userInput = args[0];
		String userOutput = args[1];

		args[1] = userOutput + OUTPUT_PATH1;
		res1 = ToolRunner.run(new Configuration(), new WordFreqDriver(), args);

		if (res1 != -1) {
			args[0] = userOutput + OUTPUT_PATH1;
			args[1] = userOutput + OUTPUT_PATH2;
			res2 = ToolRunner.run(new Configuration(), new WordCountDriver(),
					args);

			if (res2 != -1) {
				String[] newArgs = new String[3];
				newArgs[0] =  userOutput + OUTPUT_PATH2;
				newArgs[1] = userOutput;
				newArgs[2] = userInput;
				res3 = ToolRunner.run(new Configuration(), new IdfDriver(),
						newArgs);
			}

		}

		System.exit(res3);
	}

}
