package es.mbit.tf_idf.proc1tf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Recibo:
 * palabra@nombrefichero, (1,1,1,1,1,1,1,1,1...) 
 * ...
 * 
 * Emito:
 * palabra@nombrefichero, (1+1+1+1+1+1+1+1+1...) 
 * ...
 */

public class WordFreqReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		// inicializo el contador
		int wordCount = 0;

		// recorro la lista de valores y los sumo
		for (IntWritable value : values) {
			wordCount += value.get();
		}

		// emito "palabra@nombrefichero", wordcount 
		context.write(key, new IntWritable(wordCount));

	}
}