package es.mbit.tf_idf.proc1tf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Grupo1: Alicia, Flor, Javier Muñoz, Javier Finez
 * 
 * Recibo:
 * -Fichero1.txt-
 * "1	Toy Story (1995)	Adventure|Animation|Children|Comedy|Fantasy"
 * 
 * Emito:
 * palabra@nombrefichero, 1
 * 
 */

public class WordFreqMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// como recibo
		// "1	Toy Story (1995)	Adventure|Animation|Children|Comedy|Fantasy" divido por la tabulación (\t)
		// y obtengo el array ["1","Toy Story (1995)","Adventure|Animation|Children|Comedy|Fantasy"]
		String[] lineArray = value.toString().split("\t");

		if (lineArray.length == 3) {
			// los géneros están en la posición 2 del array
			// ["1","Toy Story (1995)",->"Adventure|Animation|Children|Comedy|Fantasy"<-]
			String genres = lineArray[2];

			// como los géneros están divididos por una barra
			// "Adventure|Animation|Children|Comedy|Fantasy" divido por la barra
			// y obtengo el array de géneros
			// ["Adventure","Animation","Children","Comedy","Fantasy"]
			String[] genreArray = genres.split("\\|");

			// obtengo el nombre del fichero que estoy procesando
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();

			// para cada género de la lista...
			for (String word : genreArray) {

				// paso la palabra a minúsculas para uniformizar el término
				word = word.toLowerCase();

				// creo la clave que voy a emitir (palabra@nombrefichero)
				String keyWord = word + "@" + fileName;

				// emito "palabra@nombrefichero", 1
				context.write(new Text(keyWord), new IntWritable(1));
			}
		}
	}

}
