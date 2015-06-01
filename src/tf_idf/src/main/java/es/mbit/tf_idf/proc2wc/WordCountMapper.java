package es.mbit.tf_idf.proc2wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Grupo1: Alicia, Flor, Javier Muñoz, Javier Finez
 * 
 * Recibo:
 * palabra@nombrefichero	contador
 * ...
 * 
 * Emito:
 * palabra,"nombrefichero#contador"
 * ...
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// como recibo "palabra@nombrefichero	contador" divido por la tabulación (\t)
		// y obtengo el array [palabra@nombrefichero,contador]
		String[] wordAndDocCounterArray = value.toString().split("\t");
		
		// la palabra y nombrefichero están en la posición 0 del array [->palabra@nombreficherot<-,contador]
		String wordAndDoc = wordAndDocCounterArray[0];
		
		// el contador está en la posición 1 del array [palabra@nombrefichero,->contador<-]
		String counter = wordAndDocCounterArray[1];
		
		// divido la palabra y el nombrefichero por el simbolo arroba "palabra@nombrefichero"
		// y obtengo el array [palabra,nombrefichero]
		String[] wordAndDocSplit = wordAndDoc.split("@");

		// la palabra en la posición 0 del array [->palabra<-,nombrefichero]
		String word = wordAndDocSplit[0];
		
		// el nombrefichero en la posición 1 del array [palabra,->nombrefichero<-]
		String docName = wordAndDocSplit[1];

		// creo el valor que voy a emitir: "nombrefichero#contador"
		String newValue = docName + "#" + counter;

		// Emito: palabra, "nombrefichero=contador"
		context.write(new Text(word), new Text(newValue));

	}

}
