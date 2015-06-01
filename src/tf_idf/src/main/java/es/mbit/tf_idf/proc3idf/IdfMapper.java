package es.mbit.tf_idf.proc3idf;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Grupo1: Alicia, Flor, Javier Muñoz, Javier Finez
 * 
 * Recibo:
 * "palabra1@nombrefichero", "contador/docsConPalabra"
 * ...
 * 
 * Emito:
 * "palabra@nombrefichero", "TF*IDF"
 * ...
 */

public class IdfMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final DecimalFormat DF = new DecimalFormat("###.########");
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// como recibo "palabra@nombrefichero	contador/docsConPalabra" divido por la tabulación (\t)
		// y obtengo el array [palabra@nombrefichero,contador/docsConPalabra]
		String[] generalArray = value.toString().split("\t");
		
		// la palabra y nombrefichero están en la posición 0 del array [->palabra@nombreficherot<-,contador/docsConPalabra]
		String wordAndDoc = generalArray[0];
		
		// el contador está en la posición 1 del array [palabra@nombrefichero,->contador/docsConPalabra<-]
		String counterAndTotal = generalArray[1];
		
		// divido la palabra y el nombrefichero por el simbolo arroba "palabra@nombrefichero"
		// y obtengo el array [palabra,nombrefichero]
		String[] wordAndDocSplit = wordAndDoc.split("@");

		// la palabra en la posición 0 del array [->palabra<-,nombrefichero]
		String word = wordAndDocSplit[0];

		// el nombrefichero en la posición 1 del array [palabra,->nombrefichero<-]
		String docName = wordAndDocSplit[1];
		
		// divido el contador y el docsConPalabra por el simbolo "/"
		// y obtengo el array [contador,docsConPalabra]
		String[] counterAndTotalArray = counterAndTotal.split("/");
		
		// el contador está en la posición 0 del array [->contador<-,docsConPalabra]
		Integer counter = Integer.parseInt(counterAndTotalArray[0]);

		// el nombrefichero en la posición 1 del array [palabra,->nombrefichero<-]
		String docsWithWord = counterAndTotalArray[1];

		// obtengo el total de documentos que estoy tratando
		int totalDocs = context.getConfiguration().getInt(IdfDriver.NUMBEROFDOCS, 0);
		
		// idf = log (totalDocs / docsWithWord)
		Double idf = Math.log10(Double.valueOf(totalDocs) / Double.valueOf(docsWithWord));

		// tfidf = tf * idf
		Double tfidf = counter * idf;
		
		// creo el valor que voy a emitir: "palabra@nombrefichero"
		String newKey = word + "@" + docName;

		// Emito: "palabra@nombrefichero", "tf*idf"
		context.write(new Text(newKey), new Text(DF.format(tfidf)));

	}
	
}
