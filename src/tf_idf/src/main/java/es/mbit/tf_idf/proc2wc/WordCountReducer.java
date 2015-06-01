package es.mbit.tf_idf.proc2wc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * Grupo1: Alicia, Flor, Javier Muñoz, Javier Finez
 * 
 * Recibo:
 * 
 * palabra, ["nombrefichero1#contador1","nombrefichero2#contador2","nombrefichero3#contador3","nombrefichero4#contador4",...]
 * ...
 * 
 * Emito:
 * "palabra1@nombrefichero", "contador/docsConPalabra"
 * ...
 */

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//creo un contador para sumar todos los documentos donde aparece la palabra
		int docsWithWord = 0;

		//creo un mapa para almacenar cada fichero y contador asociado a la palabra que estoy tratando
        Map<String, Integer> filenameCounterMap = new HashMap<String, Integer>();
		
		// calculo el total de ficheros...
        for (Text val : values) {

        	// como recibo "nombrefichero#contador" divido por el símbolo "#"
    		// y obtengo el array [nombrefichero,contador]
            String[] wordCounter = val.toString().split("#");
            
    		// la palabra en la posición 0 del array [->nombrefichero<-, contador]
            String filename= wordCounter[0];

    		// el contador en la posición 1 del array [palabra, ->contador<-]
            Integer counter = Integer.valueOf(wordCounter[1]);

            // añado al mapa el nombre de fichero y el contador asociado
            filenameCounterMap.put(filename, counter);
            
            // incremento el contador de ficheros donde aparece la palabra
        	docsWithWord += 1;
        }
		
		//para cada "nombrefichero" guardado en el mapa de ficheros/contadores...
        for (String filename : filenameCounterMap.keySet()) {
        	            
        	// obtengo la palabra
        	String word = key.toString();
        	
        	// la clave que voy a emitir es "palabra@nombrefichero"
            String newKey = word + "@" + filename;

        	// obtengo el contador asociado al fichero que estoy tratando
            Integer counter = filenameCounterMap.get(filename);
            
        	// el valor que voy a emitir es "contador1/totalPalabrasEnDoc"
            String newValue = counter + "/" + docsWithWord;
            
            // emito "palabra1@nombrefichero", "contador1/totalPalabrasEnDoc"
            context.write(new Text(newKey), new Text(newValue));
        }

	}
}