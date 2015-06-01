Realizar una aplicación MapReduce para el siguiente algoritmo:

documentoTf-idf(del inglés Termfrequency-Inversedocumentfrequency), frecuencia de término -frecuencia inversa de documento (o sea, la frecuencia de ocurrencia del término en la colección de documentos), es una medida numérica que expresa cuán relevante es una palabra para un documento en una colección. Esta medida se utiliza a menudo como un factor de ponderación en la recuperación de información y la minería de texto. El valor tf-idfaumenta proporcionalmente al número de veces que una palabra aparece en el documento, pero es compensada por la frecuencia de la palabra en la colección de documentos, lo que permite manejar el hecho de que algunas palabras son generalmente más comunes que otras.

En la plataforma exite una carpeta con más infor y con el dataset.

TF-IDF: Material a entregar:

-Paquete .jarcon el código compilado del programa

-Clases .java con el código del programa

-Fichero de resultado con las palabras (géneros) que más se repiten en las colecciones.

-El programa debe poder ejecutarse con la estructura de la siguiente instrucción:


hadoopjartfidf.jar nombreClasedriver directorioEntradaColeccionesdirectorioSalida


# ejecución del algortimo tf*idf
hadoop jar tfidf.jar es.mbit.tf_idf.TfIdfDriver /tmp/movies /tmp/salidaok

# ejecución parcial del primer proceso
hadoop jar tfidf.jar es.mbit.tf_idf.proc1tf.WordFreqDriver /tmp/movies /tmp/mov_result

# ejecución parcial del segundo proceso
hadoop jar tfidf.jar es.mbit.tf_idf.proc2wc.WordCountDriver /tmp/mov_result /tmp/mov_result1

# ejecución parcial del tercer proceso
hadoop jar tfidf.jar es.mbit.tf_idf.proc3idf.IdfDriver /tmp/mov_result1 /tmp/mov_result2 /tmp/movies

