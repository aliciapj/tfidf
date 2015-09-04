Tf-idf(del inglés Termfrequency-Inversedocumentfrequency), frecuencia de término -frecuencia inversa de documento (o sea, la frecuencia de ocurrencia del término en la colección de documentos), es una medida numérica que expresa cuán relevante es una palabra para un documento en una colección. Esta medida se utiliza a menudo como un factor de ponderación en la recuperación de información y la minería de texto. El valor tf-idfaumenta proporcionalmente al número de veces que una palabra aparece en el documento, pero es compensada por la frecuencia de la palabra en la colección de documentos, lo que permite manejar el hecho de que algunas palabras son generalmente más comunes que otras.

Ejecución del algortimo tf*idf: 
hadoop jar tfidf.jar es.mbit.tf_idf.TfIdfDriver /tmp/movies /tmp/salidaok

Ejecución parcial del primer proceso: 
hadoop jar tfidf.jar es.mbit.tf_idf.proc1tf.WordFreqDriver /tmp/movies /tmp/mov_result

Ejecución parcial del segundo proceso: 
hadoop jar tfidf.jar es.mbit.tf_idf.proc2wc.WordCountDriver /tmp/mov_result /tmp/mov_result1

Ejecución parcial del tercer proceso: 
hadoop jar tfidf.jar es.mbit.tf_idf.proc3idf.IdfDriver /tmp/mov_result1 /tmp/mov_result2 /tmp/movies
