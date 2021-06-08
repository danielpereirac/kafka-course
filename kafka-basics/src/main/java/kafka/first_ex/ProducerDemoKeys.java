package kafka.first_ex;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys
{
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootsrapServers="127.0.0.1:9092";
        //create kafka properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());



        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i= 0 ; i<10 ; i++) {

            String topic = "first_topic";
            String key = "id_"+ Integer.toString(i);
            String value = "Message with key "+ Integer.toString(i);
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);

            logger.info("Key: " + key);

            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is sucessfullt send or an expeception is thrown

                    if (e == null) {
                        logger.info("Received new metadata \n"
                                + "Topic: " + recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"
                                + "Offset: " + recordMetadata.offset() + "\n"
                                + "Timestamp: " + recordMetadata.timestamp() + "\n"

                        );
                    } else {

                        logger.error("Error While producing: ", e);
                    }
                }
            }).get(); // block sent to make it synch -- dont do in prod
        }


        producer.flush();

        producer.close();

    }
}
