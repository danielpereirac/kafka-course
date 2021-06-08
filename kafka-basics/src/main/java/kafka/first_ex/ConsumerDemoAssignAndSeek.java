package kafka.first_ex;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {
        System.out.println("Hello World");

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        //String group_id = "new_consumer_demo_application";
        String topic ="first_topic";

        //consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create the consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        //subscribe consume to my topic
        //consumer.subscribe(Collections.singleton(topic));

        //assign and seek are mostly use to replay data or fetch

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offSetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offSetToReadFrom);

        int  numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;
        while (keepOnReading)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records)
            {
                numberOfMessageReadSoFar +=1;
                logger.info( "Key: " + record.key() + "\n"
                        + "Value: " + record.value() + "\n"
                        + "Partition: " + record.partition() + "\n"
                        + "Offset: " + record.offset() + "\n");

                if(numberOfMessageReadSoFar>numberOfMessagesToRead){
                    keepOnReading= false;
                    break;
                }
            }

        }

        logger.info("Exiting the application");
    }
}
