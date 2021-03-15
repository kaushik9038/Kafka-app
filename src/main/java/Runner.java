import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

public class Runner {
    public static void main(String[] args) throws Exception {

        String line = "";
        String splitBy = ",";
        try {
//parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader("/home/kaushik/Desktop/Kafka/test19.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {

                String[] port = line.split(splitBy);
                for (String s : port) {
                    System.out.println(s);
                    extracted(s);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private static void extracted(String singleValue) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ProducerRecord producerRecord = new ProducerRecord("channel_1", "test", singleValue);
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        kafkaProducer.send(producerRecord);
        kafkaProducer.close();




    }

}