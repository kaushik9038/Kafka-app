import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Runner {
    public static void main(String[] args) throws Exception {

        String line = "";
        String splitBy = ",";
        try {
//parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader("/home/kaushik/Desktop/Kafka/UNSW_NB15_testing-set.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {

                String[] employee = line.split(splitBy);
                for (String s : employee) {
                    extracted(s);
                }

            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }



    private static void extracted(String singleValue) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ProducerRecord producerRecord = new ProducerRecord("channel","selftut" , singleValue);
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }

    public static String csvReader() throws Exception {
        String line = "";
        String splitBy = ",";
        try {
//parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader("/home/kaushik/Desktop/Kafka/UNSW_NB15_testing-set.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {

                String[] employee = line.split(splitBy);
                for (String s : employee) {
                    return s;
                }

            }

    }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}