import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;
import java.util.Random;

public class KafkaProducertest {

    public static void main(String[] args) {


        String topics="tp10074_kj_xsmd_batch01";

        String brokers="10.4.70.194:9092,10.4.70.195:9092,10.4.70.196:9092,10.4.72.87:9092,10.4.72.88:9092,10.4.72.89:9092,10.4.72.90:9092,10.4.72.91:9092,10.4.72.92:9092,10.4.72.93:9092,10.4.72.94:9092,10.4.72.95:9092,10.4.72.96:9092,10.4.72.97:9092,10.4.72.98:9092,10.4.72.99:9092";

        if("test".equals(args[0])){
            brokers="10.4.66.91:9092,10.4.66.92:9092,10.4.66.93:9092,10.4.66.94:9092,10.4.66.95:9092";
        }

        String truststorePath = args[1];
        String keystorePath = args[2];

        System.out.println("truststorePath:"+truststorePath);
        System.out.println("keystorePath:"+keystorePath);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        //configure the following three settings for SSL Encryption
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "tUCoKGLZiY7Bkjr4");
        // configure the following three settings for SSL Authentication
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "tUCoKGLZiY7Bkjr4");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "tUCoKGLZiY7Bkjr4");

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 1; i < 100 ; i++) {
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(
                    topics, "key-" + i, "message-"+(100-i) );
            producer.send(data);
            System.out.println("发送成功:"+i);
        }

        producer.close();

    }
}
