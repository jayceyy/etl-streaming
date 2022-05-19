
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Collections;
import java.util.Properties;


public class KafkaConsumerTest {

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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "tUCoKGLZiY7Bkjr4");

        //configure the following three settings for SSL Authentication
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "tUCoKGLZiY7Bkjr4");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "tUCoKGLZiY7Bkjr4");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = null;
        try{
            consumer = new KafkaConsumer<String, String>(props);
            System.out.println("连接成功:consumer==========="+consumer);
        } catch (Exception e){
            System.out.println("连接失败:");
            e.printStackTrace();
        }

        consumer.subscribe(Collections.singletonList(topics));


        boolean flag = true;

        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(5000);

            if(records==null || records.isEmpty()){
                System.out.println("----未拉取到数据----");
            }else {
                if(records.count()>1000){
                    System.out.println("拉取到的数据条数:"+records.count());
                }else {
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                }
            }
            flag = false;
        }


    }

}


