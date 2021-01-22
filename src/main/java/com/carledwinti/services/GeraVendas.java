package com.carledwinti.services;

import com.carledwinti.model.Venda;
import com.carledwinti.serializer.VendaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;

public class GeraVendas {

    private static Random random = new Random();
    private static long operacao =0l;
    private static BigDecimal valorIngresso = BigDecimal.valueOf(500);
    private static String topic = "venda-ingressos";

    public static void geraVenda(){
        long cliente = random.nextLong();
        //bound/limite
        int qtdIngressos = random.nextInt(10);

        //cmd --> kafka-topics --bootstrap-server localhost:9093 --create --topic venda-ingressos
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VendaSerializer.class.getName());

        try (KafkaProducer<String, Venda> kafkaProducer = new KafkaProducer<>(properties)) {

            while (true) {
                Venda venda = new Venda(operacao++, cliente, qtdIngressos, valorIngresso.multiply(BigDecimal.valueOf(qtdIngressos)));
                ProducerRecord<String, Venda> vendaProducerRecord = new ProducerRecord<>(topic, venda);
                kafkaProducer.send(vendaProducerRecord);
                Thread.sleep(200);//esperando um tempo
                System.out.println("Venda com numero de operacao: " + operacao + " enviada com sucesso!");
            }
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }catch (Exception e){
            System.err.println(e.getMessage());
        }
    }
}
