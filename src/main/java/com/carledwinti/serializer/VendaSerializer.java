package com.carledwinti.serializer;

import com.carledwinti.model.Venda;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class VendaSerializer implements Serializer<Venda> {

    @Override
    public byte[] serialize(String topic, Venda venda) {

        try {
            byte[] vendaInBytes = new ObjectMapper().writeValueAsBytes(venda);
            return vendaInBytes;
        } catch (JsonProcessingException e) {
            System.err.println(e.getMessage());
        }catch (Exception e){
            System.err.println(e.getMessage());
        }
        return null;
    }
}
