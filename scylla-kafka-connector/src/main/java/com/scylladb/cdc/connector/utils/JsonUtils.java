package com.scylladb.cdc.connector.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonUtils {
    private JsonUtils(){}

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public static <T> T fromJson(String jsonStr, Class<T> klass) throws IOException {
        return OBJECT_MAPPER.readValue(jsonStr,klass);
    }

    public static <T> T fromJson(String jsonStr, TypeReference<T> typeReference) throws IOException {
        return OBJECT_MAPPER.readValue(jsonStr,typeReference);
    }

    public static <T> String toJson(T object) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(object);
    }
}
