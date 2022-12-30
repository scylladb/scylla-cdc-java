package com.scylladb.cdc.connector.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {
    private JsonUtils(){}

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
}
