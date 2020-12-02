package com.datastax.driver.core.querybuilder;

import static com.datastax.driver.core.querybuilder.Utils.appendName;
import static com.datastax.driver.core.querybuilder.Utils.appendValue;

import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.querybuilder.Assignment.ListSetIdxAssignment;

public class ListSetIdxTimeUUIDAssignment extends ListSetIdxAssignment { //extends Assignment {

    private final UUID idx;
    private final Object value;

    public ListSetIdxTimeUUIDAssignment(String name, UUID idx, Object value) {
        super(name, 0, value);
        this.idx = idx;
        this.value = value;
    }

    @Override
    void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
        appendName(name, codecRegistry, sb).append("[scylla_timeuuid_list_index(").append(idx).append(")]=");
        appendValue(value, codecRegistry, sb, variables);
    }

    @Override
    boolean containsBindMarker() {
        return Utils.containsBindMarker(value);
    }

    @Override
    boolean isIdempotent() {
        return true;
    }

}