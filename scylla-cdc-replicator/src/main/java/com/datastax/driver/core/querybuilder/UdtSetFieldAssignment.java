package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.CodecRegistry;

import java.util.List;

public class UdtSetFieldAssignment extends Assignment.SetAssignment {
    private final String field;
    private final Object value;

    public UdtSetFieldAssignment(String name, String field, Object value) {
        super(name, value);
        this.field = field;
        this.value = value;
    }

    @Override
    void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
        Utils.appendName(this.name, codecRegistry, sb).append('.');
        sb.append(this.field);
        sb.append("=");
        Utils.appendValue(this.value, codecRegistry, sb, variables);
    }

    @Override
    boolean containsBindMarker() {
        return Utils.containsBindMarker(this.value);
    }

    @Override
    boolean isIdempotent() {
        return true;
    }
}