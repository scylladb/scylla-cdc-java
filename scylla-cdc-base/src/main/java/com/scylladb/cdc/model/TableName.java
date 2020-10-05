package com.scylladb.cdc.model;

import java.util.Objects;

import com.google.common.base.Preconditions;

public final class TableName implements Comparable<TableName> {
    public final String keyspace;
    public final String name;

    public TableName(String keyspace, String name) {
        this.keyspace = Preconditions.checkNotNull(keyspace);
        this.name = Preconditions.checkNotNull(name);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TableName && keyspace.equals(((TableName) o).keyspace) && name.equals(((TableName) o).name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, name);
    }

    @Override
    public String toString() {
        return String.format("TableName(%s, %s)", keyspace, name);
    }

    @Override
    public int compareTo(TableName o) {
        int cmp = keyspace.compareTo(o.keyspace);
        return cmp != 0 ? cmp : name.compareTo(o.name);
    }
}
