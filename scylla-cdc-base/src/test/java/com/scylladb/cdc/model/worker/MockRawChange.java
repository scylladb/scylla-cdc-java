package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.Field;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class MockRawChange implements RawChange {
    private final ChangeSchema changeSchema;
    private final Map<String, Object> columnValues;

    public MockRawChange(ChangeSchema changeSchema, Map<String, Object> columnValues) {
        this.changeSchema = Preconditions.checkNotNull(changeSchema);
        this.columnValues = Preconditions.checkNotNull(columnValues);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ChangeSchema changeSchema;
        private final Map<String, Object> columnValues = new HashMap<String, Object>() {{
            put("cdc$batch_seq_no", 0);
            put("cdc$end_of_batch", true);
        }};

        public Builder withChangeSchema(ChangeSchema changeSchema) {
            this.changeSchema = changeSchema;
            return this;
        }

        public Builder withStreamId(ByteBuffer streamId) {
            this.columnValues.put("cdc$stream_id", streamId);
            return this;
        }

        public Builder withStreamId(String streamId) {
            byte[] parsedBytes = BaseEncoding.base16().decode(streamId.replace("0x", "").toUpperCase());
            return withStreamId(ByteBuffer.wrap(parsedBytes));
        }

        public Builder withTime(UUID timeuuid) {
            this.columnValues.put("cdc$time", timeuuid);
            return this;
        }

        public Builder withTime(String timeuuid) {
            return withTime(UUID.fromString(timeuuid));
        }

        public Builder withOperation(byte operation) {
            this.columnValues.put("cdc$operation", operation);
            return this;
        }

        public Builder withOperation(RawChange.OperationType operation) {
            return withOperation(operation.operationId);
        }

        public Builder withBatchSequenceNumber(int batchSequenceNumber) {
            this.columnValues.put("cdc$batch_seq_no", batchSequenceNumber);
            return this;
        }

        public Builder withEndOfBatch(boolean isEndOfBatch) {
            this.columnValues.put("cdc$end_of_batch", isEndOfBatch);
            return this;
        }

        public Builder withTTL(long ttl) {
            this.columnValues.put("cdc$ttl", ttl);
            return this;
        }

        public Builder addPrimaryKey(String columnName, Object value) {
            this.columnValues.put(columnName, value);
            return this;
        }

        public Builder addAtomicRegularColumn(String columnName, Object value) {
            this.columnValues.put(columnName, value);
            if (value == null) {
                this.columnValues.put("cdc$deleted_" + columnName, true);
            }
            return this;
        }

        private Field makeField(Object value, ChangeSchema.DataType type) {
            return new Field() {
                @Override
                public Object getAsObject() {
                    return value;
                }

                @Override
                public ChangeSchema.DataType getDataType() {
                    return type;
                }
            };
        }

        public Builder addFrozenSetRegularColumn(String columnName, Set<Object> set, ChangeSchema.DataType setDataType) {
            Set<Field> transformedSet = null;
            if (set != null) {
                transformedSet = set.stream().map(e -> makeField(e, setDataType)).collect(Collectors.toSet());
            }
            return addAtomicRegularColumn(columnName, transformedSet);
        }

        public Builder addFrozenListRegularColumn(String columnName, List<Object> list, ChangeSchema.DataType listDataType) {
            List<Field> transformedList = null;
            if (list != null) {
                transformedList = list.stream().map(e -> makeField(e, listDataType)).collect(Collectors.toList());
            }
            return addAtomicRegularColumn(columnName, transformedList);
        }

        public Builder addNonfrozenSetRegularColumnOverwrite(String columnName, Set<Object> set, ChangeSchema.DataType setDataType) {
            Set<Field> transformedSet = null;
            if (set != null) {
                transformedSet = set.stream().map(e -> makeField(e, setDataType)).collect(Collectors.toSet());
            }

            this.columnValues.put(columnName, transformedSet);
            this.columnValues.put("cdc$deleted_" + columnName, true);
            // cdc$deleted_elements_ is not set.
            return this;
        }

        public Builder addNonfrozenListRegularColumnOverwrite(String columnName, List<Object> list, ChangeSchema.DataType listDataType) {
            List<Field> transformedList = null;
            if (list != null) {
                transformedList = list.stream().map(e -> makeField(e, listDataType)).collect(Collectors.toList());
            }

            this.columnValues.put(columnName, transformedList);
            this.columnValues.put("cdc$deleted_" + columnName, true);
            // cdc$deleted_elements_ is not set.
            return this;
        }

        public MockRawChange build() {
            return new MockRawChange(changeSchema, columnValues);
        }
    }

    @Override
    public ChangeSchema getSchema() {
        return changeSchema;
    }

    @Override
    public Object getAsObject(ChangeSchema.ColumnDefinition c) {
        return columnValues.get(c.getColumnName());
    }

    @Override
    public Cell getCell(ChangeSchema.ColumnDefinition c) {
        return new Cell() {
            @Override
            public ChangeSchema.ColumnDefinition getColumnDefinition() {
                return c;
            }

            @Override
            public ByteBuffer getAsUnsafeBytes() {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public Object getAsObject() {
                return MockRawChange.this.getAsObject(c);
            }

            @Override
            public ChangeSchema.DataType getDataType() {
                return c.getCdcLogDataType();
            }
        };
    }

    @Override
    public boolean isNull(ChangeSchema.ColumnDefinition c) {
        return getAsObject(c) == null;
    }

    @Override
    public ByteBuffer getAsUnsafeBytes(ChangeSchema.ColumnDefinition c) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MockRawChange cells = (MockRawChange) o;
        return changeSchema.equals(cells.changeSchema) &&
                columnValues.equals(cells.columnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeSchema, columnValues);
    }
}
