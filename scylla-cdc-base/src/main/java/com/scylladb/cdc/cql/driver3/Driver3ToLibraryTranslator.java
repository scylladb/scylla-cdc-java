package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.scylladb.cdc.model.cql.Field;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Driver3ToLibraryTranslator {
    public static Object translate(Object driverObject, ChangeSchema.DataType dataType) {
        // Some types returned by getObject() are
        // some classes of Java Driver. We should
        // translate them into a non-Java-Driver-specific
        // types. (for example UDTValue)

        if (driverObject == null) {
            return null;
        }

        // Even though List, Map, Set are
        // a standard Java library types,
        // they can contain values that
        // are Java-Driver-specific.
        //
        // For example: map<int, some_udt>
        //
        // Moreover, we will box them into Field.
        // Recursively call translateObject on their elements.

        // TODO - use dataType instead of instanceof
        if (driverObject instanceof List) {
            List<Object> driverList = (List<Object>) driverObject;
            ChangeSchema.DataType innerType = dataType.getTypeArguments().get(0);

            return driverList.stream().map(o -> translate(o, innerType))
                    .map(o -> new Field(innerType, o)).collect(Collectors.toList());
        } else if (driverObject instanceof Set) {
            Set<Object> driverSet = (Set<Object>) driverObject;
            ChangeSchema.DataType innerType = dataType.getTypeArguments().get(0);

            // Deliberately using LinkedHashSet to preserve the same order as driverObject.
            return driverSet.stream().map(o -> translate(o, innerType))
                    .map(o -> new Field(innerType, o)).collect(Collectors.toCollection(LinkedHashSet::new));
        } else if (driverObject instanceof Map) {
            Map<Object, Object> driverMap = (Map<Object, Object>) driverObject;
            ChangeSchema.DataType keyType = dataType.getTypeArguments().get(0);
            ChangeSchema.DataType valueType = dataType.getTypeArguments().get(1);

            // Deliberately using LinkedHashMap to preserve the same order as driverObject.
            Map<Field, Field> translatedMap = new LinkedHashMap<>();
            for (Map.Entry<Object, Object> entry : driverMap.entrySet()) {
                Field key = new Field(keyType, translate(entry.getKey(), keyType));
                Field value = new Field(valueType, translate(entry.getValue(), valueType));
                translatedMap.put(key, value);
            }
            return translatedMap;
        }

        // Java-Driver-specific types
        if (driverObject instanceof UDTValue) {
            UDTValue driverUDT = (UDTValue) driverObject;

            Map<String, Field> translatedMap = new LinkedHashMap<>();
            for (Map.Entry<String, ChangeSchema.DataType> entry : dataType.getUdtType().getFields().entrySet()) {
                Object translatedObject = translate(driverUDT.getObject(entry.getKey()), entry.getValue());
                Field field = new Field(entry.getValue(), translatedObject);
                translatedMap.put(entry.getKey(), field);
            }
            return translatedMap;
        } else if (driverObject instanceof TupleValue) {
            TupleValue driverTuple = (TupleValue) driverObject;
            List<Field> translatedTuple = new ArrayList<>();
            int idx = 0;
            for (ChangeSchema.DataType fieldType : dataType.getTypeArguments()) {
                Object translatedObject = translate(driverTuple.getObject(idx), fieldType);
                translatedTuple.add(new Field(fieldType, translatedObject));
                idx++;
            }
            return translatedTuple;
        } else if (driverObject instanceof com.datastax.driver.core.Duration) {
            com.datastax.driver.core.Duration driverDuration = (com.datastax.driver.core.Duration) driverObject;
            return new RawChange.Duration(driverDuration.getMonths(), driverDuration.getDays(), driverDuration.getNanoseconds());
        } else if (driverObject instanceof LocalDate) {
            LocalDate driverDate = (LocalDate) driverObject;
            return new RawChange.CqlDate(driverDate.getYear(), driverDate.getMonth(), driverDate.getDay());
        }

        // No translation needed, the object is
        // already of a good type.
        return driverObject;
    }
}
