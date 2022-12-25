package com.scylladb.cdc.model.worker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class CheckPointDetails {
    private String keySpaceTableNameCombination;
    private long lastReadTimestamp;
}
