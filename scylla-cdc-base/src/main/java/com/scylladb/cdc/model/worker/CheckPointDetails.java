package com.scylladb.cdc.model.worker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class CheckPointDetails {
    private Integer id;
    private String instanceName;
    private long lastReadTimestamp;
}
