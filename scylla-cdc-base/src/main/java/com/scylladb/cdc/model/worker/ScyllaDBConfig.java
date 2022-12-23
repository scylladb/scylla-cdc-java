package com.scylladb.cdc.model.worker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author buntykumar
 * @version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScyllaDBConfig {
    private String host;
    private Integer port;
    private String userName;
    private String scPassword;
}
