package com.scylladb.cdc.connector.alerting;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;


/**
 * @author buntykumar
 * @version 1.0
 */

@Getter
@Setter
@AllArgsConstructor
@Builder(builderClassName = "Builder")
public class SlackMessage implements Serializable {
    private String channel;
    private String userName;
    private String text;
    private String iconEmoji;
}
