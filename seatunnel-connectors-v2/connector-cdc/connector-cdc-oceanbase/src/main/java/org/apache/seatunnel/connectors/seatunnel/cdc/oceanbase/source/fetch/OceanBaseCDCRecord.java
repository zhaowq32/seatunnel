package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.fetch;

import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
public class OceanBaseCDCRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The database name in log message, would be in format 'tenant.db'. */
    private final String database;

    private final String table;
    private final DataMessage.Record.Type type;

    /** Timestamp in seconds. */
    private final long timestamp;

    private final Map<String, DataMessage.Record.Field> fieldsBefore;
    private final Map<String, DataMessage.Record.Field> fieldsAfter;

    public OceanBaseCDCRecord(LogMessage logMessage) {
        this.database = logMessage.getDbName();
        this.table = logMessage.getTableName();
        this.type = logMessage.getOpt();
        this.timestamp = Long.parseLong(logMessage.getTimestamp());
        this.fieldsBefore = new HashMap<>();
        this.fieldsAfter = new HashMap<>();

        for (DataMessage.Record.Field field : logMessage.getFieldList()) {
            if (field.isPrev()) {
                fieldsBefore.put(field.getFieldname(), field);
            } else {
                fieldsAfter.put(field.getFieldname(), field);
            }
        }
    }

    /**
     * Convert field map to 'field name'->'field value (string)'
     *
     * @param fields Original field map.
     * @return Field value map.
     */
    @SuppressWarnings("rawtypes")
    public static Map toValueMap(Map<String, DataMessage.Record.Field> fields) {
        Map<String, String> map = new HashMap<>();
        fields.forEach(
                (name, field) -> {
                    if (field.getValue() == null) {
                        map.put(name, null);
                    } else {
                        if ("binary".equalsIgnoreCase(field.getEncoding())) {
                            map.put(name, field.getValue().toString("utf8"));
                        } else {
                            map.put(name, field.getValue().toString(field.getEncoding()));
                        }
                    }
                });
        return map;
    }
}
