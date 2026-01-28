package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.fetch;

import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * OceanBase CDC 记录封装类
 *
 * <p>该类将 OceanBase LogProxy 返回的 {@link LogMessage} 封装为更易用的 CDC 记录对象。
 *
 * <p>主要功能:
 *
 * <ul>
 *   <li>将 LogMessage 中的字段按照 before/after 进行分类存储
 *   <li>提供统一的字段值提取方法
 *   <li>处理字段编码转换（如 binary 编码转 UTF-8）
 * </ul>
 *
 * <p><strong>重要提示</strong>: 该类使用的是 {@code com.oceanbase.oms.logmessage} 包下的新 API, 而非已废弃的 {@code
 * com.oceanbase.clogproxy.client.logmessage} 包。
 */
@Getter
public class OceanBaseCDCRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数据库名称
     *
     * <p>来自 LogMessage 的 dbName,格式为 'tenant.db'。
     */
    private final String database;

    /** 表名称 */
    private final String table;

    /** 操作类型（INSERT/UPDATE/DELETE/BEGIN/COMMIT/HEARTBEAT） */
    private final DataMessage.Record.Type type;

    /**
     * 时间戳（秒）
     *
     * <p>注意: LogMessage.getTimestamp() 返回的是 String 类型,需要转换为 long。
     */
    private final long timestamp;

    /**
     * UPDATE/DELETE 操作的旧值字段（before image）
     *
     * <p>Key: 字段名, Value: 字段对象
     *
     * <p>通过 field.isPrev() == true 来判断。
     */
    private final Map<String, DataMessage.Record.Field> fieldsBefore;

    /**
     * INSERT/UPDATE 操作的新值字段（after image）
     *
     * <p>Key: 字段名, Value: 字段对象
     *
     * <p>通过 field.isPrev() == false 来判断。
     */
    private final Map<String, DataMessage.Record.Field> fieldsAfter;

    /**
     * 构造函数,从 LogMessage 解析 CDC 记录
     *
     * <p>主要处理:
     *
     * <ol>
     *   <li>提取基本元数据（库名、表名、操作类型、时间戳）
     *   <li>将 LogMessage 的字段列表按照 isPrev() 分为 before 和 after 两组
     *   <li>使用 Map 存储,自动去重（避免 Schema 构建时的字段名重复错误）
     * </ol>
     *
     * @param logMessage OceanBase LogProxy 返回的原始日志消息
     */
    public OceanBaseCDCRecord(LogMessage logMessage) {
        // 提取元数据
        this.database = logMessage.getDbName();
        this.table = logMessage.getTableName();
        this.type = logMessage.getOpt();
        this.timestamp = Long.parseLong(logMessage.getTimestamp());
        this.fieldsBefore = new HashMap<>();
        this.fieldsAfter = new HashMap<>();

        // 将字段列表按 isPrev() 分类存储
        // isPrev()==true 表示 UPDATE/DELETE 的旧值
        // isPrev()==false 表示 INSERT/UPDATE 的新值
        for (DataMessage.Record.Field field : logMessage.getFieldList()) {
            if (field.isPrev()) {
                fieldsBefore.put(field.getFieldname(), field);
            } else {
                fieldsAfter.put(field.getFieldname(), field);
            }
        }
    }

    /**
     * 将字段 Map 转换为值 Map
     *
     * <p>将 {@code Map<String, Field>} 转换为 {@code Map<String, String>}, 便于 Debezium 格式的数据封装。
     *
     * <p>处理逻辑:
     *
     * <ul>
     *   <li>如果字段值为 null,则 Map 中存储 null
     *   <li>如果编码为 "binary",则使用 utf8 解码
     *   <li>否则使用字段自带的编码方式解码
     * </ul>
     *
     * @param fields 原始字段 Map
     * @return 字段名-字段值（字符串）的 Map
     */
    @SuppressWarnings("rawtypes")
    public static Map toValueMap(Map<String, DataMessage.Record.Field> fields) {
        Map<String, String> map = new HashMap<>();
        fields.forEach(
                (name, field) -> {
                    if (field.getValue() == null) {
                        map.put(name, null);
                    } else {
                        // 特殊处理 binary 编码,强制使用 utf8 解码
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
