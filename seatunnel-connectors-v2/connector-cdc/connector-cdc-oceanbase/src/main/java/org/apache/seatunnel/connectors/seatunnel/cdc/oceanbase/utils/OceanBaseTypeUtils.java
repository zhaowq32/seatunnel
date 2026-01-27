/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.utils;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Utilities for converting from OceanBase types to SeaTunnel types. OceanBase type conversion based
 * on Debezium Column metadata.
 */
@Slf4j
public class OceanBaseTypeUtils {

    // Type name constants for OceanBase
    private static final String OCEANBASE_BIT = "BIT";
    private static final String OCEANBASE_TINYINT = "TINYINT";
    private static final String OCEANBASE_SMALLINT = "SMALLINT";
    private static final String OCEANBASE_MEDIUMINT = "MEDIUMINT";
    private static final String OCEANBASE_INT = "INT";
    private static final String OCEANBASE_INTEGER = "INTEGER";
    private static final String OCEANBASE_BIGINT = "BIGINT";
    private static final String OCEANBASE_FLOAT = "FLOAT";
    private static final String OCEANBASE_DOUBLE = "DOUBLE";
    private static final String OCEANBASE_DECIMAL = "DECIMAL";
    private static final String OCEANBASE_NUMERIC = "NUMERIC";
    private static final String OCEANBASE_CHAR = "CHAR";
    private static final String OCEANBASE_VARCHAR = "VARCHAR";
    private static final String OCEANBASE_TEXT = "TEXT";
    private static final String OCEANBASE_TINYTEXT = "TINYTEXT";
    private static final String OCEANBASE_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String OCEANBASE_LONGTEXT = "LONGTEXT";
    private static final String OCEANBASE_BINARY = "BINARY";
    private static final String OCEANBASE_VARBINARY = "VARBINARY";
    private static final String OCEANBASE_BLOB = "BLOB";
    private static final String OCEANBASE_TINYBLOB = "TINYBLOB";
    private static final String OCEANBASE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String OCEANBASE_LONGBLOB = "LONGBLOB";
    private static final String OCEANBASE_DATE = "DATE";
    private static final String OCEANBASE_TIME = "TIME";
    private static final String OCEANBASE_DATETIME = "DATETIME";
    private static final String OCEANBASE_TIMESTAMP = "TIMESTAMP";
    private static final String OCEANBASE_YEAR = "YEAR";
    private static final String OCEANBASE_ENUM = "ENUM";
    private static final String OCEANBASE_SET = "SET";
    private static final String OCEANBASE_JSON = "JSON";

    /** Convert Debezium column to SeaTunnel data type. */
    public static SeaTunnelDataType<?> convertFromColumn(
            Column column, RelationalDatabaseConnectorConfig dbzConnectorConfig) {
        return convertToSeaTunnelColumn(column, dbzConnectorConfig).getDataType();
    }

    /** Convert Debezium column to SeaTunnel column. */
    public static org.apache.seatunnel.api.table.catalog.Column convertToSeaTunnelColumn(
            io.debezium.relational.Column column,
            RelationalDatabaseConnectorConfig dbzConnectorConfig) {

        Optional<String> defaultValueExpression = column.defaultValueExpression();
        Object defaultValue = defaultValueExpression.orElse(null);

        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.name())
                        .columnType(column.typeName())
                        .dataType(column.typeName())
                        .scale(column.scale().orElse(0))
                        .nullable(column.isOptional())
                        .defaultValue(defaultValue);

        if (column.length() >= 0) {
            builder.length((long) column.length()).precision((long) column.length());
        }

        // Build the BasicTypeDefine and convert to SeaTunnel column
        return convertBasicTypeDefine(builder.build());
    }

    /** Convert BasicTypeDefine to SeaTunnel PhysicalColumn */
    private static org.apache.seatunnel.api.table.catalog.Column convertBasicTypeDefine(
            BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String oceanbaseDataType = typeDefine.getDataType().toUpperCase();

        // Handle UNSIGNED types
        boolean isUnsigned = oceanbaseDataType.contains("UNSIGNED");
        oceanbaseDataType = oceanbaseDataType.replace(" UNSIGNED", "").trim();

        switch (oceanbaseDataType) {
            case OCEANBASE_BIT:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 1) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    long byteLength = typeDefine.getLength() / 8;
                    byteLength += typeDefine.getLength() % 8 > 0 ? 1 : 0;
                    builder.columnLength(byteLength);
                }
                break;

            case OCEANBASE_TINYINT:
                if (typeDefine.getColumnType().equalsIgnoreCase("tinyint(1)")) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else if (isUnsigned) {
                    builder.dataType(BasicType.SHORT_TYPE);
                } else {
                    builder.dataType(BasicType.BYTE_TYPE);
                }
                break;

            case OCEANBASE_SMALLINT:
                if (isUnsigned) {
                    builder.dataType(BasicType.INT_TYPE);
                } else {
                    builder.dataType(BasicType.SHORT_TYPE);
                }
                break;

            case OCEANBASE_MEDIUMINT:
            case OCEANBASE_INT:
            case OCEANBASE_INTEGER:
            case OCEANBASE_YEAR:
                if (isUnsigned) {
                    builder.dataType(BasicType.LONG_TYPE);
                } else {
                    builder.dataType(BasicType.INT_TYPE);
                }
                break;

            case OCEANBASE_BIGINT:
                if (isUnsigned) {
                    // BIGINT UNSIGNED -> DECIMAL(20, 0)
                    DecimalType decimalType = new DecimalType(20, 0);
                    builder.dataType(decimalType);
                    builder.columnLength(20L);
                    builder.scale(0);
                } else {
                    builder.dataType(BasicType.LONG_TYPE);
                }
                break;

            case OCEANBASE_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;

            case OCEANBASE_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;

            case OCEANBASE_DECIMAL:
            case OCEANBASE_NUMERIC:
                long precision = typeDefine.getPrecision() != null ? typeDefine.getPrecision() : 10;
                int scale = typeDefine.getScale() != null ? typeDefine.getScale() : 0;
                DecimalType decimalType = new DecimalType((int) precision, scale);
                builder.dataType(decimalType);
                builder.columnLength(precision);
                builder.scale(scale);
                break;

            case OCEANBASE_CHAR:
            case OCEANBASE_VARCHAR:
            case OCEANBASE_ENUM:
            case OCEANBASE_SET:
            case OCEANBASE_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(1L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;

            case OCEANBASE_TINYTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(255L);
                break;

            case OCEANBASE_TEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(65535L);
                break;

            case OCEANBASE_MEDIUMTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(16777215L);
                break;

            case OCEANBASE_LONGTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(4294967295L);
                break;

            case OCEANBASE_BINARY:
            case OCEANBASE_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(1L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;

            case OCEANBASE_TINYBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(255L);
                break;

            case OCEANBASE_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(65535L);
                break;

            case OCEANBASE_MEDIUMBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(16777215L);
                break;

            case OCEANBASE_LONGBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(4294967295L);
                break;

            case OCEANBASE_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;

            case OCEANBASE_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;

            case OCEANBASE_DATETIME:
            case OCEANBASE_TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;

            default:
                log.warn("Unknown OceanBase type: {}, using STRING type", oceanbaseDataType);
                builder.dataType(BasicType.STRING_TYPE);
                break;
        }

        return builder.build();
    }
}
