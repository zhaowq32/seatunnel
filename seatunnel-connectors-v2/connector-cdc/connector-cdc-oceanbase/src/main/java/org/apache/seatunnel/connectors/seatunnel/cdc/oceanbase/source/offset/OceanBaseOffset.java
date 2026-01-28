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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import java.util.HashMap;
import java.util.Map;

/**
 * OceanBase offset based on timestamp checkpoint from LogProxy. The checkpoint format is typically
 * "timestamp@sequence"
 */
public class OceanBaseOffset extends Offset {

    private static final long serialVersionUID = 1L;

    /** 偏移量 Map 中 checkpoint 的 key */
    public static final String CHECKPOINT_KEY = "checkpoint";

    /** 偏移量 Map 中 timestamp 的 key */
    public static final String TIMESTAMP_KEY = "timestamp";

    /**
     * 初始偏移量,表示从最早的时间点开始
     *
     * <p>时间戳为 0,checkpoint 为 "0"。
     */
    public static final OceanBaseOffset INITIAL_OFFSET = new OceanBaseOffset(0L, "0");

    /**
     * 无停止偏移量,表示持续读取直到任务被取消
     *
     * <p>时间戳为 Long.MAX_VALUE。
     */
    public static final OceanBaseOffset NO_STOPPING_OFFSET =
            new OceanBaseOffset(Long.MAX_VALUE, String.valueOf(Long.MAX_VALUE));

    /**
     * 从 Map 构造偏移量
     *
     * @param offset 包含 timestamp 和 checkpoint 的 Map
     */
    public OceanBaseOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    /**
     * 从时间戳和 checkpoint 构造偏移量
     *
     * @param timestamp 时间戳（秒）
     * @param checkpoint LogProxy 返回的 checkpoint 字符串,通常格式为 "timestamp@sequence"
     */
    public OceanBaseOffset(Long timestamp, String checkpoint) {
        Map<String, String> offsetMap = new HashMap<>();
        if (timestamp != null) {
            offsetMap.put(TIMESTAMP_KEY, String.valueOf(timestamp));
        }
        if (checkpoint != null) {
            offsetMap.put(CHECKPOINT_KEY, checkpoint);
        }
        this.offset = offsetMap;
    }

    /**
     * 获取时间戳（秒）
     *
     * @return 时间戳,如果不存在则返回 null
     */
    public Long getTimestamp() {
        String timestampStr = offset.get(TIMESTAMP_KEY);
        return timestampStr != null ? Long.parseLong(timestampStr) : null;
    }

    /**
     * 获取 checkpoint 字符串
     *
     * @return checkpoint 字符串
     */
    public String getCheckpoint() {
        return offset.get(CHECKPOINT_KEY);
    }

    /**
     * 比较两个偏移量的大小
     *
     * <p>比较规则:
     *
     * <ol>
     *   <li>首先比较是否为 NO_STOPPING_OFFSET (最大值)
     *   <li>然后比较 timestamp
     *   <li>如果 timestamp 相等,则比较 checkpoint 字符串
     * </ol>
     *
     * @param o 要比较的偏移量
     * @return 负数表示小于, 0 表示相等, 正数表示大于
     */
    @Override
    public int compareTo(Offset o) {
        if (o == null) {
            return 1;
        }

        OceanBaseOffset that = (OceanBaseOffset) o;

        // 比较 NO_STOPPING_OFFSET
        if (NO_STOPPING_OFFSET.equals(this) && NO_STOPPING_OFFSET.equals(that)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        // 比较 timestamp
        Long thisTimestamp = this.getTimestamp();
        Long thatTimestamp = that.getTimestamp();

        if (thisTimestamp != null && thatTimestamp != null) {
            int timestampCmp = thisTimestamp.compareTo(thatTimestamp);
            if (timestampCmp != 0) {
                return timestampCmp;
            }
        }

        // timestamp 相等或为 null,比较 checkpoint
        String thisCheckpoint = this.getCheckpoint();
        String thatCheckpoint = that.getCheckpoint();

        if (thisCheckpoint != null && thatCheckpoint != null) {
            return thisCheckpoint.compareTo(thatCheckpoint);
        }

        if (thisCheckpoint == null && thatCheckpoint == null) {
            return 0;
        }

        return thisCheckpoint == null ? -1 : 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OceanBaseOffset)) {
            return false;
        }
        OceanBaseOffset that = (OceanBaseOffset) o;
        return offset.equals(that.offset);
    }

    @Override
    public int hashCode() {
        return offset.hashCode();
    }

    @Override
    public String toString() {
        return "OceanBaseOffset{"
                + "timestamp="
                + getTimestamp()
                + ", checkpoint='"
                + getCheckpoint()
                + '\''
                + '}';
    }
}
