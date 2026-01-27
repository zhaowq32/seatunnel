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

    public static final String CHECKPOINT_KEY = "checkpoint";
    public static final String TIMESTAMP_KEY = "timestamp";

    public static final OceanBaseOffset INITIAL_OFFSET = new OceanBaseOffset(0L, "0");
    public static final OceanBaseOffset NO_STOPPING_OFFSET =
            new OceanBaseOffset(Long.MAX_VALUE, String.valueOf(Long.MAX_VALUE));

    public OceanBaseOffset(Map<String, String> offset) {
        this.offset = offset;
    }

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

    public Long getTimestamp() {
        String timestampStr = offset.get(TIMESTAMP_KEY);
        return timestampStr != null ? Long.parseLong(timestampStr) : null;
    }

    public String getCheckpoint() {
        return offset.get(CHECKPOINT_KEY);
    }

    @Override
    public int compareTo(Offset o) {
        if (o == null) {
            return 1;
        }

        OceanBaseOffset that = (OceanBaseOffset) o;

        // Compare NO_STOPPING_OFFSET
        if (NO_STOPPING_OFFSET.equals(this) && NO_STOPPING_OFFSET.equals(that)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        // Compare based on timestamp
        Long thisTimestamp = this.getTimestamp();
        Long thatTimestamp = that.getTimestamp();

        if (thisTimestamp != null && thatTimestamp != null) {
            int timestampCmp = thisTimestamp.compareTo(thatTimestamp);
            if (timestampCmp != 0) {
                return timestampCmp;
            }
        }

        // If timestamps are equal or null, compare checkpoints
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
