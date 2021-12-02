/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeSeriesData {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private int size;
    private short[] len;
    private double[] cost;
    private double[] usage;

    public enum Type {
        COST,
        USAGE;
    }

    private TimeSeriesData(int size, short[] len, double[] cost, double[] usage) {
        this.size = size;
        this.len = len;
        this.cost = cost;
        this.usage = usage;
    }

    public TimeSeriesData(double[] cost, double[] usage) {
        size = cost.length;
        short[] tmpLen = new short[size];

        // Scan the values to build the lengths array
        int chunkIndex = 0;
        double lastCost = cost[0];
        double lastUsage = usage[0];
        tmpLen[chunkIndex] = 1;
        for (int i = 1; i < size; i++) {
            // TODO: consider fuzzy equality check
            if (cost[i] == lastCost && usage[i] == lastUsage) {
                tmpLen[chunkIndex]++;
            } else {
                chunkIndex++;
                tmpLen[chunkIndex] = 1;
                lastCost = cost[i];
                lastUsage = usage[i];
            }
        }

        // Load the RLE data
        int chunks = chunkIndex + 1;
        this.len = new short[chunks];
        this.cost = new double[chunks];
        this.usage = new double[chunks];
        int offset = 0;

        for (int i = 0; i < chunks; i++) {
            this.len[i] = tmpLen[i];
            this.cost[i] = cost[offset];
            this.usage[i] = usage[offset];
            offset += this.len[i];
        }
    }

    public int size() {
        return size;
    }

    public void get(Type type, int start, int count, double[] dest) {
        if (start >= size) {
            for (int i = 0; i < count; i++)
                dest[i] = 0;
            return;
        }

        // Find start of requested data
        // TODO: Improve scheme for finding the start
        int index = 0;
        int indexStart = 0;
        while (start >= indexStart + len[index]) {
            indexStart += len[index];
            index++;
        }

        // Copy range
        double[] source = type == Type.COST ? cost : usage;
        int offset = 0;
        int chunkIndex = start - indexStart;
        for (int i = 0; i < count; i++) {
            dest[i] = source[index];
            chunkIndex++;
            if (chunkIndex >= len[index]) {
                chunkIndex = 0;
                indexStart += len[index];
                index++;

                if (index >= len.length) {
                    // ran off the end, fill with zeros
                    for (i++; i < count; i++) {
                        dest[i] = 0;
                    }
                }
            }
        }
    }

    /**
     * Serialize data using standard Java serialization DataOutput methods in the following order:<br/>
     *
     * 1. size - number of data intervals (int)<br/>
     * 2. num - number of RLE chunks (int)<br/>
     * 3. chunks - array of chunk data
     *      3a. len (int)<br/>
     *      3b. cost (double)<br/>
     *      3c. usage (double)<br/>
     */
    public void serialize(DataOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(len.length);
        for (int i = 0; i < len.length; i++) {
            out.writeShort(len[i]);
            out.writeDouble(cost[i]);
            out.writeDouble(usage[i]);
        }
    }

    public static TimeSeriesData deserialize(DataInput in) throws IOException {
        int size = in.readInt();
        int numChunks = in.readInt();
        short[] len = new short[numChunks];
        double[] cost = new double[numChunks];
        double[] usage = new double[numChunks];
        for (int i = 0; i < numChunks; i++) {
            len[i] = in.readShort();
            cost[i] = in.readDouble();
            usage[i] = in.readDouble();
        }
        return new TimeSeriesData(size, len, cost, usage);
    }
}
