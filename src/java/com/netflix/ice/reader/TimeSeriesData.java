package com.netflix.ice.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}
