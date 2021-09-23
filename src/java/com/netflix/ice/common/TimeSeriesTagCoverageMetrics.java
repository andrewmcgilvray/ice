package com.netflix.ice.common;

import com.netflix.ice.processor.TagCoverageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeSeriesTagCoverageMetrics {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private int size;
    private short[] len;
    private TagCoverageMetrics[] metrics;

    private TimeSeriesTagCoverageMetrics(int size, short[] len, TagCoverageMetrics[] metrics) {
        this.size = size;
        this.len = len;
        this.metrics = metrics;
    }

    public TimeSeriesTagCoverageMetrics(TagCoverageMetrics[] data) {
        size = data.length;
        short[] tmpLen = new short[size];

        // Scan the values to build the lengths array
        int chunkIndex = 0;
        TagCoverageMetrics lastMetric = data[0];
        tmpLen[chunkIndex] = 1;
        for (int i = 1; i < size; i++) {
            // TODO: consider fuzzy equality check
            if (data[i].equals(lastMetric)) {
                tmpLen[chunkIndex]++;
            } else {
                chunkIndex++;
                tmpLen[chunkIndex] = 1;
                lastMetric = data[i];
            }
        }

        // Load the RLE data
        int chunks = chunkIndex + 1;
        this.len = new short[chunks];
        this.metrics = new TagCoverageMetrics[chunks];
        int offset = 0;

        for (int i = 0; i < chunks; i++) {
            this.len[i] = tmpLen[i];
            this.metrics[i] = data[offset];
            offset += this.len[i];
        }
    }

    public int size() {
        return size;
    }

    public void get(int start, int count, TagCoverageMetrics[] dest) {
        if (start >= size) {
            for (int i = 0; i < count; i++)
                dest[i] = null;
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
        int offset = 0;
        int chunkIndex = start - indexStart;
        for (int i = 0; i < count; i++) {
            dest[i] = metrics[index];
            chunkIndex++;
            if (chunkIndex >= len[index]) {
                chunkIndex = 0;
                indexStart += len[index];
                index++;

                if (index >= len.length) {
                    // ran off the end, fill with zeros
                    for (i++; i < count; i++) {
                        dest[i] = null;
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
     *      3b. metrics <br/>
     */
    public void serialize(DataOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(len.length);
        for (int i = 0; i < len.length; i++) {
            out.writeShort(len[i]);
            metrics[i].serialize(out);
        }
    }

    public static TimeSeriesTagCoverageMetrics deserialize(DataInput in, int numUserTags) throws IOException {
        int size = in.readInt();
        int numChunks = in.readInt();
        short[] len = new short[numChunks];
        TagCoverageMetrics[] metrics = new TagCoverageMetrics[numChunks];
        for (int i = 0; i < numChunks; i++) {
            len[i] = in.readShort();
            metrics[i] = TagCoverageMetrics.deserialize(in, numUserTags);
        }
        return new TimeSeriesTagCoverageMetrics(size, len, metrics);
    }
}
