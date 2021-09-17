package com.netflix.ice.reader;

import com.netflix.ice.processor.TagCoverageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeSeriesTagCoverageMetrics {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private int size;
    private short[] len;
    private TagCoverageMetrics[] metrics;

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
}
