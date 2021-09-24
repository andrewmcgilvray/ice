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
package com.netflix.ice.processor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TimeSeriesTagCoverageMetrics;

public class ReadWriteTagCoverageData extends ReadWriteGenericData<TagCoverageMetrics> {
	
    static public Map<TagGroup, TagCoverageMetrics> getCreateData(List<Map<TagGroup, TagCoverageMetrics>> data, int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, TagCoverageMetrics>newHashMap());
            }
        }
        return data.get(i);
    }
    
	public ReadWriteTagCoverageData(int numUserTags) {
		super(numUserTags);
	}

	@Override
	protected void serializeTimeSeriesData(Collection<TagGroup> keys, DataOutput out) throws IOException {
		TagCoverageMetrics metrics[] = new TagCoverageMetrics[data.size()];

		for (TagGroup tagGroup: keys) {
			for (int i = 0; i < data.size(); i++) {
				Map<TagGroup, TagCoverageMetrics> map = getData(i);
				metrics[i] = map.get(tagGroup);
			}
			TimeSeriesTagCoverageMetrics tsd = new TimeSeriesTagCoverageMetrics(metrics);
			tsd.serialize(out);
		}

	}

	@Override
	protected List<Map<TagGroup, TagCoverageMetrics>> deserializeTimeSeriesData(Collection<TagGroup> keys, DataInput in) throws IOException {
		List<Map<TagGroup, TagCoverageMetrics>> data = Lists.newArrayList();
		int num = in.readInt();
		for (int i = 0; i < num; i++)
			data.add(Maps.<TagGroup, TagCoverageMetrics>newHashMap());

		boolean timeSeries = true;
		TagCoverageMetrics metrics[] = new TagCoverageMetrics[num];
		for (TagGroup tagGroup: keys) {
			TimeSeriesTagCoverageMetrics tsm = TimeSeriesTagCoverageMetrics.deserialize(in, numUserTags);
			tsm.get(0, num, metrics);
			for (int i = 0; i < num; i++) {
				if (metrics[i] != null) {
					data.get(i).put(tagGroup, metrics[i]);
				}
			}
		}
		return data;
	}

}
