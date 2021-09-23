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
package com.netflix.ice.reader;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Maps;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TimeSeriesTagCoverageMetrics;

public class ReadOnlyTagCoverageData extends ReadOnlyGenericData<TimeSeriesTagCoverageMetrics> {

	public ReadOnlyTagCoverageData(int numUserTags) {
		super(Maps.<TagGroup, TimeSeriesTagCoverageMetrics>newHashMap(), numUserTags, 0);
	}

 	@Override
	protected void deserializeTimeSeriesData(List<TagGroup> keys, DataInput in) throws IOException {
		// Load the map with a time series for each tag group
		data = Maps.newHashMap();
		for (int i = 0; i < keys.size(); i++) {
			TagGroup tg = keys.get(i);

			data.put(tg, TimeSeriesTagCoverageMetrics.deserialize(in, numUserTags));
		}
	}
}
