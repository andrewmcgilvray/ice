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

import com.google.common.collect.Lists;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.TagCoverageMetrics;

public class ReadOnlyTagCoverageData extends ReadOnlyGenericData<TagCoverageMetrics[]> {

	public ReadOnlyTagCoverageData(int numUserTags) {
		super(new TagCoverageMetrics[][]{}, Lists.<TagGroup>newArrayList(), numUserTags);
	}

	@Override
	protected TagCoverageMetrics[][] newDataMatrix(int size) {
		return new TagCoverageMetrics[size][];
	}
	
	@Override
	protected TagCoverageMetrics[] readDataArray(DataInput in) throws IOException {
		TagCoverageMetrics[] data = new TagCoverageMetrics[tagGroups.size()];
        for (int j = 0; j < tagGroups.size(); j++) {
    		Boolean hasValue = in.readBoolean();
    		if (hasValue)
    			data[j] = TagCoverageMetrics.deserialize(in, numUserTags);
        }
        return data;
 	}
}
