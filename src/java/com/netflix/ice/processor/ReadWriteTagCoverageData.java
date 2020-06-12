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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.netflix.ice.common.TagGroup;

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
	protected void writeValue(DataOutput out, TagCoverageMetrics value) throws IOException {
		out.writeBoolean(value != null);
		if (value != null)
			value.serialize(out);	
	}

	@Override
	protected TagCoverageMetrics readValue(DataInput in) throws IOException {
		Boolean hasValue = in.readBoolean();
		return hasValue ? TagCoverageMetrics.deserialize(in, numUserTags) : null;
	}
	
	@Override
    protected TagCoverageMetrics add(TagCoverageMetrics a, TagCoverageMetrics b) {
		a.add(b);
		return a;
	}

}
