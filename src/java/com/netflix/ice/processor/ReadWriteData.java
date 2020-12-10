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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Zone.BadZone;

/**
 * ReadWriteData is a list of maps that hold usage and cost data for AWS products.
 * Each map in the list has a unique set of tags representing the data in the particular
 * hour/day/week/month.
 * The list index is the hour in the month for the instance data.
 * The map keys are a TagGroup which is the unique combination of Tags associated with
 * the cost or usage number stored as the value in the map.
 */
public class ReadWriteData extends ReadWriteGenericData<Double> {
    public ReadWriteData() {
		super();
	}

    public ReadWriteData(int numUserTags) {
		super(numUserTags);
	}

    public ReadWriteData(int numUserTags, boolean cacheTagGroups) {
		super(numUserTags);
	}

	static public Map<TagGroup, Double> getCreateData(List<Map<TagGroup, Double>> data, int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, Double>newHashMap());
            }
        }
        return data.get(i);
    }

	@Override
	protected void writeValue(DataOutput out, Double value) throws IOException {
        out.writeDouble(value == null ? 0 : value);			
	}

	@Override
	protected Double readValue(DataInput in) throws IOException {
        double v = in.readDouble();
        return v != 0 ? v : null;
	}
	
	@Override
    protected Double add(Double a, Double b) {
		return a + b;
	}

	
    public void serializeCsv(OutputStreamWriter out, String resourceGroupHeader) throws IOException {
    	// write the header
    	out.write("index,");
    	TagGroup.Serializer.serializeCsvHeader(out, resourceGroupHeader);
    	out.write(",data\n");
        for (int i = 0; i < data.size(); i++) {
            Map<TagGroup, Double> map = getData(i);
            for (Entry<TagGroup, Double> entry: map.entrySet()) {
            	out.write("" + i + ",");
            	TagGroup.Serializer.serializeCsv(out, entry.getKey());
                out.write(",");
                Double v = entry.getValue();
                if (v != null)
                	out.write(v.toString());
                out.write("\n");
            }
        }
    }

    public void deserializeCsv(AccountService accountService, ProductService productService, BufferedReader in) throws IOException, BadZone {
        List<Map<TagGroup, Double>> data = Lists.newArrayList();
        
        String line;
        
        // skip the header
        in.readLine();

        Map<TagGroup, Double> map = null;
        while ((line = in.readLine()) != null) {
        	String[] items = line.split(",");
        	int hour = Integer.parseInt(items[0]);
        	while (hour >= data.size()) {
        		map = Maps.newHashMap();
        		data.add(map);
        	}
        	map = data.get(hour);
        	String[] resourceGroup = null;
        	if (items.length > 9) {
	        	// Subtract the eight items before and the value at the end == 8 + 1
	        	resourceGroup = new String[items.length - 9];
	        	for (int i = 0; i < items.length - 9; i++)
	        		resourceGroup[i] = items[i + 8];
        	}
        	TagGroup tag = null;
			try {
				tag = TagGroup.getTagGroup(items[1], items[2], items[3], items[4], items[5], items[6], items[7], resourceGroup, accountService, productService);
			} catch (ResourceException e) {
				// Should never throw because no user tags are null
			}
        	Double v = Double.parseDouble(items[items.length-1]);
        	map.put(tag, v);
        }

        this.data = data;
    }

}
