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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.ice.common.TimeSeriesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.DataVersion;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Zone.BadZone;

public class DataSerializer extends ReadWriteGenericData<DataSerializer.CostAndUsage> {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static class CostAndUsage implements ReadWriteDataSerializer.Summable<CostAndUsage> {
    	public final double cost;
    	public final double usage;
    	
    	public CostAndUsage() {
    		this.cost = 0;
    		this.usage = 0;
    	}
    	
    	public CostAndUsage(double cost, double usage) {
    		this.cost = cost;
    		this.usage = usage;
    	}
    	
    	public String toString() {
    		return "{cost: " + Double.toString(cost) + ", usage: " + Double.toString(usage) + "}";
    	}
    	
    	public boolean isZero() {
    		return cost == 0.0 && usage == 0.0;
    	}

    	public CostAndUsage add(CostAndUsage value) {
    		if (value == null || value.isZero())
    			return this;
    		
    		return new CostAndUsage(cost + value.cost, usage + value.usage);
    	}
    	
    	public CostAndUsage add(double otherCost, double otherUsage) {
    		if (otherCost == 0 && otherUsage == 0)
    			return this;
    		
    		return new CostAndUsage(cost + otherCost, usage + otherUsage);
    	}
    	
    	public CostAndUsage sub(CostAndUsage value) {
    		if (value == null || value.isZero())
    			return this;
    		
    		return new CostAndUsage(cost - value.cost, usage - value.usage);
    	}

    	public boolean equals(CostAndUsage o) {
    		return cost == o.cost && usage == o.usage;
		}
    	
    }

	public DataSerializer(int numUserTags) {
    	super(numUserTags);
	}

	static public Map<TagGroup, CostAndUsage> getCreateData(List<Map<TagGroup, CostAndUsage>> data, int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, CostAndUsage>newHashMap());
            }
        }
        return data.get(i);
    }

    public void add(int i, TagGroup tagGroup, CostAndUsage value) {
    	Map<TagGroup, CostAndUsage> map = getCreateData(i);
        map.put(tagGroup, value.add(map.get(tagGroup)));
    	if (tagGroups != null)
    		tagGroups.add(tagGroup);
    }

    public void add(int i, TagGroup tagGroup, double cost, double usage) {
    	Map<TagGroup, CostAndUsage> map = getCreateData(i);
    	CostAndUsage existing = map.get(tagGroup);
        map.put(tagGroup, existing != null ? existing.add(cost, usage) : new CostAndUsage(cost, usage));
    	if (tagGroups != null)
    		tagGroups.add(tagGroup);
    }


    Map<TagGroup, CostAndUsage> getCreateData(int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, CostAndUsage>newHashMap());
            }
        }
        return data.get(i);
    }

	/**
     * Serialize data using standard Java serialization DataOutput methods in the following order:<br/>
     *
     * 1. Version (int)<br/>
     * 2. Number of user tags (int)<br/>
     * 3. TagGroup count (int)<br/>
     * 4. TagGroup Array<br/>
     * 5. Number of hours/days/weeks/months of data (int)<br/>
     * 6. Data matrix:<br/>
     * 		6a. Data present for time interval flag (boolean)<br/>
     * 		6b. Data array for time interval consisting of a pair of cost and usage doubles for each TagGroup (if flag is true)<br/>
     */
	@Override
	protected void serializeTimeSeriesData(Collection<TagGroup> keys, DataOutput out) throws IOException {

		double cost[] = new double[data.size()];
		double usage[] = new double[data.size()];

		for (TagGroup tagGroup: keys) {
			for (int i = 0; i < data.size(); i++) {
				Map<TagGroup, CostAndUsage> map = getData(i);
				if (map.isEmpty()) {
					cost[i] = 0;
					usage[i] = 0;
				}
				else {
					CostAndUsage v = map.get(tagGroup);
					cost[i] = v == null ? 0 : v.cost;
					usage[i] = v == null ? 0 : v.usage;
				}
			}
			TimeSeriesData tsd = new TimeSeriesData(cost, usage);
			tsd.serialize(out);
		}
	}

	@Override
	protected List<Map<TagGroup, CostAndUsage>> deserializeTimeSeriesData(Collection<TagGroup> keys, DataInput in) throws IOException {
        List<Map<TagGroup, CostAndUsage>> data = Lists.newArrayList();
        int num = in.readInt();
        for (int i = 0; i < num; i++)
        	data.add(Maps.<TagGroup, CostAndUsage>newHashMap());

		boolean timeSeries = true;
		double cost[] = new double[num];
		double usage[] = new double[num];
		for (TagGroup tagGroup: keys) {
			TimeSeriesData tsd = TimeSeriesData.deserialize(in);
			tsd.get(TimeSeriesData.Type.COST, 0, num, cost);
			tsd.get(TimeSeriesData.Type.USAGE, 0, num, usage);
			for (int i = 0; i < num; i++) {
				if (cost[i] != 0 || usage[i] != 0) {
					CostAndUsage cau = new CostAndUsage(cost[i], usage[i]);
					data.get(i).put(tagGroup, cau);
				}
			}
		}

        return data;
	}

    public void serializeCsv(OutputStreamWriter out, String resourceGroupHeader) throws IOException {
    	// write the header
    	out.write("index,");
    	TagGroup.Serializer.serializeCsvHeader(out, resourceGroupHeader);
    	out.write(",cost,usage\n");
        for (int i = 0; i < data.size(); i++) {
            Map<TagGroup, CostAndUsage> map = getData(i);
            for (Entry<TagGroup, CostAndUsage> entry: map.entrySet()) {
            	out.write("" + i + ",");
            	TagGroup.Serializer.serializeCsv(out, entry.getKey());
                out.write(",");
                CostAndUsage v = entry.getValue();
                if (v == null)
                	out.write("0.0,0.0");
                else
                	out.write(Double.toString(v.cost) + "," + Double.toString(v.usage));
                out.write("\n");
            }
        }
    }
    
    public void deserializeCsv(AccountService accountService, ProductService productService, BufferedReader in) throws IOException, BadZone {
    	final int resourceStartIndex = 9; // 9 dimensions (index, costType, account, region, zone, product, operation, usageType, units)  plus cost and usage columns
    	final int numNonResourceColumns = resourceStartIndex + 2; // plus cost and usage columns
        List<Map<TagGroup, CostAndUsage>> data = Lists.newArrayList();
        
        String line;
        
        // skip the header
        in.readLine();

        Map<TagGroup, CostAndUsage> map = null;
        while ((line = in.readLine()) != null) {
        	String[] items = line.split(",");
        	int hour = Integer.parseInt(items[0]);
        	while (hour >= data.size()) {
        		map = Maps.newHashMap();
        		data.add(map);
        	}
        	map = data.get(hour);
        	String[] resourceGroup = null;
        	if (items.length > numNonResourceColumns) {
	        	resourceGroup = new String[items.length - numNonResourceColumns];
	        	for (int i = 0; i < items.length - numNonResourceColumns; i++)
	        		resourceGroup[i] = items[i + resourceStartIndex];
        	}
        	TagGroup tag = null;
			try {
				tag = TagGroup.getTagGroup(items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], resourceGroup, accountService, productService);
			} catch (ResourceException e) {
				// Should never throw because no user tags are null
			}
        	Double cost = Double.parseDouble(items[items.length-2]);
        	Double usage = Double.parseDouble(items[items.length-1]);
        	map.put(tag, new CostAndUsage(cost, usage));
        }

        this.data = data;
    }
}
