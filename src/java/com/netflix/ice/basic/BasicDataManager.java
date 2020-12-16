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
package com.netflix.ice.basic;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;

import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.AggregateType;
import com.netflix.ice.reader.DataManager;
import com.netflix.ice.reader.InstanceMetricsService;
import com.netflix.ice.reader.ReadOnlyData;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.UsageUnit;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTagKey;
import com.netflix.ice.tag.Zone.BadZone;

/**
 * This class reads data from s3 bucket and feeds the data to UI
 */
public class BasicDataManager extends CommonDataManager<ReadOnlyData, double[]> implements DataManager {

    protected InstanceMetricsService instanceMetricsService;
    protected int numUserTags;
    protected boolean forReservations;
    
    public BasicDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress, int numUserTags,
    		int monthlyCacheSize, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService, InstanceMetricsService instanceMetricsService) {
    	super(startDate, dbName, consolidateType, tagGroupManager, compress, monthlyCacheSize, workBucketConfig, accountService, productService);
        this.instanceMetricsService = instanceMetricsService;
        this.numUserTags = numUserTags;
        this.forReservations = false;
    }
    	
    public BasicDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress, int numUserTags,
    		int monthlyCacheSize, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService, InstanceMetricsService instanceMetricsService, boolean forReservations) {
    	super(startDate, dbName, consolidateType, tagGroupManager, compress, monthlyCacheSize, workBucketConfig, accountService, productService);
        this.instanceMetricsService = instanceMetricsService;
        this.numUserTags = numUserTags;
        this.forReservations = forReservations;
    }
    	
	public int size(DateTime start) throws ExecutionException {
		ReadOnlyData data = getReadOnlyData(start);
		return data.getTagGroups().size();
	}
	
    @Override
    protected ReadOnlyData newEmptyData() {
    	return new ReadOnlyData(numUserTags);
    }

    @Override
    protected ReadOnlyData deserializeData(DataInputStream in) throws IOException, BadZone {
	    ReadOnlyData result = new ReadOnlyData(numUserTags);
	    result.deserialize(accountService, productService, in, forReservations);
	    return result;
    }
            
    private double adjustForUsageUnit(UsageUnit usageUnit, UsageType usageType, double value) {
    	double multiplier = 1.0;
    	
    	switch (usageUnit) {
    	default:
    		return value;
    	
    	case ECUs:
    		multiplier = instanceMetricsService.getInstanceMetrics().getECU(usageType);
    		break;
    		
    	case vCPUs:
    		multiplier = instanceMetricsService.getInstanceMetrics().getVCpu(usageType);
    		break;
    	case Normalized:
    		multiplier = instanceMetricsService.getInstanceMetrics().getNormalizationFactor(usageType);
    		break;
    	}
    	return value * multiplier;    		
    }

	@Override
    protected void addData(double[] from, double[] to) {
        for (int i = 0; i < from.length; i++)
        	to[i] += from[i];
    }
	
    @Override
    protected boolean hasData(double[] data) {
    	// Check for values in the data array and ignore if all zeros
    	for (double d: data) {
    		if (d != 0.0)
    			return true;
    	}
    	return false;
    }

	@Override
	protected double[] getResultArray(int size) {
        return new double[size];
	}

    private double aggregate(List<Integer> columns, List<TagGroup> tagGroups, UsageUnit usageUnit, double[] data) {
		double result = 0.0;
		if (data != null) {
	        for (int i = 0; i < columns.size(); i++) {
	        	double d = data[columns.get(i)];
	        	if (d != 0.0)
	        		result += adjustForUsageUnit(usageUnit, tagGroups.get(i).usageType, d);
	        }
		}
        return result;
	}

	@Override
    protected int aggregate(ReadOnlyData data, int from, int to, double[] result, List<Integer> columns, List<TagGroup> tagGroups, UsageUnit usageUnit) {		
        int fromIndex = from;
        int resultIndex = to;
        while (resultIndex < result.length && fromIndex < data.getNum()) {
        	double[] fromData = data.getData(fromIndex++);
            result[resultIndex] = aggregate(columns, tagGroups, usageUnit, fromData);
            resultIndex++;
        }
        return fromIndex - from;
	}
	
	@Override
	protected Map<Tag, double[]> processResult(Map<Tag, double[]> data, TagType groupBy, AggregateType aggregate, List<UserTagKey> tagKeys) {
		Map<Tag, double[]> result = Maps.newTreeMap();
		for (Tag t: data.keySet()) {
			result.put(t, data.get(t));
		}
		
		if (aggregate != AggregateType.none && result.values().size() > 0) {
		    double[] aggregated = new double[result.values().iterator().next().length];
			for (double[] d: result.values()) {
		        for (int i = 0; i < d.length; i++)
		        	aggregated[i] += d[i];
		    }
		    result.put(Tag.aggregated, aggregated);          
		}
		
		return result;
	}


}
