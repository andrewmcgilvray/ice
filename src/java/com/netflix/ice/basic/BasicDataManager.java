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

import com.netflix.ice.reader.*;
import org.apache.commons.lang.time.StopWatch;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.UserTagKey;
import com.netflix.ice.tag.Zone.BadZone;

/**
 * This class reads data from s3 bucket and feeds the data to UI
 */
public class BasicDataManager extends CommonDataManager<ReadOnlyData, TimeSeriesData> implements DataManager {

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
		return data.numTagGroups();
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

    private int aggregate(boolean isCost, ReadOnlyData data, int from, int to, double[] result, List<TagGroup> tagGroups, UsageUnit usageUnit) {
        int fromIndex = from;

		int numToCopy = Math.min(result.length - to, data.getNum() - from);
		double[] values = new double[numToCopy];
		for (TagGroup tg: tagGroups) {
			TimeSeriesData tsd = data.getData(tg);
			tsd.get(isCost ? TimeSeriesData.Type.COST : TimeSeriesData.Type.USAGE, fromIndex, numToCopy, values);
			for (int i = 0; i < numToCopy; i++) {
				if (isCost)
					result[to+i] += values[i];
				else
					result[to+i] += adjustForUsageUnit(usageUnit, tg.usageType, values[i]);
			}
		}
		return numToCopy;
	}
	
	private Map<Tag, double[]> processResult(boolean isCost, Map<Tag, double[]> data, TagType groupBy, AggregateType aggregate, List<UserTagKey> tagKeys) {
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

    /*
     * Aggregate all the data matching the tags in tagLists at requested time for the specified to and from indices.
     */
    private int aggregateData(boolean isCost, DateTime time, TagLists tagLists, int from, int to, double[] result, UsageUnit usageUnit, TagType groupBy, Tag tag, int userTagGroupByIndex) throws ExecutionException {
        ReadOnlyData data = getReadOnlyData(time);

		// Figure out which tagGroups we're going to aggregate
		List<TagGroup> tagGroups = getTagGroups(groupBy, tag, userTagGroupByIndex, data, tagLists);
		if (tagGroups == null)
			return 0;
		return aggregate(isCost, data, from, to, result, tagGroups, usageUnit);
    }
        
    private double[] getData(boolean isCost, Interval interval, TagLists tagLists, UsageUnit usageUnit, TagType groupBy, Tag tag, int userTagGroupByIndex) throws ExecutionException {
    	Interval adjusted = getAdjustedInterval(interval);
        DateTime start = adjusted.getStart();
        DateTime end = adjusted.getEnd();

        double[] result = new double[getSize(interval)];

        do {
            int resultIndex = getResultIndex(start, interval);
            int fromIndex = getFromIndex(start, interval);            
            int count = aggregateData(isCost, start, tagLists, fromIndex, resultIndex, result, usageUnit, groupBy, tag, userTagGroupByIndex);
            fromIndex += count;
            resultIndex += count;

            if (consolidateType  == ConsolidateType.hourly)
                start = start.plusMonths(1);
            else if (consolidateType  == ConsolidateType.daily)
                start = start.plusYears(1);
            else
                break;
        }
        while (start.isBefore(end));
        
        return result;
    }
    
    private boolean hasData(double[] d) {
    	for (int i = 0; i < d.length; i++) {
    		if (d[i] != 0.0)
    			return true;
    	}
    	return false;
    }
    
    private void addData(double[] from, double[] to) {
    	for (int i = 0; i < to.length; i++) {
    		to[i] += from[i];
    	}
    }

    private Map<Tag, double[]> getGroupedData(boolean isCost, Interval interval, Map<Tag, TagLists> tagListsMap, UsageUnit usageUnit, TagType groupBy, int userTagGroupByIndex) {
        Map<Tag, double[]> rawResult = Maps.newTreeMap();
//        StopWatch sw = new StopWatch();
//        sw.start();

		Tag none = UserTag.get(UserTag.none);
        
        // For each of the groupBy values
        for (Tag tag: tagListsMap.keySet()) {
            try {
                //logger.info("Tag: " + tag + ", TagLists: " + tagListsMap.get(tag));
                double[] data = getData(isCost, interval, tagListsMap.get(tag), usageUnit, groupBy, tag, userTagGroupByIndex);
                
            	// Check for values in the data array and ignore if all zeros
                if (hasData(data)) {
	                if (groupBy == TagType.Tag) {
	                	Tag userTag = tag.name.isEmpty() ? none : tag;
	                	
	        			if (rawResult.containsKey(userTag)) {
	        				// aggregate current data with the one already in the map
	        				addData(data, rawResult.get(userTag));
	        			}
	        			else {
	        				// Put in map using the user tag
	        				rawResult.put(userTag, data);
	        			}
	                }
	                else {
	                	rawResult.put(tag == null ? none : tag, data);
	                }
                }
            }
            catch (ExecutionException e) {
                logger.error("error in getData for " + tag + " " + interval, e);
            }
        }
//        sw.stop();
//        logger.info("getGroupedData elapsed time " + sw);
        return rawResult;
    }

    private Map<Tag, double[]> getRawData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit, int userTagGroupByIndex) {
    	//logger.info("Entered with groupBy: " + groupBy + ", userTagGroupByIndex: " + userTagGroupByIndex + ", tagLists: " + tagLists);
    	Map<Tag, TagLists> tagListsMap = tagGroupManager.getTagListsMap(interval, tagLists, groupBy, exclude, userTagGroupByIndex);
    	return getGroupedData(isCost, interval, tagListsMap, usageUnit, groupBy, userTagGroupByIndex);
    }

	@Override
    protected Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit, int userTagGroupByIndex, List<UserTagKey> tagKeys) {
    	StopWatch sw = new StopWatch();
    	sw.start();
    	Map<Tag, double[]> rawResult = getRawData(isCost, interval, tagLists, groupBy, aggregate, exclude, usageUnit, userTagGroupByIndex);
        Map<Tag, double[]> result = processResult(isCost, rawResult, groupBy, aggregate, tagKeys);
        logger.debug("getData elapsed time: " + sw);
        return result;
    }

}
