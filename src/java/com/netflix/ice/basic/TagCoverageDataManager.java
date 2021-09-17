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
import com.netflix.ice.processor.TagCoverageMetrics;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.UserTagKey;
import com.netflix.ice.tag.Zone.BadZone;

public class TagCoverageDataManager extends CommonDataManager<ReadOnlyTagCoverageData, TimeSeriesTagCoverageMetrics> implements DataManager {
    //private final static Logger staticLogger = LoggerFactory.getLogger(TagCoverageDataManager.class);
	
	protected List<UserTagKey> userTagKeys;

	public TagCoverageDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress, List<UserTagKey> userTagKeys,
			int monthlyCacheSize, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService) {
		super(startDate, dbName, consolidateType, tagGroupManager, compress, monthlyCacheSize, workBucketConfig, accountService, productService);
		this.userTagKeys = userTagKeys;
	}

	public int size(DateTime start) throws ExecutionException {
		ReadOnlyTagCoverageData data = getReadOnlyData(start);
		return data.numTagGroups();
	}

	protected int getUserTagKeysSize() {
		return userTagKeys.size();
	}
    
	private void addData(TagCoverageMetrics[] from, TagCoverageMetrics[] to) {
        for (int i = 0; i < from.length; i++) {
        	if (from[i] == null)
        		continue;
        	else if (to[i] == null)
        		to[i] = from[i];
        	else
            	to[i].add(from[i]);
        }
	}

	@Override
	protected ReadOnlyTagCoverageData newEmptyData() {
    	return new ReadOnlyTagCoverageData(getUserTagKeysSize());
	}

	@Override
	protected ReadOnlyTagCoverageData deserializeData(DataInputStream in)
			throws IOException, BadZone {
	    ReadOnlyTagCoverageData result = new ReadOnlyTagCoverageData(getUserTagKeysSize());
	    result.deserialize(accountService, productService, in);
	    return result;
	}

    private int aggregate(ReadOnlyTagCoverageData data, int from, int to, TagCoverageMetrics[] result, List<TagGroup> tagGroups, UsageUnit usageUnit) {
        int fromIndex = from;

		int numToCopy = Math.min(result.length - to, data.getNum() - from);
		TagCoverageMetrics[] values = new TagCoverageMetrics[numToCopy];

		for (TagGroup tg: tagGroups) {
			TimeSeriesTagCoverageMetrics tsd = data.getData(tg);
			tsd.get(fromIndex, numToCopy, values);
			for (int i = 0; i < numToCopy; i++) {
				result[to+i].add(values[i]);
			}
		}
		return numToCopy;
	}
	
	private boolean hasData(TagCoverageMetrics[] data) {
    	// Check for values in the data array and ignore if all zeros
    	for (TagCoverageMetrics d: data) {
    		if (d != null && d.getTotal() > 0)
    			return true;
    	}
    	return false;
	}
	
	protected List<UserTagKey> getUserTagKeys() {
		return userTagKeys;
	}

    protected Map<Tag, double[]> processResult(Map<Tag, TagCoverageMetrics[]> data, TagType groupBy, AggregateType aggregate, List<UserTagKey> tagKeys) {
    	return TagCoverageDataManager.processResult(data, groupBy, aggregate, tagKeys, getUserTagKeys());
    }
    
    /*
     * Class to hold a single tag coverage ratio
     */
    private static class Ratio {
    	public int total;
    	public int count;
    	
    	Ratio(int total, int count) {
    		this.total = total;
    		this.count = count;
    	}
    	
    	public void add(int total, int count) {
    		this.total += total;
    		this.count += count;
    	}
    }
    
    static public Map<Tag, double[]> processResult(Map<Tag, TagCoverageMetrics[]> data, TagType groupBy, AggregateType aggregate, List<UserTagKey> tagKeys, List<UserTagKey> userTagKeys) {
    	// list of tagKeys we want to export
    	List<Integer> tagKeyIndecies = Lists.newArrayList();
    	for (UserTagKey tagKey: tagKeys) {
    		for (int i = 0; i < userTagKeys.size(); i++) {
    			if (tagKey.name.equals(userTagKeys.get(i).name))
    				tagKeyIndecies.add(i);
    		}
    	}    	
    	
		Map<Tag, double[]> result = Maps.newTreeMap();
		Ratio[] aggregateCoverage = null;
		
		if (groupBy == null || groupBy == TagType.TagKey) {
			// All data is under the aggregated tag
			TagCoverageMetrics[] metricsArray = data.get(Tag.aggregated);
			if (metricsArray == null)
				return result;

			if (aggregateCoverage == null) {
				aggregateCoverage = new Ratio[metricsArray.length];
			}

			double[][] d = new double[tagKeys.size()][metricsArray.length];
			
			for (int i = 0; i < metricsArray.length; i++) {
				for (int j = 0; j < tagKeyIndecies.size(); j++) {
					if (metricsArray[i] != null) {
						d[j][i] = metricsArray[i].getPercentage(tagKeyIndecies.get(j));
						if (aggregateCoverage[i] == null)
							aggregateCoverage[i] = new Ratio(metricsArray[i].getTotal(), metricsArray[i].getCount(j));
						else
							aggregateCoverage[i].add(metricsArray[i].getTotal(), metricsArray[i].getCount(j));						
					}
				}
			}
			
			// Put the data into the map
			if (groupBy == null && tagKeyIndecies.size() > 0) {
				result.put(Tag.aggregated, d[0]);
			}
			else {
				for (int j = 0; j < tagKeyIndecies.size(); j++) {
					result.put(tagKeys.get(j), d[j]);
				}
			}
		}
		else {
			int userTagIndex = tagKeyIndecies.get(0);
			
			for (Tag tag: data.keySet()) {
				TagCoverageMetrics[] metricsArray = data.get(tag);
				if (metricsArray == null)
					continue;
				
				if (aggregateCoverage == null) {
					aggregateCoverage = new Ratio[metricsArray.length];
				}
				
				double[] d = new double[metricsArray.length];
				
				for (int i = 0; i < metricsArray.length; i++) {
					if (metricsArray[i] != null) {
						d[i] = metricsArray[i].getPercentage(userTagIndex);
						if (aggregateCoverage[i] == null)
							aggregateCoverage[i] = new Ratio(metricsArray[i].getTotal(), metricsArray[i].getCount(userTagIndex));
						else
							aggregateCoverage[i].add(metricsArray[i].getTotal(), metricsArray[i].getCount(userTagIndex));
					}
				}
				
				// Put the data into the map
				result.put(tag, d);
			}
		}
		
		if (!result.containsKey(Tag.aggregated) && aggregateCoverage != null) {
			// Convert aggregated ratios to percentage
		    double[] aggregated = new double[aggregateCoverage.length];
		    for (int i = 0; i < aggregateCoverage.length; i++) {
		    	if (aggregateCoverage[i] == null) {
		    		aggregated[i] = 0;
		    		continue;
		    	}
		        aggregated[i] = aggregateCoverage[i].total > 0 ? (double) aggregateCoverage[i].count / (double) aggregateCoverage[i].total * 100.0 : 0.0;
		    }
		    result.put(Tag.aggregated, aggregated);          
		}
		
		return result;
	}

    /*
     * Aggregate all the data matching the tags in tagLists at requested time for the specified to and from indices.
     */
    private int aggregateData(DateTime time, TagLists tagLists, int from, int to, TagCoverageMetrics[] result, UsageUnit usageUnit, TagType groupBy, Tag tag, int userTagGroupByIndex) throws ExecutionException {
        ReadOnlyTagCoverageData data = getReadOnlyData(time);

		// Figure out which tagGroups we're going to aggregate
		List<TagGroup> tagGroups = getTagGroups(groupBy, tag, userTagGroupByIndex, data, tagLists);
		if (tagGroups == null)
			return 0;
		return aggregate(data, from, to, result, tagGroups, usageUnit);
    }
        
    private TagCoverageMetrics[] getData(Interval interval, TagLists tagLists, UsageUnit usageUnit, TagType groupBy, Tag tag, int userTagGroupByIndex) throws ExecutionException {
    	Interval adjusted = getAdjustedInterval(interval);
        DateTime start = adjusted.getStart();
        DateTime end = adjusted.getEnd();

        TagCoverageMetrics[] result = new TagCoverageMetrics[getSize(interval)];

        do {
            int resultIndex = getResultIndex(start, interval);
            int fromIndex = getFromIndex(start, interval);            
            int count = aggregateData(start, tagLists, fromIndex, resultIndex, result, usageUnit, groupBy, tag, userTagGroupByIndex);
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
    
    private Map<Tag, TagCoverageMetrics[]> getGroupedData(Interval interval, Map<Tag, TagLists> tagListsMap, UsageUnit usageUnit, TagType groupBy, int userTagGroupByIndex) {
        Map<Tag, TagCoverageMetrics[]> rawResult = Maps.newTreeMap();
//        StopWatch sw = new StopWatch();
//        sw.start();
        
        // For each of the groupBy values
        for (Tag tag: tagListsMap.keySet()) {
            try {
                //logger.info("Tag: " + tag + ", TagLists: " + tagListsMap.get(tag));
            	TagCoverageMetrics[] data = getData(interval, tagListsMap.get(tag), usageUnit, groupBy, tag, userTagGroupByIndex);
                
            	// Check for values in the data array and ignore if all zeros
                if (hasData(data)) {
	                if (groupBy == TagType.Tag) {
	                	Tag userTag = tag.name.isEmpty() ? UserTag.get(UserTag.none) : tag;
	                	
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
	                	rawResult.put(tag, data);
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

    public Map<Tag, TagCoverageMetrics[]> getRawData(Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, int userTagGroupByIndex) {
    	return getRawData(interval, tagLists, groupBy, aggregate, null, null, userTagGroupByIndex);
    }
    
    private Map<Tag, TagCoverageMetrics[]> getRawData(Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit, int userTagGroupByIndex) {
    	//logger.info("Entered with groupBy: " + groupBy + ", userTagGroupByIndex: " + userTagGroupByIndex + ", tagLists: " + tagLists);
    	Map<Tag, TagLists> tagListsMap = tagGroupManager.getTagListsMap(interval, tagLists, groupBy, exclude, userTagGroupByIndex);
    	return getGroupedData(interval, tagListsMap, usageUnit, groupBy, userTagGroupByIndex);
    }
    
    
	@Override
    protected Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit, int userTagGroupByIndex, List<UserTagKey> tagKeys) {
    	StopWatch sw = new StopWatch();
    	sw.start();
    	Map<Tag, TagCoverageMetrics[]> rawResult = getRawData(interval, tagLists, groupBy, aggregate, exclude, usageUnit, userTagGroupByIndex);
        Map<Tag, double[]> result = processResult(rawResult, groupBy, aggregate, tagKeys);
        logger.debug("getData elapsed time: " + sw);
        return result;
    }

}
