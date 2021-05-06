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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Months;
import org.joda.time.Weeks;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.AggregateType;
import com.netflix.ice.reader.DataManager;
import com.netflix.ice.reader.ReadOnlyGenericData;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.reader.UsageUnit;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTagKey;

public abstract class CommonDataManager<T extends ReadOnlyGenericData<D>, D>  extends DataFileCache<T> implements DataManager {

    protected TagGroupManager tagGroupManager;
    
	public CommonDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress,
    		int monthlyCacheSize, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService) {
    	super(startDate, dbName, consolidateType, compress, monthlyCacheSize, workBucketConfig, accountService, productService);
        this.tagGroupManager = tagGroupManager;
	}
	
    protected void getColumns(TagType groupBy, Tag tag, int userTagGroupByIndex, T data, TagLists tagLists, List<Integer> columnIndecies, List<TagGroup> tagGroups) {    	
    	Map<TagGroup, Integer> m = data.getTagGroups(groupBy, tag, userTagGroupByIndex);
    	if (m == null) {
    		// No index, do it the hard way
            int columnIndex = 0;
            for (TagGroup tagGroup: data.getTagGroups()) {
            	boolean contains = tagLists.contains(tagGroup, true);
                if (contains) {
                	columnIndecies.add(columnIndex);
                	tagGroups.add(tagGroup);
                }
                columnIndex++;
            }    		
    		return;
    	}
    	
        for (TagGroup tagGroup: m.keySet()) {
        	boolean contains = tagLists.contains(tagGroup, true);
            if (contains) {
            	columnIndecies.add(m.get(tagGroup));
            	tagGroups.add(tagGroup);
            }
        }
    }
    
    protected int getFromIndex(DateTime start, Interval interval) {
    	int fromIndex = 0;
    	if (!interval.getStart().isBefore(start)) {
            if (consolidateType == ConsolidateType.hourly) {
                fromIndex = Hours.hoursBetween(start, interval.getStart()).getHours();
            }
            else if (consolidateType == ConsolidateType.daily) {
                fromIndex = Days.daysBetween(start, interval.getStart()).getDays();
            }
            else if (consolidateType == ConsolidateType.weekly) {
                fromIndex = Weeks.weeksBetween(start, interval.getStart()).getWeeks();
                if (start.getDayOfWeek() != interval.getStart().getDayOfWeek())
                    fromIndex++;
            }
            else if (consolidateType == ConsolidateType.monthly) {
                fromIndex = Months.monthsBetween(start, interval.getStart()).getMonths();
            }
    	}
    	return fromIndex;
    }
       
    protected int getResultIndex(DateTime start, Interval interval) {
    	int resultIndex = 0;
        if (interval.getStart().isBefore(start)) {
            if (consolidateType == ConsolidateType.hourly) {
                resultIndex = Hours.hoursBetween(interval.getStart(), start).getHours();
            }
            else if (consolidateType == ConsolidateType.daily) {
                resultIndex = Days.daysBetween(interval.getStart(), start).getDays();
            }
            else if (consolidateType == ConsolidateType.weekly) {
                resultIndex = Weeks.weeksBetween(interval.getStart(), start).getWeeks();
            }
            else if (consolidateType == ConsolidateType.monthly) {
                resultIndex = Months.monthsBetween(interval.getStart(), start).getMonths();
            }
        }
    	return resultIndex;
    }
    
    public int getDataLength(DateTime start) {
        try {
            T data = getReadOnlyData(start);
            return data.getNum();
        }
        catch (ExecutionException e) {
            logger.error("error in getDataLength for " + start, e);
            return 0;
        }
    }

    abstract protected Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit, int userTagGroupByIndex, List<UserTagKey> tagKeys);

	@Override
    public Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit, int userTagGroupByIndex) {
    	return getData(isCost, interval, tagLists, groupBy, aggregate, exclude, usageUnit, userTagGroupByIndex, null);
    }

	@Override
	public Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists,
			TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude,
			UsageUnit usageUnit) {
		return getData(isCost, interval, tagLists, groupBy, aggregate, exclude, usageUnit, 0);
	}

	@Override
	public Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists,
			TagType groupBy, AggregateType aggregate, int userTagGroupByIndex, List<UserTagKey> tagKeys) {
		return getData(isCost, interval, tagLists, groupBy, aggregate, null, null, userTagGroupByIndex, tagKeys);
	}
}
