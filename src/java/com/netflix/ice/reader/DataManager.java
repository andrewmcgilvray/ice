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

import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTagKey;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Interface to feed data to UI.
 */
public interface DataManager {

    /**
     * Get map of data.
     * @param interval
     * @param tagLists
     * @param groupBy
     * @param aggregate
     * @param forReservation
     * @return
     */
    Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit);

    /**
     * Get map of data.
     * @param interval
     * @param tagLists
     * @param groupBy
     * @param aggregate
     * @param forReservation
     * @return
     */
    Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, List<Operation.Identity.Value> exclude, UsageUnit usageUnit, int userTagGroupByIndex);

    Map<Tag, double[]> getData(boolean isCost, Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, int userTagGroupByIndex, List<UserTagKey> tagKeys);

    /**
     * Get data length.
     * @param start
     * @return
     */
    int getDataLength(DateTime start);

    int size(DateTime start) throws ExecutionException;

}
