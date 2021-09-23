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

import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TimeSeriesData;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Zone;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadOnlyData extends ReadOnlyGenericData<TimeSeriesData> {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected boolean forReservations;

    public ReadOnlyData(int numUserTags) {
        super(Maps.<TagGroup, TimeSeriesData>newHashMap(), numUserTags, 0);
        forReservations = false;
    }
    
    public ReadOnlyData(Map<TagGroup, TimeSeriesData> data, int numUserTags, int numIntervals) {
		super(data, numUserTags, numIntervals);
	}

    public void deserialize(AccountService accountService, ProductService productService, DataInput in, boolean forReservations) throws IOException, Zone.BadZone {
        this.forReservations = forReservations;
        super.deserialize(accountService, productService, in);
    }
	@Override
	protected void deserializeTimeSeriesData(List<TagGroup> keys, DataInput in) throws IOException {
        // Read the data into cost and usage arrays indexed by time interval and TagGroup
        int numKeys = keys.size();

        // Load the map with a time series for each tag group
        this.data = Maps.newHashMap();
        for (int i = 0; i < keys.size(); i++) {
            TagGroup tg = keys.get(i);

            // If forReservations, skip all data that isn't for a reservation or savings plan operation
            if (forReservations && !(tg.operation instanceof Operation.ReservationOperation || tg.operation instanceof Operation.SavingsPlanOperation))
                continue;

            this.data.put(tg, TimeSeriesData.deserialize(in));
        }
    }
}
