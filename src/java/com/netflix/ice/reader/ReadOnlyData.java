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

import com.google.common.collect.Lists;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Operation.SavingsPlanOperation;
import com.netflix.ice.tag.Zone.BadZone;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadOnlyData extends ReadOnlyGenericData<Double> {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    public ReadOnlyData(int numUserTags) {
        super(new Double[][]{}, Lists.<TagGroup>newArrayList(), numUserTags);
    }
    
    public ReadOnlyData(Double[][] data, List<TagGroup> tagGroups, int numUserTags) {
        super(data, tagGroups, numUserTags);
    }

	@Override
	protected Double[][] newDataMatrix(int size) {
		return new Double[size][];
	}

	@Override
	protected Double[] newDataArray(int size) {
		return new Double[size];
	}

	@Override
	protected Double readValue(DataInput in) throws IOException {
		Double v = in.readDouble();
		return v == null || v == 0 ? null : v;
	}
	
	@Override
    public void deserialize(AccountService accountService, ProductService productService, DataInput in, boolean forReservations) throws IOException, BadZone {
    	super.deserialize(accountService, productService, in, !forReservations);
    	
    	if (forReservations) {
    		//Strip out all data that isn't for a reservation or savings plan operation
    		
    		// Build a column map index
    		List<Integer> columnMap = Lists.newArrayList();
            for (int i = 0; i < tagGroups.size(); i++) {
            	if (tagGroups.get(i).operation instanceof ReservationOperation || tagGroups.get(i).operation instanceof SavingsPlanOperation)
            		columnMap.add(i);
            }

            // Copy the tagGroups
    		List<TagGroup> newTagGroups = Lists.newArrayList();
    		for (int i: columnMap)
            	newTagGroups.add(tagGroups.get(i));
            this.tagGroups = newTagGroups;
            
    		// Copy the data
            for (int i = 0; i < data.length; i++)  {
            	Double[] oldData = data[i];
            	Double[] newData = null;
            	if (oldData != null) {            		
            		newData = newDataArray(columnMap.size());
	            	for (int j = 0; j < columnMap.size(); j++)
	            		newData[j] = oldData[columnMap.get(j)];
            	}
	            data[i] = newData;
            }
        	buildIndecies();
    	}
    }

}
