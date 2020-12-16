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

public class ReadOnlyData extends ReadOnlyGenericData<double[]> {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public ReadOnlyData(int numUserTags) {
        super(new double[][]{}, Lists.<TagGroup>newArrayList(), numUserTags);
    }
    
    public ReadOnlyData(double[][] data, List<TagGroup> tagGroups, int numUserTags) {
        super(data, tagGroups, numUserTags);
    }
    
	@Override
	protected double[][] newDataMatrix(int size) {
		return new double[size][];
	}
	
	@Override
	protected double[] readDataArray(DataInput in) throws IOException {
        double[] data = new double[tagGroups.size()];
        for (int j = 0; j < tagGroups.size(); j++) {
            double v = in.readDouble();
            if (v != 0) {
                data[j] = v;
            }
        }
        return data;
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
            	double[] oldData = data[i];
            	double[] newData = null;
            	if (oldData != null) {            		
            		newData = new double[columnMap.size()];
	            	for (int j = 0; j < columnMap.size(); j++)
	            		newData[j] = oldData[columnMap.get(j)];
            	}
	            data[i] = newData;
            }
        	buildIndecies();
    	}
    }

}
