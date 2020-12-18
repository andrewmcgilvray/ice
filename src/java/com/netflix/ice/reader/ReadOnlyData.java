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

public class ReadOnlyData extends ReadOnlyGenericData<ReadOnlyData.Data> {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static class Data {
    	private double[] cost;
    	private double[] usage;
    	
    	public Data(int size) {
    		cost = new double[size];
    		usage = new double[size];
    	}
    	
    	public Data(double[] cost, double[] usage) {
    		this.cost = cost;
    		this.usage = usage;
    	}
    	
    	public int size() {
    		return Math.max(cost.length, usage.length);
    	}
    	
    	public double[] getCost() {
    		return cost;
    	}
    	
    	public double[] getUsage() {
    		return usage;
    	}
    	
    	public void add(Data from) {
            for (int i = 0; i < from.cost.length; i++) {
            	cost[i] += from.cost[i];
            	usage[i] += from.usage[i];
            }
    	}
    	
    	public boolean hasCostData() {
        	// Check for values in the data array and ignore if all zeros
            for (int i = 0; i < cost.length; i++) {
            	if (cost[i] != 0.0)
            		return true;
            }
            return false;
    	}
    	
    	public boolean hasUsageData() {
        	// Check for values in the data array and ignore if all zeros
            for (int i = 0; i < usage.length; i++) {
            	if (usage[i] != 0.0)
            		return true;
            }
            return false;
    	}
    }
    
    
    
    public ReadOnlyData(int numUserTags) {
        super(new Data[]{}, Lists.<TagGroup>newArrayList(), numUserTags);
    }
    
    public ReadOnlyData(Data[] data, List<TagGroup> tagGroups, int numUserTags) {
        super(data, tagGroups, numUserTags);
    }
    
	@Override
	protected Data[] newDataMatrix(int size) {
		return new Data[size];
	}
	
	@Override
	protected Data readDataArray(DataInput in) throws IOException {
        Data data = new Data(tagGroups.size());
        double[] cost = data.getCost();
        double[] usage = data.getUsage();
        for (int i = 0; i < tagGroups.size(); i++) {
            cost[i] = in.readDouble();
            usage[i] = in.readDouble();
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
            	Data oldData = data[i];
            	Data newData = null;
            	if (oldData != null) {            		
            		newData = new Data(columnMap.size());
            		double[] oldCost = oldData.getCost();
            		double[] oldUsage = oldData.getUsage();
            		double[] newCost = newData.getCost();
            		double[] newUsage = newData.getUsage();
            		
	            	for (int j = 0; j < columnMap.size(); j++) {
	            		newCost[j] = oldCost[columnMap.get(j)];
	            		newUsage[j] = oldUsage[columnMap.get(j)];
	            	}
            	}
	            data[i] = newData;
            }
        	buildIndecies();
    	}
    }

}
