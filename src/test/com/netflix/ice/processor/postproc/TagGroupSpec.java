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
package com.netflix.ice.processor.postproc;

import org.apache.commons.lang.StringUtils;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;

public class TagGroupSpec {
	String costType;
	String account;
	String region;
	String zone;
	String productServiceCode;
	String operation;
	String usageType;
	String[] resourceGroup;
	CostAndUsage value;
	
	public TagGroupSpec(String costType, String account, String region, String zone, String product, String operation, String usageType, String[] resourceGroup, double cost, double usage) {
		this.costType = costType;
		this.account = account;
		this.region = region;
		this.zone = zone;
		this.productServiceCode = product;
		this.operation = operation;
		this.usageType = usageType;
		this.resourceGroup = resourceGroup;
		this.value = new CostAndUsage(cost, usage);
	}

	public TagGroupSpec(String costType, String account, String region, String product, String operation, String usageType, String[] resourceGroup, double cost, double usage) {
		this.costType = costType;
		this.account = account;
		this.region = region;
		this.productServiceCode = product;
		this.operation = operation;
		this.usageType = usageType;
		this.value = new CostAndUsage(cost, usage);
		this.resourceGroup = resourceGroup;
	}

	public TagGroupSpec(String costType, String account, String region, String product, String operation, String usageType, String[] resourceGroup) {
		this.costType = costType;
		this.account = account;
		this.region = region;
		this.productServiceCode = product;
		this.operation = operation;
		this.usageType = usageType;
		this.value = null;
		this.resourceGroup = resourceGroup;
	}

	public TagGroupSpec(String costType, String account, String region, String product, String operation, String usageType, double cost, double usage) {
		this.costType = costType;
		this.account = account;
		this.region = region;
		this.productServiceCode = product;
		this.operation = operation;
		this.usageType = usageType;
		this.value = new CostAndUsage(cost, usage);
		this.resourceGroup = null;
	}

	public TagGroup getTagGroup(AccountService as, ProductService ps) throws Exception {
		return TagGroup.getTagGroup(costType, account, region, zone, productServiceCode, operation, usageType, "", resourceGroup, as, ps);
	}
	
	public TagGroup getTagGroup(String account, AccountService as, ProductService ps) throws Exception {
		return TagGroup.getTagGroup(costType, account, region, zone, productServiceCode, operation, usageType, "", resourceGroup, as, ps);
	}
	
	public String toString() {
		return "[" + 
				account + "," +
				region + "," +
				zone + "," +
				productServiceCode + "," +
				operation + "," +
				usageType + "," +
				"[" + StringUtils.join(resourceGroup) + "]" +
				"]";
	}

	static public void loadData(TagGroupSpec[] dataSpecs, DataSerializer data, int hour, AccountService as, ProductService ps) throws Exception {
        for (TagGroupSpec spec: dataSpecs)
        	data.put(hour, spec.getTagGroup(as, ps), spec.value);
    }

}
