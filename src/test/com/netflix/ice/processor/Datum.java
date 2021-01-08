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
package com.netflix.ice.processor;

import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

public class Datum {
	private final static ProductService productService = new BasicProductService();
	
	public static Zone ap_southeast_2a;
	public static Zone us_east_1a;
	
	static {
		try {
			ap_southeast_2a = Region.AP_SOUTHEAST_2.getZone("ap-southeast-2a");
			us_east_1a = Region.US_EAST_1.getZone("us-east-1a");
		} catch(BadZone e) {};
	}

	public TagGroup tagGroup;
	public CostAndUsage cau;
	
	public Datum(TagGroup tagGroup, double cost, double usage)
	{
		this.tagGroup = tagGroup;
		this.cau = new CostAndUsage(cost, usage);
	}
	
	public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, double cost, double usage)
	{
		this.tagGroup = TagGroup.getTagGroup(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), null);
		this.cau = new CostAndUsage(cost, usage);
	}
	
	public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, String resources, double cost, double usage) throws ResourceException
	{
		ResourceGroup rg = resources == null ? null : ResourceGroup.getResourceGroup(resources.split(",", -1));
		this.tagGroup = TagGroup.getTagGroup(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), rg);
		this.cau = new CostAndUsage(cost, usage);
	}
	
	public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, String resources, ReservationArn rsvArn, double cost, double usage) throws ResourceException
	{
		ResourceGroup rg = resources == null ? null : ResourceGroup.getResourceGroup(resources.split(",", -1));
		this.tagGroup = TagGroupRI.get(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), rg, rsvArn);
		this.cau = new CostAndUsage(cost, usage);
	}
	
	public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, String resources, SavingsPlanArn spArn, double cost, double usage) throws ResourceException
	{
		ResourceGroup rg = resources == null ? null : ResourceGroup.getResourceGroup(resources.split(",", -1));
		this.tagGroup = TagGroupSP.get(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), rg, spArn);
		this.cau = new CostAndUsage(cost, usage);
	}
	
	public String toString() {
		return tagGroup.toString() + ":" + cau.toString();
	}
	
	public TagGroup getTagGroupWithoutResources() {
		return tagGroup.withResourceGroup(null);
	}
}
