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
package com.netflix.ice.common;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

/*
 * TagGroupRI adds the reservationArn for usage of a reserved instance to a TagGroup.
 * The reservationArn is used by the ReservationProcessor to properly associate usage
 * of an RI to the reservation so that lending/borrowing, amortization, reassignment of usage
 * and calculation of unused RIs can be done correctly.
 * 
 * reservationArns are only available when processing the new Cost and Usage Reports, not the old
 * Detailed Billing Reports. TagGroupRIs are converted to TagGroups by the ReservationProcessor
 * once RI usage calculations are done, so only TagGroups are serialized to external data files.
 */
public class TagGroupRI extends TagGroupArn<ReservationArn> {
	private static final long serialVersionUID = 1L;
	
	private TagGroupRI(CostType costType, Account account, Region region, Zone zone,
			Product product, Operation operation, UsageType usageType,
			ResourceGroup resourceGroup, ReservationArn reservationArn) {
		super(costType, account, region, zone, product, operation, usageType,
				resourceGroup, reservationArn);
	}
    
    private static Map<TagGroupRI, TagGroupRI> tagGroups = Maps.newConcurrentMap();
    
    public static TagGroupRI get(TagGroup tg) {
    	if (tg instanceof TagGroupRI)
    		return (TagGroupRI) tg;
    	return get(tg.costType, tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup, null);
    }
    
    public static TagGroupRI get(String costType, String account, String region, String zone, String product, String operation, String usageTypeName, String usageTypeUnit, 
    		String[] resourceGroup, String reservationArn, AccountService accountService, ProductService productService) throws BadZone, ResourceException {
        Region r = Region.getRegionByName(region);
        return get(
        	CostType.get(costType),
    		accountService.getAccountByName(account),
        	r, StringUtils.isEmpty(zone) ? null : r.getZone(zone),
        	productService.getProductByServiceCode(product),
        	Operation.getOperation(operation),
            UsageType.getUsageType(usageTypeName, usageTypeUnit),
            ResourceGroup.getResourceGroup(resourceGroup),
            ReservationArn.get(reservationArn));   	
    }
    
    public static TagGroupRI get(CostType costType, Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup, ReservationArn reservationArn) {
        TagGroupRI newOne = new TagGroupRI(costType, account, region, zone, product, operation, usageType, resourceGroup, reservationArn);
        TagGroupRI oldOne = tagGroups.get(newOne);
        if (oldOne != null) {
            return oldOne;
        }
        else {
            tagGroups.put(newOne, newOne);
            return newOne;
        }
    }

    @Override
    public TagGroupRI withCostType(CostType ct) {
        return get(ct, account, region, zone, product, operation, usageType, resourceGroup, arn);
    }
    
    @Override
    public TagGroupRI withoutResourceGroup() {
    	return get(costType, account, region, zone, product, operation, usageType, null, arn);
    }
}
