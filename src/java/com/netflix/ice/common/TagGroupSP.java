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
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

/*
 * TagGroupSP adds the SavingsPlanArn for covered usage of an instance to a TagGroup.
 * The SavingsPlanArn is used by the SavingsPlanProcessor to split amortization and
 * recurring fees from the effective cost values.
 */
public class TagGroupSP extends TagGroupArn {
	private static final long serialVersionUID = 1L;
	
	private TagGroupSP(Account account, Region region, Zone zone,
			Product product, Operation operation, UsageType usageType,
			ResourceGroup resourceGroup, SavingsPlanArn savingsPlanArn) {
		super(account, region, zone, product, operation, usageType,
				resourceGroup, savingsPlanArn);
	}
    
    private static Map<TagGroupSP, TagGroupSP> tagGroups = Maps.newConcurrentMap();
    
    public SavingsPlanArn getArn() {
    	return (SavingsPlanArn) arn;
    }
    
    public static TagGroupSP get(TagGroup tg) {
    	if (tg instanceof TagGroupSP)
    		return (TagGroupSP) tg;
    	return get(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup, null);
    }
    
    public static TagGroupSP get(String account, String region, String zone, String product, String operation, String usageTypeName, String usageTypeUnit, String resourceGroup, String savingsPlanArn, AccountService accountService, ProductService productService) throws BadZone {
        Region r = Region.getRegionByName(region);
        return get(
    		accountService.getAccountByName(account),
        	r, StringUtils.isEmpty(zone) ? null : r.getZone(zone),
        	productService.getProductByName(product),
        	Operation.getOperation(operation),
            UsageType.getUsageType(usageTypeName, usageTypeUnit),
            StringUtils.isEmpty(resourceGroup) ? null : ResourceGroup.getResourceGroup(resourceGroup, resourceGroup.equals(product)),
            SavingsPlanArn.get(savingsPlanArn));   	
    }
    
    public static TagGroupSP get(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup, SavingsPlanArn savingsPlanArn) {
        TagGroupSP newOne = new TagGroupSP(account, region, zone, product, operation, usageType, resourceGroup, savingsPlanArn);
        TagGroupSP oldOne = tagGroups.get(newOne);
        if (oldOne != null) {
            return oldOne;
        }
        else {
            tagGroups.put(newOne, newOne);
            return newOne;
        }
    }

}
