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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.Tag;

public class SavingsPlanProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private CostAndUsageData data;
    private AccountService accountService;
	
	public SavingsPlanProcessor(CostAndUsageData costAndUsageData, AccountService accountService) {
		this.data = costAndUsageData;
		this.accountService = accountService;
	}
	
	public void process(Product product) {
		if (!data.hasSavingsPlans())
			return;

    	logger.info("---------- Process " + data.getSavingsPlans().size() + " Savings Plans for " + (product == null ? "Non-resource" : product));

		if (data.get(product) == null) {
			logger.warn("   No data for " + product);
			return;
		}
		
		for (int i = 0; i < data.getNum(product); i++) {
			// For each hour of usage...
			processHour(product, i);
		}		
	}
	
	private void processHour(Product product, int hour) {
		DataSerializer ds = data.get(product);
	    Map<TagGroup, DataSerializer.CostAndUsage> dataMap = ds.getData(hour);
		Map<SavingsPlanArn, SavingsPlan> savingsPlans = data.getSavingsPlans();

		List<TagGroupSP> spTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: dataMap.keySet()) {
	    	if (!(tagGroup instanceof TagGroupSP) || (product != null && product != tagGroup.product) || !tagGroup.operation.isBonus())
	    		continue;
	    	
	    	spTagGroups.add((TagGroupSP) tagGroup);
	    }
	    	    
	    for (TagGroupSP bonusTg: spTagGroups) {	    	
	    	// Split the effective cost into recurring and amortization pieces if appropriate.
	    	SavingsPlan sp = savingsPlans.get(bonusTg.arn);
	    	
	    	if (sp == null) {
	    		logger.error("No savings plan in the map at hour " + hour + " for tagGroup: " + bonusTg);
	    		continue;
	    	}
	    	DataSerializer.CostAndUsage cau = ds.remove(hour, bonusTg);
	    	
    		String accountId = sp.tagGroup.arn.getAccountId();
	    	if (sp.paymentOption != PurchaseOption.NoUpfront) {
	    		// Add amortization
	    		Operation amortOp = null;
	    		if (accountId.equals(bonusTg.account.getId())) {
	    			amortOp = Operation.getSavingsPlanAmortized(sp.paymentOption);
	    		}
	    		else {
	    			amortOp = Operation.getSavingsPlanBorrowedAmortized(sp.paymentOption);
	    			// Create Lent records for account that owns the savings plan
	        		TagGroup tg = TagGroup.getTagGroup(accountService.getAccountById(accountId), bonusTg.region, bonusTg.zone, bonusTg.product, Operation.getSavingsPlanLentAmortized(sp.paymentOption), bonusTg.usageType, bonusTg.resourceGroup);
	    	    	ds.add(hour, tg, cau.cost * sp.normalizedAmortization, 0);
	    		}	    		
	    		
	    		TagGroup tg = bonusTg.withOperation(amortOp);
    	    	ds.add(hour, tg, cau.cost * sp.normalizedAmortization, 0);
	    	}
	    	
    		Operation op = null;
    		if (accountId.equals(bonusTg.account.getId())) {
    			op = Operation.getSavingsPlanUsed(sp.paymentOption);
    		}
    		else {
    			op = Operation.getSavingsPlanBorrowed(sp.paymentOption);
    			
    			// Create Lent records for account that owns the savings plan
        		TagGroup tg = TagGroup.getTagGroup(accountService.getAccountById(accountId), bonusTg.region, bonusTg.zone, bonusTg.product, Operation.getSavingsPlanLent(sp.paymentOption), bonusTg.usageType, bonusTg.resourceGroup);
    	    	ds.add(hour, tg, cau.cost * sp.normalizedRecurring, cau.usage);
    		}
    		
    		TagGroup tg = bonusTg.withOperation(op);
	    	ds.add(hour, tg, cau.cost * sp.normalizedRecurring, cau.usage);
	    }
	    
	    // Scan the usage and cost maps to clean up any leftover entries with TagGroupSP
	    cleanup(hour, ds, savingsPlans);
	}
		
	private void cleanup(int hour, DataSerializer ds, Map<SavingsPlanArn, SavingsPlan> savingsPlans) {
	    List<TagGroupSP> spTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: ds.getTagGroups(hour)) {
	    	if (tagGroup instanceof TagGroupSP) {
	    		spTagGroups.add((TagGroupSP) tagGroup);
	    	}
	    }
	    
	    Map<Tag, Integer> leftovers = Maps.newHashMap();
	    for (TagGroupSP tg: spTagGroups) {
	    	Integer i = leftovers.get(tg.operation);
	    	i = 1 + ((i == null) ? 0 : i);
	    	leftovers.put(tg.operation, i);
	    	
//	    	if (tg.operation.isBonus()) {
//	    		logger.info("Bonus savings plan at hour " + hour + ": " + savingsPlans.get(tg.arn));
//	    	}

	    	DataSerializer.CostAndUsage v = ds.remove(hour, tg);
	    	TagGroup newTg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup);
	    	ds.add(hour, newTg, v);
	    }
	    for (Tag t: leftovers.keySet()) {
	    	logger.info("Found " + leftovers.get(t) + " unconverted SP TagGroups on hour " + hour + " for operation " + t);
	    }
	}
}
