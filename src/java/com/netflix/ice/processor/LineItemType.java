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

import com.netflix.ice.tag.CostType;

public enum LineItemType {
	Credit(CostType.credits),
	DiscountedUsage(CostType.recurring),
	EdpDiscount(null),
	Fee(CostType.subscriptions),
	Refund(CostType.refunds),
	RIFee(CostType.subscriptions),
	RiVolumeDiscount(CostType.credits),
	Tax(CostType.taxes),
	Usage(CostType.recurring),
	SavingsPlanUpfrontFee(CostType.subscriptions),
	SavingsPlanRecurringFee(CostType.recurring),
	SavingsPlanCoveredUsage(CostType.recurring),
	SavingsPlanNegation(null),
	PrivateRateDiscount(CostType.credits);

	private CostType costType;
	
	private LineItemType(CostType costType) {
		this.costType = costType;
	}
	
	public CostType getCostType() {
		return costType;
	}
}
