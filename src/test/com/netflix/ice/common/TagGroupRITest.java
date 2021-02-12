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

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.UsageType;

public class TagGroupRITest {

	@Test
	public void testCompareTo() {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		TagGroup tg1 = TagGroup.getTagGroup(
				CostType.recurring,
				as.getAccountById("111111111234", ""),
				Region.US_EAST_1, 
				null, 
				ps.getProduct("AWS Relational Database Service", "AmazonRDS"), 
				Operation.getOperation("CreateDBInstance"), 
				UsageType.getUsageType("RDS:GP2-Storage", "GB"), 
				null);

		TagGroup tg2 = TagGroupRI.get(
				CostType.recurring, 
				as.getAccountById("111111111234", ""), 
				Region.US_EAST_1, 
				null, 
				ps.getProduct("AWS Relational Database Service", "AmazonRDS"), 
				Operation.getOperation("CreateDBInstance"), 
				UsageType.getUsageType("RDS:GP2-Storage", "GB"), 
				null,
				ReservationArn.get("arn"));

		assertTrue("TagGroupRI should be greater than TagGroup", tg1.compareTo(tg2) < 0);
	}

}
