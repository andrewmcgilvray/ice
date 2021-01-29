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

import static org.junit.Assert.*;

import java.io.File;
import java.util.Date;

import org.junit.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class CostAndUsageReportLineItemTest {
    private static final String resourcesDir = "src/test/resources";

	String[] item = {
			"somelineitemid",	// LineItemId
			"Anniversary",		// BillType
			"234567890123",		// PayerAccountId
			"234567890123",		// UsageAccountId
			"DiscountedUsage",	// LineItemType
			"2017-09-01T00:00:00Z",	// UsageStartDate
			"2017-09-01T01:00:00Z", // UsageEndDate
			"APS2-InstanceUsage:db.t2.micro", // UsageType
			"CreateDBInstance:0014", // Operation
			"ap-southeast-2", // AvailabilityZone
			"arn:aws:rds:ap-southeast-2:123456789012:db:ss1v3i6xr3d1hss", // ResourceId
			"1.0000000000", // UsageAmount
			"", // NormalizationFactor
			"0.0000000000", // UnblendedRate
			"0.0000000000", // UnblendedCost
			"PostgreSQL, db.t2.micro reserved instance applied", // LineItemDescription
			"Amazon Relational Database Service", // ProductName
			"0.5", // normalizationSizeFactor
			"APS2-InstanceUsage:db.t2.micro", // usagetype
			"Partial Upfront", // PurchaseOption
			"0.0280000000", // publicOnDemandCost
			"Reserved", // term
			"Hrs", // unit
			"Prod", // resourceTags/user:Environment
	};
	
	@Test
	public void testGetUsageQuantity() {
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
		CostAndUsageReport caur = new CostAndUsageReport(s3ObjectSummary, new File(resourcesDir, "LineItemTest-Manifest.json"), null, "");
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		li.setItems(item);
		assertEquals("Usage quantity is incorrect", Double.parseDouble(li.getUsageQuantity()), 1.0, 0.001);
	}
	
	@Test
	public void testResourceTags() {
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
		CostAndUsageReport caur = new CostAndUsageReport(s3ObjectSummary, new File(resourcesDir, "LineItemTest-Manifest.json"), null, "");
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		li.setItems(item);
		assertEquals("Wrong resource tags size", 1, li.getResourceTagsSize());
		String[] tagsHeader = li.getResourceTagsHeader();
		assertEquals("Wrong tags header size", 1, tagsHeader.length);
		assertEquals("Wrong tags header value", "user:Environment", tagsHeader[0]);
		assertEquals("Wrong resource tag value", "Prod", li.getResourceTag(0));
		assertEquals("Wrong tags string", "Prod", li.getResourceTags().get("Environment"));
	}

}
