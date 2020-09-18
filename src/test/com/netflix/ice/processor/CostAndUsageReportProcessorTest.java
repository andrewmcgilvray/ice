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
import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.netflix.ice.common.LineItem;

public class CostAndUsageReportProcessorTest {
    private static final String resourcesDir = "src/test/resources";

	@Test
	public void testReportName() {
		String bucketPrefix = "prefix/hourly-cost-and-usage";
		String reportName = CostAndUsageReportProcessor.reportName(bucketPrefix);
		assertEquals("Incorrect report name", "hourly-cost-and-usage", reportName);
	}

	private LineItem getLineItem(String line) throws IOException {
		CostAndUsageReportProcessor cauProc = new CostAndUsageReportProcessor(null);
		File manifest = new File(resourcesDir + "/manifestTest.json");
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
        CostAndUsageReport cauReport = new CostAndUsageReport(s3ObjectSummary, manifest, cauProc, "");
        LineItem lineItem = new CostAndUsageReportLineItem(true, null, cauReport);
		lineItem.setItems(line.split(","));
		return lineItem;
	}
	
	@Test
	public void testLineItem() throws IOException {
		String cau1 = "somelineitemid,2017-08-01T00:00:00Z/2017-08-01T01:00:00Z,,AWS,Anniversary,123456789012,2017-08-01T00:00:00Z,2017-09-01T00:00:00Z,234567890123,DiscountedUsage,2017-08-01T00:00:00Z,2017-08-01T01:00:00Z,AmazonEC2,USW2-BoxUsage:c3.4xlarge,RunInstances,us-west-2a,i-02345901991a472d6,1.00000000,32.0,32.0,USD,0.0000000000,0.00000000,0.2632173639,0.26321736,\"Linux/UNIX (Amazon VPC) c3.4xlarge reserved instance applied\",,Amazon Elastic Compute Cloud,,,,,,,,,,2.8 GHz,,Yes,,,,,,,,,,,,,55,,,Yes,,,,,,,,,,,,,,Compute optimized,c3.4xlarge,c3,,,No License required,US West (Oregon),AWS Region,,,,,,,30 GiB,,,,,,High,32,Linux,RunInstances,,,Intel Xeon E5-2680 v2 (Ivy Bridge),,NA,,64-bit,Intel AVX; Intel Turbo,Compute Instance,,,,,us-west-2,,,,,,,,AmazonEC2,Amazon Elastic Compute Cloud,9GHZN7VCNV2MGV4N,,,,2 x 160 SSD,,,,,Shared,,,,,,USW2-BoxUsage:c3.4xlarge,16,,,,,,1yr,standard,Partial Upfront,0.8400000000,0.8400000000,Reserved,Hrs,,,arn:aws:ec2:us-west-2:123456789012:reserved-instances/aaaaaaaa-1942-qqqq-xxxx-cec03ddb1234,,,,,,,,,,";
		LineItem lineItem = getLineItem(cau1);
		
		assertEquals("AccountID wrong", "234567890123", lineItem.getAccountId());	    
		assertEquals("Product is wrong", "Amazon Elastic Compute Cloud", lineItem.getProduct());	    
		assertEquals("Region is wrong", "us-west-2", lineItem.getProductRegion().name);	    
		assertEquals("Zone is wrong", "us-west-2a", lineItem.getZone());	    
		assertEquals("Reserved is wrong", "Reserved", lineItem.getReserved());	    
		assertEquals("Description is wrong", "\"Linux/UNIX (Amazon VPC) c3.4xlarge reserved instance applied\"", lineItem.getDescription());	    
		assertEquals("UsageType is wrong", "USW2-BoxUsage:c3.4xlarge", lineItem.getUsageType());	    
		assertEquals("Operation is wrong", "RunInstances", lineItem.getOperation());	    
		assertEquals("UsageQuantity is wrong", Double.parseDouble(lineItem.getUsageQuantity()), 1.0, 0.001);	    
		assertEquals("StartTime is wrong", "2017-08-01T00:00:00Z", lineItem.getStartTime());		
		assertEquals("EndTime is wrong", "2017-08-01T01:00:00Z", lineItem.getEndTime());		
		assertEquals("Rate is wrong", "0.2632173639", lineItem.getRate());		
		assertEquals("Cost is wrong", "0.26321736", lineItem.getCost());		
		assertEquals("Resource is wrong", "i-02345901991a472d6", lineItem.getResource());		
		assertTrue("HasResources is wrong", lineItem.hasResources());		
		assertEquals("Start millis is wrong " + lineItem.getStartMillis(), lineItem.getStartMillis(), 1501545600000L);		
		assertEquals("End millis is wrong " + lineItem.getEndMillis(), lineItem.getEndMillis(), 1501549200000L); 
		
		String tagsHeader = StringUtils.join(lineItem.getResourceTagsHeader(), ",");
		assertEquals("Resource tags header is wrong", tagsHeader, "aws:createdBy,user:Environment,user:Email,user:BusinessUnit,user:Name,user:Project,user:Product");
		
		// items defined above starts out with no tags
		assertEquals("Resource tag size is wrong", 0, lineItem.getResourceTagsSize());		
		assertEquals("ResourceTag is wrong", "", lineItem.getResourceTag(0));		
		assertEquals("ResourceTagString is wrong", 0, lineItem.getResourceTags().size());		
		assertTrue("IsReserved is wrong", lineItem.isReserved());
	}

}
