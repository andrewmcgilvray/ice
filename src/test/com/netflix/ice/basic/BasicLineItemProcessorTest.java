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
package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.netflix.ice.basic.BasicLineItemProcessor.ReformedMetaData;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.LineItem.BillType;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.CostAndUsageReportLineItemProcessor;
import com.netflix.ice.processor.CostAndUsageReportLineItem;
import com.netflix.ice.processor.CostAndUsageReportProcessor;
import com.netflix.ice.processor.CostAndUsageReport;
import com.netflix.ice.processor.DetailedBillingReportLineItem;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.Instances;
import com.netflix.ice.processor.LineItemProcessor.Result;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.UserTag;

public class BasicLineItemProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final String resourcesDir = "src/test/resources";
    private static final String manifest2017 = "manifestTest.json";
    private static final String manifest2018 = "manifest-2018-01.json";
    private static final String manifest2019 = "manifest-2019-01.json";
    private static final String manifest2019a = "manifest-2019-12.json";
    
    private static final String ec2 = Product.Code.Ec2.serviceName;
    private static final String rds = Product.Code.RdsFull.serviceName;
    private static final String es = Product.Code.Elasticsearch.serviceName;
    private static final String ec = Product.Code.ElastiCache.serviceName;
    private static final String redshift = Product.Code.Redshift.serviceName;
    private static final String awsConfig = "AWSConfig";
    
    private static final String[] emptyUserTags = { "", "" };

    static final String[] dbrHeader = {
		"InvoiceID","PayerAccountId","LinkedAccountId","RecordType","RecordId","ProductName","RateId","SubscriptionId","PricingPlanId","UsageType","Operation","AvailabilityZone","ReservedInstance","ItemDescription","UsageStartDate","UsageEndDate","UsageQuantity","BlendedRate","BlendedCost","UnBlendedRate","UnBlendedCost"
    };


    public static AccountService accountService = null;
    private static ProductService productService = null;
    public static CostAndUsageReportLineItem cauLineItem;
    public static ResourceService resourceService;

    @BeforeClass
	public static void beforeClass() throws Exception {
    	init(new BasicAccountService());
    }
    
    public static void init(AccountService as) throws Exception {
    	accountService = as;
    	
		productService = new BasicProductService();

		cauLineItem = newCurLineItem(manifest2017, null);
        
		String[] customTags = new String[]{ "Environment", "Email" };
		resourceService = new BasicResourceService(productService, customTags, false);
	}
    
    private static CostAndUsageReportLineItem newCurLineItem(String manifestFilename, DateTime costAndUsageNetUnblendedStartDate) throws IOException {
		CostAndUsageReportProcessor cauProc = new CostAndUsageReportProcessor(null);
		File manifest = new File(resourcesDir, manifestFilename);
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
        CostAndUsageReport cauReport = new CostAndUsageReport(s3ObjectSummary, manifest, cauProc);
        return new CostAndUsageReportLineItem(false, costAndUsageNetUnblendedStartDate, cauReport);
    }
    
    public BasicLineItemProcessor newBasicLineItemProcessor() {
    	return newBasicLineItemProcessor(cauLineItem, null);
    }
	
    public BasicLineItemProcessor newBasicLineItemProcessor(LineItem lineItem, Reservation reservation) {
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, PurchaseOption.PartialUpfront);
		if (reservation != null)
			reservationService.injectReservation(reservation);
    	
    	resourceService.initHeader(lineItem.getResourceTagsHeader(), "123456789012");
    	if (lineItem instanceof CostAndUsageReportLineItem)
    		return new CostAndUsageReportLineItemProcessor(accountService, productService, reservationService, resourceService);
    	else
    		return new BasicLineItemProcessor(accountService, productService, reservationService, resourceService);    	
    }
    
    private ReformedMetaData testReform(Line line, PurchaseOption purchaseOption) throws IOException {
		CostAndUsageReportLineItem lineItem = newCurLineItem(manifest2017, null);
		lineItem.setItems(line.getCauLine(lineItem));
		return newBasicLineItemProcessor().reform(lineItem, purchaseOption);
    }
    
    @Test
    public void testGetRegion() throws IOException {
    	CostAndUsageReportLineItemProcessor lineItemProcessor = new CostAndUsageReportLineItemProcessor(accountService, productService, null, resourceService);
    	CostAndUsageReportLineItem lineItem = newCurLineItem(manifest2017, null);
    	
    	// Test case where region is in usage-type prefix
    	Line line = new Line(LineItemType.Usage, "global", "us-west-2", "AWS Systems Manager", "USW2-AWS-Auto-Steps-Tier1", null, null, null, null, null, null, null, null);
    	lineItem.setItems(line.getCauLine(lineItem));
    	Region r = lineItemProcessor.getRegion(lineItem);    	
    	assertEquals("Wrong region from usage type", Region.US_WEST_2, r);
    	
    	// Case where region should be pulled from the availability zone
    	line = new Line(LineItemType.Usage, "global", "us-west-2", "AWS Systems Manager", "AWS-Auto-Steps-Tier1", null, null, null, null, null, null, null, null);
    	lineItem.setItems(line.getCauLine(lineItem));
    	r = lineItemProcessor.getRegion(lineItem);    	
    	assertEquals("Wrong region from availability zone", Region.US_WEST_2, r);
    	
    	// Case where region should default to us-east-1
    	line = new Line(LineItemType.Usage, "", "", "AWS Systems Manager", "AWS-Auto-Steps-Tier1", null, null, null, null, null, null, null, null);
    	lineItem.setItems(line.getCauLine(lineItem));
    	r = lineItemProcessor.getRegion(lineItem);    	
    	assertEquals("Wrong region from availability zone", Region.US_EAST_1, r);
    	
    	// Case where region should come from product/region
    	line = new Line(LineItemType.Usage, "eu-west-1", "", "AWS Systems Manager", "AWS-Auto-Steps-Tier1", null, null, null, null, null, null, null, null);
    	lineItem.setItems(line.getCauLine(lineItem));
    	r = lineItemProcessor.getRegion(lineItem);    	
    	assertEquals("Wrong region from availability zone", Region.EU_WEST_1, r);
    }
    
	@Test
	public void testReformEC2Spot() throws IOException {
		Line line = new Line(ec2, "RunInstances:SV001", "USW2-SpotUsage:c4.large", "c4.large Linux/UNIX Spot Instance-hour in US West (Oregon) in VPC Zone #1", null, "0.02410000", null);
	    ReformedMetaData rmd = testReform(line, PurchaseOption.NoUpfront);
	    assertTrue("Operation should be spot instance but got " + rmd.operation, rmd.operation == Operation.spotInstances);
	}

	@Test
	public void testReformEC2ReservedPartialUpfront() throws IOException {
		Line line = new Line(ec2, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "0.34", null);
	    ReformedMetaData rmd = testReform(line, PurchaseOption.PartialUpfront);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	}

	@Test
	public void testReformEC2ReservedPartialUpfrontWithPurchaseOption() throws IOException {
		Line line = new Line(ec2, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "0.34", "Partial Upfront");
	    ReformedMetaData rmd = testReform(line, PurchaseOption.NoUpfront);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	}

	@Test
	public void testReformRDSReservedAllUpfront() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.0", null);
	    ReformedMetaData rmd = testReform(line, PurchaseOption.AllUpfront);
	    assertTrue("Operation should be All instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesAllUpfront);
	}

	@Test
	public void testReformRDSReservedAllUpfrontWithPurchaseOption() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.0", "All Upfront");
	    ReformedMetaData rmd = testReform(line, PurchaseOption.NoUpfront);
	    assertTrue("Operation should be All instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesAllUpfront);
	}

	@Test
	public void testReformRDSReservedPartialUpfront() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", PricingTerm.reserved, "0.021", null);
	    ReformedMetaData rmd = testReform(line, PurchaseOption.PartialUpfront);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.012", null);	    
	    rmd = testReform(line, PurchaseOption.PartialUpfront);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	@Test
	public void testReformRDSReservedPartialUpfrontWithPurchaseOption() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", PricingTerm.reserved, "0.021", "Partial Upfront");
	    ReformedMetaData rmd = testReform(line, PurchaseOption.NoUpfront);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.012", "Partial Upfront");	    
	    rmd = testReform(line, PurchaseOption.NoUpfront);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	private boolean tagMatches(Tag tag, String expect) {
		if (tag == null) {
			return expect == null;
		}
		if (tag instanceof Product)
			return ((Product)tag).getIceName().equals(expect);
		return tag.name.equals(expect);
	}
	
	private boolean resourceGroupMatches(ResourceGroup rg, String[] expect) {
		if (rg == null) {
			return expect == null;
		}
		if (expect == null)
			return false;
		
		UserTag[] tags = rg.getUserTags();
		for (int i = 0; i < tags.length; i++) {
			if (tags[i] == null) {
				if (expect[i] != null)
					return false;
			}
			else if (expect[i] == null || !tags[i].name.equals(expect[i]))
				return false;
		}
		return true;
	}
	
	private final int regionIndex = 0;
	private final int zoneIndex = 1;
	private final int productIndex = 2;
	private final int operationIndex = 3;
	private final int usageTypeIndex = 4;
		
	public static enum PricingTerm {
		onDemand,
		reserved,
		spot,
		none
	}
	
	public class Line {
		static final int numDbrItems = 21;
		
		public BillType billType = BillType.Anniversary;
		public LineItemType lineItemType;
		public String account;
		public String region;
		public String zone;
		public String product;
		public String productCode = "";
		public String operation;
		public String type;
		public String description;
		public PricingTerm term;
		public String start;	// Start date in ISO format
		public String end;	// End date in ISO format
		public String quantity;
		public String cost;
		public String purchaseOption;
		public String reservationARN;
		public String resource = "";
		public String environment = "";
		public String email = "";
		public String amortization = "";
		public String recurring = "";
		public String publicOnDemandCost = "";
		public String amortizedUpfrontFeeForBillingPeriod = "";
		public String unusedQuantity = "";
		public String unusedAmortizedUpfrontFeeForBillingPeriod = "";
		public String unusedRecurringFee = "";
		public String numberOfReservations = "";
		public String reservationStartTime = "";
		public String reservationEndTime = "";
		public String normalizationFactor = "";
		public String savingsPlanAmortizedUpfrontCommitmentForBillingPeriod = "";
		public String savingsPlanRecurringCommitmentForBillingPeriod = "";
		public String savingsPlanStartTime = "";
		public String savingsPlanEndTime = "";
		public String savingsPlanArn = "";
		public String savingsPlanEffectiveCost = "";
		public String savingsPlanTotalCommitmentToDate = "";
		public String savingsPlanUsedCommitment = "";
		public String savingsPlanPaymentOption = "";
		public String taxType = "";
		public String legalEntity = "";
		
		
		// For basic testing
		public Line(LineItemType lineItemType, String region, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption) {
			init(lineItemType, "234567890123", region, zone, product, type, operation, 
				description, term, start, end, quantity, cost, purchaseOption, term == PricingTerm.reserved ? "arn" : "");
		}
		
		// For testing reform method in BasicLineItemProcessor
		public Line(String product, String operation, String type, String description, PricingTerm term, String cost, String purchaseOption) {
			init(null, null, null, null, product, type, operation, 
					description, term, null, null, null, cost, purchaseOption, term == PricingTerm.reserved ? "arn" : "");
			lineItemType = LineItemType.Usage;
		}
								
		private void init(LineItemType lineItemType, String account, String region, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption,
				String reservationARN) {
			this.lineItemType = lineItemType;
			this.account = account;
			this.region = region;
			this.zone = zone;
			this.product = product;
			this.operation = operation;
			this.type = type;
			this.description = description;
			this.term = term;
			this.start = start;
			this.end = end;
			this.quantity = quantity;
			this.cost = cost;
			this.purchaseOption = purchaseOption;
			this.reservationARN = reservationARN;
		}
		
		// For resource testing
		public void setResources(String resource, String environment, String email) {
			this.resource = resource;
			this.environment = environment;
			this.email = email;
		}
		
		// For DiscountedUsage testing
		public void setDiscountedUsageFields(String amortization, String recurring, String publicOnDemandCost) {
			this.amortization = amortization;
			this.recurring = recurring;
			this.publicOnDemandCost = publicOnDemandCost;
		}
		
		// For RIFee testing
		public void setRIFeeFields(String amortizedUpfrontFeeForBillingPeriod, String unusedQuantity, String unusedAmortizedUpfrontFeeForBillingPeriod, String unusedRecurringFee,
				String numberOfReservations, String reservationStartTime, String reservationEndTime) {
			this.amortizedUpfrontFeeForBillingPeriod = amortizedUpfrontFeeForBillingPeriod;
			this.unusedQuantity = unusedQuantity;
			this.unusedAmortizedUpfrontFeeForBillingPeriod = unusedAmortizedUpfrontFeeForBillingPeriod;
			this.unusedRecurringFee = unusedRecurringFee;
			this.numberOfReservations = numberOfReservations;
			this.reservationStartTime = reservationStartTime;
			this.reservationEndTime = reservationEndTime;
			if (this.purchaseOption != null && !this.purchaseOption.isEmpty()) {
				logger.error("Don't set purchase option in RIFee records - AWS has these blank");
				this.purchaseOption = "";
			}
		}
		public void setNormalizationFactor(String normalizationFactor) {
			this.normalizationFactor = normalizationFactor;
		}
		public void setProductCode(String productCode) {
			this.productCode = productCode;
		}
		public void setBillType(BillType billType) {
			this.billType = billType;
		}
		
		// For SavingsPlan testing
		public void setSavingsPlanRecurringFeeFields(
				String savingsPlanAmortizedUpfrontCommitmentForBillingPeriod,
				String savingsPlanRecurringCommitmentForBillingPeriod,
				String savingsPlanStartTime,
				String savingsPlanEndTime,
				String savingsPlanArn,
				String savingsPlanTotalCommitmentToDate,
				String savingsPlanUsedCommitment,
				String savingsPlanPaymentOption) {
			this.savingsPlanAmortizedUpfrontCommitmentForBillingPeriod = savingsPlanAmortizedUpfrontCommitmentForBillingPeriod;
			this.savingsPlanRecurringCommitmentForBillingPeriod = savingsPlanRecurringCommitmentForBillingPeriod;
			this.savingsPlanStartTime = savingsPlanStartTime;
			this.savingsPlanEndTime = savingsPlanEndTime;
			this.savingsPlanArn = savingsPlanArn;
			this.savingsPlanTotalCommitmentToDate = savingsPlanTotalCommitmentToDate;
			this.savingsPlanUsedCommitment = savingsPlanUsedCommitment;
			this.savingsPlanPaymentOption = savingsPlanPaymentOption;
		}
		
		public void setSavingsPlanCoveredUsageFields(
				String savingsPlanStartTime,
				String savingsPlanEndTime,
				String savingsPlanArn,
				String savingsPlanEffectiveCost,
				String savingsPlanPaymentOption,
				String publicOnDemandCost) {
			this.savingsPlanStartTime = savingsPlanStartTime;
			this.savingsPlanEndTime = savingsPlanEndTime;
			this.savingsPlanArn = savingsPlanArn;
			this.savingsPlanEffectiveCost = savingsPlanEffectiveCost;
			this.savingsPlanPaymentOption = savingsPlanPaymentOption;
			this.publicOnDemandCost = publicOnDemandCost;
		}
		
		// For Tax testing
		public void setTaxFields(String taxType, String legalEntity) {
			this.taxType = taxType;
			this.legalEntity = legalEntity;
		}
		
		String[] getDbrLine() {
			String[] items = new String[Line.numDbrItems];
			for (int i = 0; i < items.length; i++)
				items[i] = "";
			items[1] = account; // payer account
			items[2] = account;
			items[11] = zone;
			items[5] = product;
			items[10] = operation;
			items[9] = type;
			items[13] = description;
			items[12] = term == PricingTerm.reserved ? "Y" : "";
			items[14] = LineItem.amazonBillingDateFormat.print(LineItem.amazonBillingDateFormatISO.parseDateTime(start));
			items[15] = LineItem.amazonBillingDateFormat.print(LineItem.amazonBillingDateFormatISO.parseDateTime(end));
			items[16] = quantity;
			items[20] = cost;
			return items;
		}
		
		public String[] getCauLine(CostAndUsageReportLineItem lineItem) {
	        String[] items = new String[lineItem.size()];
			for (int i = 0; i < items.length; i++)
				items[i] = "";
			items[lineItem.getBillTypeIndex()] = billType.name();
			items[lineItem.getPayerAccountIdIndex()] = account;
			items[lineItem.getAccountIdIndex()] = account;
			items[lineItem.getLineItemProductCodeIndex()] = productCode;
			items[lineItem.getZoneIndex()] = zone;
			items[lineItem.getProductRegionIndex()] = region;
			items[lineItem.getProductIndex()] = product;
			items[lineItem.getOperationIndex()] = operation;
			items[lineItem.getUsageTypeIndex()] = type;
			items[lineItem.getProductUsageTypeIndex()] = type;
			items[lineItem.getDescriptionIndex()] = description;
			items[lineItem.getReservedIndex()] = term == PricingTerm.reserved ? "Reserved" : term == PricingTerm.spot ? "" : term == PricingTerm.onDemand ? "OnDemand" : "";
			items[lineItem.getStartTimeIndex()] = start;
			items[lineItem.getEndTimeIndex()] = end;
			items[lineItem.getUsageQuantityIndex()] = quantity;
			items[lineItem.getLineItemNormalizationFactorIndex()] = normalizationFactor;
			items[lineItem.getLineItemTypeIndex()] = lineItemType.name();
			items[lineItem.getCostIndex()] = cost;
			items[lineItem.getPurchaseOptionIndex()] = purchaseOption;
			items[lineItem.getReservationArnIndex()] = reservationARN;
			items[lineItem.getResourceIndex()] = resource;
			if (lineItem.getResourceTagStartIndex() + 2 < items.length) {
				items[lineItem.getResourceTagStartIndex() + 1] = environment;
				items[lineItem.getResourceTagStartIndex() + 2] = email;
			}
			
			switch (lineItemType) {
			case RIFee:
				// RIFee is used to get recurring and amortization fees for unused reservations
				set(lineItem.getAmortizedUpfrontFeeForBillingPeriodIndex(), items, amortizedUpfrontFeeForBillingPeriod);
				int unusedIndex = lineItem.getUnusedQuantityIndex();
				if (unusedIndex >= 0) {
					items[unusedIndex] = unusedQuantity;
					items[lineItem.getUnusedRecurringFeeIndex()] = unusedRecurringFee;
					items[lineItem.getUnusedAmortizedUpfrontFeeForBillingPeriodIndex()] = unusedAmortizedUpfrontFeeForBillingPeriod;
				}
				if (lineItem.getReservationNumberOfReservationsIndex() >= 0)
					items[lineItem.getReservationNumberOfReservationsIndex()] = numberOfReservations;
				if (lineItem.getReservationStartTimeIndex() >= 0) {
					items[lineItem.getReservationStartTimeIndex()] = reservationStartTime;
					items[lineItem.getReservationEndTimeIndex()] = reservationEndTime;
				}
				break;
			
			case DiscountedUsage:
				set(lineItem.getAmortizedUpfrontCostForUsageIndex(), items, amortization);
				set(lineItem.getRecurringFeeForUsageIndex(), items, recurring);
				int publicOnDemandCostIndex = lineItem.getPublicOnDemandCostIndex();
				if (publicOnDemandCostIndex >= 0)
					items[publicOnDemandCostIndex] = this.publicOnDemandCost;
				//items[lineItem.getCostIndex()] = "0"; // Discounted usage doesn't carry cost
				break;
				
			case SavingsPlanRecurringFee:
				set(lineItem.getSavingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex(), items, savingsPlanAmortizedUpfrontCommitmentForBillingPeriod);
				set(lineItem.getSavingsPlanRecurringCommitmentForBillingPeriodIndex(), items, savingsPlanRecurringCommitmentForBillingPeriod);
				set(lineItem.getSavingsPlanStartTimeIndex(), items, savingsPlanStartTime);
				set(lineItem.getSavingsPlanEndTimeIndex(), items, savingsPlanEndTime);
				set(lineItem.getSavingsPlanArnIndex(), items, savingsPlanArn);
				set(lineItem.getSavingsPlanTotalCommitmentToDateIndex(), items, savingsPlanTotalCommitmentToDate);
				set(lineItem.getSavingsPlanUsedCommitmentIndex(), items, savingsPlanUsedCommitment);
				set(lineItem.getSavingsPlanPaymentOptionIndex(), items, savingsPlanPaymentOption);
				break;
				
			case SavingsPlanCoveredUsage:
				set(lineItem.getSavingsPlanStartTimeIndex(), items, savingsPlanStartTime);
				set(lineItem.getSavingsPlanEndTimeIndex(), items, savingsPlanEndTime);
				set(lineItem.getSavingsPlanArnIndex(), items, savingsPlanArn);
				set(lineItem.getSavingsPlanEffectiveCostIndex(), items, savingsPlanEffectiveCost);
				set(lineItem.getSavingsPlanPaymentOptionIndex(), items, savingsPlanPaymentOption);
				set(lineItem.getPublicOnDemandCostIndex(), items, publicOnDemandCost);
				break;
				
			case Tax:
				set(lineItem.getTaxTypeIndex(), items, taxType);
				set(lineItem.getLegalEntityIndex(), items, legalEntity);
				break;
				
			default:
				break;
			}
			
			return items;
		}
		
		private void set(int index, String[] items, String value) {
			if (index >= 0)
				items[index] = value;
		}
	}
		
	public static enum Which {
		dbr,
		cau,
		both
	}

	public class ProcessTest {
		public Line line;
		private String[] expectedTag;
		private Double usage; // non-null if we expect a usageTag
		private Double cost; // non-null if we expect a costTag
		private Result result;
		private int daysInMonth;
		private String[] expectedResourceGroup = null;
		private Product product = null;
		private Double resourceCost = null;
		private Double amortization = null;
		private Double savings = null;
		private boolean delayed = false;
		private int numExpectedUsageTags;
		private int numExpectedCostTags;
		private int numExpectedResourceCostTags;
		private Reservation reservation = null;
		private Double unusedCost = null;
				
		public ProcessTest(Line line, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth) {
			this.line = line;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.numExpectedUsageTags = this.expectedTag == null || this.usage == null ? 0 : 1;
			this.numExpectedCostTags = this.expectedTag == null || this.cost == null ? 0 : 1;
		}
		
		// Constructor for testing RIFee line item types
		public ProcessTest(Line line, String[] expectedTag, Double usage, Result result, int daysInMonth, Double amortization, boolean delayed, int numExpectedUsageTags, int numExpectedCostTags, Double cost, Double unusedCost) {
			this.line = line;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost; // used if before Jan 1, 2018
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.amortization = amortization;
			this.delayed = delayed;
			this.numExpectedUsageTags = numExpectedUsageTags;
			this.numExpectedCostTags = numExpectedCostTags;
			this.unusedCost = unusedCost; // used if after Jan 1, 2018
		}
		
		public ProcessTest(Line line, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth, Double amortization, Double savings) {
			this.line = line;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.amortization = amortization;
			this.savings = savings;
			this.numExpectedUsageTags = this.expectedTag == null || this.usage == null ? 0 : 1;
			this.numExpectedCostTags = this.expectedTag == null || this.cost == null ? 0 : 1;
			
			DateTime dt = new DateTime(line.start, DateTimeZone.UTC);

			if (this.amortization != null && dt.getYear() >= 2018 && this.amortization > 0.0)
				this.numExpectedCostTags++;
			if (this.savings != null)
				this.numExpectedCostTags++;
		}
		
		public void setResources(String[] expectedResourceGroup, Product product, Double resourceCost, int numExpectedResourceCostTags) {
			this.expectedResourceGroup = expectedResourceGroup;
			this.product = product;
			this.resourceCost = resourceCost;
			this.numExpectedResourceCostTags = numExpectedResourceCostTags;
		}
		
		public void addReservation(Reservation res) {
			this.reservation = res;
		}
	
		public void run(Which which) throws Exception {
			run(which, "2017-06-01T00:00:00Z", null);
		}
		
		// For testing taxes which have the full month for the interval, but need to only be split across the hours in the partial month
		public void run(Which which, String start, String netUnblendedStart, String reportDate) throws Exception {
			DateTime dt = new DateTime(start, DateTimeZone.UTC);
			long startMilli = dt.withDayOfMonth(1).getMillis();
			long reportMilli = new DateTime(reportDate, DateTimeZone.UTC).getMillis();
			
			if (which == Which.dbr || which == Which.both) {
		        LineItem dbrLineItem = new DetailedBillingReportLineItem(false, true, dbrHeader);
		        dbrLineItem.setItems(line.getDbrLine());
				runProcessTest(dbrLineItem, Which.dbr, false, startMilli, reportMilli);
			}
			
			if (which == Which.cau || which == Which.both) {
				String manifest = "";
				switch(dt.getYear()) {
				case 2018:
					manifest = manifest2018;
					break;
				case 2019:
				case 2020:
					manifest = dt.getMonthOfYear() == 12 ? manifest2019a : manifest2019;
					break;
				default:
					manifest = manifest2017;
					break;
				}

				CostAndUsageReportLineItem lineItem = newCurLineItem(manifest, netUnblendedStart == null ? null : new DateTime(netUnblendedStart, DateTimeZone.UTC));
				lineItem.setItems(line.getCauLine(lineItem));
				runProcessTest(lineItem, Which.cau, true, startMilli, reportMilli);
			}
		}
		
		public void run(Which which, String start, String netUnblendedStart) throws Exception {        
			run(which, start, netUnblendedStart, new DateTime(DateTimeZone.UTC).toString());
		}
		
		public void runProcessTest(LineItem lineItem, Which which, boolean isCostAndUsageReport, long startMilli, long reportMilli) throws Exception {
			Instances instances = null;
			String reportName = which == Which.dbr ? "DBR" : "CUR";
			CostAndUsageData costAndUsageData = new CostAndUsageData(startMilli, null, null, TagCoverage.none, accountService, productService);
			
			BasicLineItemProcessor lineItemProc = newBasicLineItemProcessor(lineItem, reservation);
			
			if (delayed) {
				// Make sure we have one hour of cost and usage data so monthly fees get tallied properly
				costAndUsageData.getCost(null).getData(0);
				costAndUsageData.getUsage(null).getData(0);
			}
			Result result = lineItemProc.process("", reportMilli, delayed, "", lineItem, costAndUsageData, instances, 0.0);
			assertEquals(reportName + " Incorrect result", this.result, result);
			
			if (result == Result.delay) {
				// Expand the data by number of hours in month
				costAndUsageData.getUsage(null).getData(daysInMonth * 24 - 1);
				costAndUsageData.getCost(null).getData(daysInMonth * 24 - 1);
				result = lineItemProc.process("", reportMilli, true, "", lineItem, costAndUsageData, instances, 0.0);
			}
			
			// Check usage data
			logger.info("Test:");
			int gotLen = costAndUsageData.getUsage(null).getTagGroups().size();
			for (TagGroup tg: costAndUsageData.getUsage(null).getTagGroups())
				logger.info(" - usage: " + costAndUsageData.getUsage(null).get(0, tg) + ", " + tg);
			assertEquals(reportName + " Incorrect number of usage tags", numExpectedUsageTags, gotLen);
			if (gotLen > 0) {
				TagGroup got = (TagGroup) costAndUsageData.getUsage(null).getTagGroups().toArray()[0];
				//logger.info("Got Tag: " + got);
				String errors = checkTag(got, expectedTag, null);
				assertTrue(reportName + " Tag is not correct: " + errors, errors.isEmpty());
				double usage = costAndUsageData.getUsage(null).get(0, got);
				assertEquals(reportName + " Usage is incorrect", usage, usage, 0.001);
			}
			// Check cost data
			gotLen = costAndUsageData.getCost(null).getTagGroups().size();
			for (TagGroup tg: costAndUsageData.getCost(null).getTagGroups())
				logger.info(" - cost: " + costAndUsageData.getCost(null).get(0, tg) + ", " + tg);
			assertEquals(reportName + " Incorrect number of cost tags", numExpectedCostTags, gotLen);
			if (gotLen > 0) {
				checkCostAndUsage(costAndUsageData, which, reportName, null, isCostAndUsageReport, null);
			}
			
			// Check resource cost data
			if (product != null && resourceCost != null) {
				gotLen = costAndUsageData.getCost(product).getTagGroups().size();
				assertEquals(reportName + " Incorrect number of resource cost tags", numExpectedResourceCostTags, gotLen);
				if (gotLen > 0) {
					checkCostAndUsage(costAndUsageData, which, reportName, product, isCostAndUsageReport, expectedResourceGroup);
				}
			}
			
			// Check reservations if any
			if (lineItem.getLineItemType() == LineItemType.RIFee && lineItem.getStartMillis() >= CostAndUsageReportLineItemProcessor.jan1_2018) {
				// We should have a Reservation record for the RIFee line item
				assertEquals("Wrong number of reservations", 1, costAndUsageData.getReservations().size());
				Reservation r = costAndUsageData.getReservations().values().iterator().next();
				String errors = checkTag(r.tagGroup, expectedTag, expectedResourceGroup);
				assertTrue(reportName + " Tag is not correct: " + errors, errors.isEmpty());
				assertEquals("wrong reservation amortization", amortization, r.hourlyFixedPrice * r.count, 0.001);
				assertEquals("wrong reservation recurring fee", cost, r.usagePrice * r.count, 0.001);
			}
		}
		
		private void checkCostAndUsage(CostAndUsageData costAndUsageData, Which which, String reportName, Product p, boolean isCostAndUsageReport, String[] resourceGroup) {
			ReadWriteData costData = costAndUsageData.getCost(p);
			for (TagGroup tg: costData.getTagGroups()) {
				// check for proper TagGroup type
				if (tg.operation.isSavingsPlan()) {
					if (!tg.operation.isUnused() && !tg.operation.isSavings())
					assertTrue(reportName + " TagGroup is not instance of TagGroupSP", tg instanceof TagGroupSP);
				}
				else if (which == Which.cau && tg.operation.name.contains("Credit")) {
					assertFalse(reportName + " TagGroup is instance of TagGroupRI", tg instanceof TagGroupRI);
				}
				else if (which == Which.cau && !tg.operation.isSpot() && !tg.product.isDynamoDB() && !tg.product.isSupport() && !tg.product.isEc2()) {
					assertTrue(reportName + " TagGroup is not instance of TagGroupRI", tg instanceof TagGroupRI);
				}

				// check for matching operation
				if (tg.operation.isAmortized() && !tg.operation.isSavingsPlan() && amortization != null) {
					String[] amortizedTag = expectedTag.clone();
					amortizedTag[operationIndex] = ReservationOperation.getAmortized(((ReservationOperation) tg.operation).getPurchaseOption()).name;
					String errors = checkTag(tg, amortizedTag, resourceGroup);
					assertTrue(reportName + " Amortization Tag is not correct: " + errors, errors.length() == 0);
					double cost = costData.get(0, tg);
					assertEquals(reportName + " Cost is incorrect", amortization, cost, 0.001);				
				}
				else if (tg.operation.isUnused() && tg.operation.isAmortized() && amortization != null) {
					String[] amortizedTag = expectedTag.clone();
					amortizedTag[operationIndex] = Operation.getSavingsPlanUnusedAmortized(((Operation.SavingsPlanOperation) tg.operation).getPaymentOption()).name;
					String errors = checkTag(tg, amortizedTag, resourceGroup);
					assertTrue(reportName + " Amortization Tag is not correct: " + errors, errors.length() == 0);
					double cost = costData.get(0, tg);
					assertEquals(reportName + " Cost is incorrect", amortization, cost, 0.001);				
				}
				else if (tg.operation.isSavingsPlan() && tg.operation.isSavings() && savings != null) {
					String[] savingsTag = expectedTag.clone();
					savingsTag[operationIndex] = Operation.getSavingsPlanSavings(((Operation.SavingsPlanOperation) tg.operation).getPaymentOption()).name;
					String errors = checkTag(tg, savingsTag, resourceGroup);
					assertTrue(reportName + " Savings Tag is not correct: " + errors, errors.length() == 0);
					double cost = costData.get(0, tg);
					assertEquals(reportName + " Cost is incorrect", savings, cost, 0.001);				
				}
				else if (tg.operation.isSavings() && savings != null) {
					String[] savingsTag = expectedTag.clone();
					savingsTag[operationIndex] = ReservationOperation.getSavings(((ReservationOperation) tg.operation).getPurchaseOption()).name;
					String errors = checkTag(tg, savingsTag, resourceGroup);
					assertTrue(reportName + " Savings Tag is not correct: " + errors, errors.length() == 0);
					double cost = costData.get(0, tg);
					assertEquals(reportName + " Cost is incorrect", savings, cost, 0.001);				
				}
				else if ((tg.operation.isUnused() && !tg.operation.isAmortized()) && unusedCost != null) {
					String errors = checkTag(tg, expectedTag, resourceGroup);
					assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);					
					double cost = costData.get(0, tg);
					assertEquals(reportName + " Cost is incorrect", unusedCost, cost, 0.001);				
				}
				else {
					String errors = checkTag(tg, expectedTag, resourceGroup);
					assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);					
					double cost = costData.get(0, tg);
					assertEquals(reportName + " Cost is incorrect", this.cost, cost, 0.001);				
				}
				if (product != null && product.isEc2Instance() && isCostAndUsageReport) {
					assertTrue("Tag group is wrong type", tg instanceof TagGroupRI || tg.operation.isSpot() || tg.operation.isOnDemand());
				}
			}
		}
		
		private String checkTag(TagGroup tagGroup, String[] tags, String[] resourceGroup) {
			StringBuilder errors = new StringBuilder();
			if (!tagMatches(tagGroup.account, "234567890123"))
				errors.append("Account mismatch[ expected: " + "234567890123" + ", got " + tagGroup.account + "], ");
			if (!tagMatches(tagGroup.region, tags[regionIndex]))
				errors.append("Region mismatch[ expected: " + tags[regionIndex] + ", got " + tagGroup.region + "], ");
			if (!tagMatches(tagGroup.zone, tags[zoneIndex]))
				errors.append("Zone mismatch[ expected: " + tags[zoneIndex] + ", got " + tagGroup.zone + "], ");
			if (!tagMatches(tagGroup.product, tags[productIndex]))
				errors.append("Product mismatch[ expected: " + tags[productIndex] + ", got " + tagGroup.product + "], ");
			if (!tagMatches(tagGroup.operation, tags[operationIndex]))
				errors.append("Operation mismatch[ expected: " + tags[operationIndex] + ", got " + tagGroup.operation + "], ");
			if (!tagMatches(tagGroup.usageType, tags[usageTypeIndex]))
				errors.append("UsageType mismatch[ expected: " + tags[usageTypeIndex] + ", got " + tagGroup.usageType + "], ");
			if (!resourceGroupMatches(tagGroup.resourceGroup, resourceGroup)) {
				List<String> rg = resourceGroup == null ? null : Lists.newArrayList(resourceGroup);
				errors.append("ResourceGroup mismatch[ expected: " + rg + ", got " + tagGroup.resourceGroup + "], ");
			}
			String ret = errors.toString();
			if (!ret.isEmpty()) // strip final ", "
				ret = ret.substring(0, ret.length() - 2);
			return ret;
		}		
	}
	
	@Test
	public void testReservedAllUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - All Upfront", "c4.2xlarge.windows" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run(Which.dbr);
		test = new ProcessTest(line, tag, 1.0, null, Result.hourly, 30);
		test.run(Which.cau);
		
		// Test 2017 manifest which doesn't support amortization. Savings will come back as full price
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "3.0");
		test = new ProcessTest(line, tag, 1.0, null, Result.hourly, 30);
		test.run(Which.cau);
		
		// Test 2018 which does support amortization
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2018-06-01T00:00:00Z", "2018-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "3.0");
		test = new ProcessTest(line, tag, 1.0, null, Result.hourly, 30, 1.5, 1.5);
		test.run(Which.cau, "2018-06-01T00:00:00Z", null);
	}
	
	@Test
	public void testReservedPartialUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("1.0", "1.0", "3.0");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.34, Result.hourly, 30);
		test.run(Which.dbr);
	}
	
	@Test
	public void testReservedNoUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.45 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.45", "No Upfront");
		line.setDiscountedUsageFields("0", "0.45", "0.60");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - No Upfront", "c4.2xlarge.windows" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.45, Result.hourly, 31, null, 0.15);
		test.run(Which.cau, "2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Test with resource tags
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] resourceGroup = {"Prod", "john.doe@foobar.com" };
		test = new ProcessTest(line, tag, 1.0, 0.45, Result.hourly, 31, null, 0.15);
		test.setResources(resourceGroup, productService.getProduct(Product.Code.Ec2Instance), 0.0, 2);
		test.run(Which.cau, "2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "0.25", "0.085", "Partial Upfront");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows" };
		ProcessTest test = new ProcessTest(line, tag, 0.25, 0.085, Result.hourly, 30);
		test.run(Which.dbr);
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamilyRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", rds, "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0002", "MySQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "0.5", "0.0", "Partial Upfront");
		line.setDiscountedUsageFields("0.00739", "0.00902", "0.026");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.mysql" };
		ProcessTest test = new ProcessTest(line, tag, 0.25, 0.00902, Result.hourly, 31, 0.00739, 0.00959);
		test.run(Which.cau, "2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows" };
		String[] resourceGroup = { "Prod", "john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.34, Result.hourly, 30);
		test.setResources(resourceGroup, productService.getProduct(Product.Code.Ec2Instance), 0.0, 1);
		test.run(Which.cau);
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.34, Result.hourly, 30);
		test.run(Which.cau);
		
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("0.32", "0.36", "1.02");
		tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows" };
		test = new ProcessTest(line, tag, 1.0, 0.36, Result.hourly, 31, 0.32, 0.34);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("0.32", "0.36", "1.02");
		tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows" };
		test = new ProcessTest(line, tag, 1.0, 0.36, Result.hourly, 31, 0.32, 0.34);
		test.run(Which.cau, "2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

	}
	
	@Test
	public void testRIFeeNoUpfront() throws Exception {
		// Test Cost and Usage Prior to Jan 1, 2018 (Uses Reservations pulled by Capacity Poller, so we inject one)
		Line line = new Line(LineItemType.RIFee, "eu-west-1", "", ec2, "EU-HeavyUsage:t2.small", "RunInstances", "USD 0.0146 hourly fee per Linux/UNIX (Amazon VPC), t2.small instance", PricingTerm.none, "2017-11-01T00:00:00Z", "2017-12-01T00:00:00Z", "18600", "271.56", "");
		line.setRIFeeFields("", "", "", "", "25", "", "");
		String[] tag = new String[] { "eu-west-1", null, "EC2 Instance", "Bonus RIs - No Upfront", "t2.small" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.monthly, 31, 0.0, true, 0, 0, 0.377, null);
		TagGroupRI tg = TagGroupRI.get("234567890123", tag[0], tag[1], tag[2], tag[3], tag[4], "hours", null, "arn", accountService, productService);
		Reservation r = new Reservation(tg, 25, 0, 0, PurchaseOption.NoUpfront, 0.0, 0.028);
		test.addReservation(r);
		test.run(Which.cau, "2017-11-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Test Cost and Usage After Jan 1, 2018 (Uses RIFee data in CUR)
		// 25 RIs at 0.0146/hr recurring
		line = new Line(LineItemType.RIFee, "eu-west-1", "", ec2, "EU-HeavyUsage:t2.small", "RunInstances", "USD 0.0146 hourly fee per Linux/UNIX (Amazon VPC), t2.small instance", PricingTerm.none, "2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z", "18600", "271.56", "");
		line.setRIFeeFields("0", "0", "0", "0", "25", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "eu-west-1", null, "EC2 Instance", "Used RIs - No Upfront", "t2.small" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 0.0, true, 0, 0, 0.365, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");		
	}
	
	@Test
	public void testRIFeePartialUpfront() throws Exception {
		// Test case before we had support for Amortization
		// 1 RI with 1.0/hr recurring and 2.0/hr upfront
		Line line = new Line(LineItemType.RIFee, "ap-southeast-2", "", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "720.0", "");
		line.setRIFeeFields("1440", "", "", "", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		String[] tag = new String[] { "ap-southeast-2", null, "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.monthly, 30, 2.0, true, 0, 0, 1.0, null);
		test.run(Which.cau);

		// Test reservation/unused rates
		// 1 RI with 1.0/hr recurring and 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "720", "720.0", "");
		line.setRIFeeFields("1440", "0", "0", "0", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "ap-southeast-2", null, "EC2 Instance", "Used RIs - Partial Upfront", "c4.2xlarge.windows" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 2.0, true, 0, 0, 1.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-06-01T00:00:00Z", "2019-02-01T00:00:00Z");
	}
	
	@Test
	public void testReservedMonthlyFeeRDS() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "1440.0", "");
		line.setRIFeeFields("720.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		String[] tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - Partial Upfront", "db.t2.micro.postgres" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.ignore, 30, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "0", "");
		line.setRIFeeFields("2880.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - All Upfront", "db.t2.micro.postgres" };
		test = new ProcessTest(line, tag, null, Result.ignore, 30, 4.0, true, 0, 0, 0.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "2160", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - No Upfront", "db.t2.micro.postgres" };
		test = new ProcessTest(line, tag, null, Result.ignore, 30, 0.0, true, 0, 0, 3.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Partial Upfront Multi-AZ 1 RI at 2.0/hr recurring and 1.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "720", "1440.0", "");
		line.setRIFeeFields("720.0", "0", "0", "0", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("1");
		tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - Partial Upfront", "db.t2.micro.multiaz.postgres" };
		test = new ProcessTest(line, tag, null, Result.ignore, 30, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontHourlyUsageRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "", rds, "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0014", "PostgreSQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "Partial Upfront");
		String[] tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.postgres" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, null, Result.hourly, 30);
		test.run(Which.cau);
	}
	
	@Test
	public void testRIPurchase() throws Exception {
		Line line = new Line(LineItemType.Fee, "ap-southeast-2", "", ec2, "", "", "Sign up charge for subscription: 647735683, planId: 2195643", PricingTerm.reserved, "2017-06-09T21:21:37Z", "2018-06-09T21:21:36Z", "150.0", "9832.500000", "");
		ProcessTest test = new ProcessTest(line, null, 0.0, 0.0, Result.ignore, 30);
		test.run(Which.both);
	}
	
	@Test
	public void testSpot() throws Exception {
		Line line = new Line(LineItemType.Usage, "ap-northeast-2", "", ec2, "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "");
		String[] tag = new String[] { "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.349, Result.hourly, 30);
		test.run(Which.both);
	}
	@Test
	public void testSpotWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.Usage, "ap-northeast-2", "", ec2, "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = { "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge" };
		String[] resourceGroup = { "Prod", "john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.349, Result.hourly, 30);
		test.setResources(resourceGroup, productService.getProduct(Product.Code.Ec2Instance), 0.0, 1);
		test.run(Which.cau);
	}
	
	@Test
	public void testReservedMonthlyFeeElasticsearch() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		String[] tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Used RIs - Partial Upfront", "r4.xlarge.elasticsearch" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "0", "");
		line.setRIFeeFields("2976.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Used RIs - All Upfront", "r4.xlarge.elasticsearch" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 4.0, true, 0, 0, 0.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "2232.0", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Used RIs - No Upfront", "r4.xlarge.elasticsearch" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 0.0, true, 0, 0, 3.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedElasticsearch() throws Exception {
		// Partial Upfront
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Partial Upfront");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		String[] tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Bonus RIs - Partial Upfront", "r4.xlarge.elasticsearch" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("0.30", "0", "0.66");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Bonus RIs - All Upfront", "r4.xlarge.elasticsearch" };
		test = new ProcessTest(line, tag, 1.0, null, Result.hourly, 31, 0.30, 0.36);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "No Upfront");
		line.setDiscountedUsageFields("0", "0.34", "0.66");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Bonus RIs - No Upfront", "r4.xlarge.elasticsearch" };
		test = new ProcessTest(line, tag, 1.0, 0.34, Result.hourly, 31, 0.0, 0.32);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedDynamoDB() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", "Amazon DynamoDB", "WriteCapacityUnit-Hrs", "CommittedThroughput", "DynamoDB, Reserved Write Capacity used this month", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Heavy Utilization");
		line.setDiscountedUsageFields("0.00028082", "0.00020992", "0.0013");
		String[] tag = new String[] { "us-east-1", null, "DynamoDB", "CommittedThroughput", "WriteCapacityUnit-Hrs" };
		// TODO: support DynamoDB amortization
		//test = new ProcessTest(line, tag, 1.0, 0.0, Result.hourly, 31, 0.32, 0.34);
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.0, Result.hourly, 31);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
		
	@Test
	public void testReservedMonthlyFeeElastiCache() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		String[] tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Partial Upfront", "cache.m5.medium.redis" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "0", "");
		line.setRIFeeFields("2976.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - All Upfront", "cache.m5.medium.redis" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 4.0, true, 0, 0, 0.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "2232.0", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - No Upfront", "cache.m5.medium.redis" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 0.0, true, 0, 0, 3.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// Heavy Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Heavy Utilization", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Medium Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "MediumUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Medium Utilization", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Light Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "LightUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Light Utilization", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");		

		// Heavy Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront --- test with different region
		line = new Line(LineItemType.RIFee, "us-west-2", "", ec, "USW2-HeavyUsage:cache.t2.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-west-2", null, "ElastiCache", "Used RIs - Heavy Utilization", "cache.t2.medium.redis" };
		test = new ProcessTest(line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.setResources(emptyUserTags, null, 0.0, 0);
		test.run(Which.cau, "2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
	}
	
	@Test
	public void testReservedElastiCache() throws Exception {
		// Partial Upfront
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Partial Upfront");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		String[] tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Partial Upfront", "cache.m3.medium.redis" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("0.30", "0", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - All Upfront", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, 1.0, null, Result.hourly, 31, 0.30, 0.36);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "No Upfront");
		line.setDiscountedUsageFields("0", "0.34", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - No Upfront", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, 1.0, 0.34, Result.hourly, 31, 0.0, 0.32);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// Heavy Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Heavy Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Heavy Utilization", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Medium Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Medium Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Medium Utilization", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Light Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Light Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Light Utilization", "cache.m3.medium.redis" };
		test = new ProcessTest(line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run(Which.cau, "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
	}
	
	@Test
	public void testEC2Credit() throws Exception {
		Line line = new Line(LineItemType.Credit, "us-east-1", "", ec2, "HeavyUsage:m4.large", "RunInstances", "MB - Pricing Adjustment", PricingTerm.reserved, "2019-08-01T00:00:00Z", "2019-09-01T00:00:00Z", "0.0000000000", "-38.3100000000", "");
		String[] tag = new String[] { "us-east-1", null, "EC2 Instance", "Credit - RI", "m4.large" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.delay, 31, null, false, 0, 1, -0.0515, null);
		test.run(Which.cau, "2019-08-01T00:00:00Z", "2019-01-01T00:00:00Z");				
	}
		
	@Test
	public void testRedshiftCredit() throws Exception {
		// Credits sometimes end one second into next month, so make sure we deal with that
		Line line = new Line(LineItemType.Credit, "us-east-1", "", redshift, "Node:ds2.xlarge", "RunComputeNode:0001", "AWS Credit", PricingTerm.onDemand, "2020-03-01T00:00:00Z", "2020-04-01T00:00:01Z", "0.0000000000", "-38.3100000000", "");
		String[] tag = new String[] { "us-east-1", null, "Redshift", "Credit - On-Demand Instance", "ds2.xlarge" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.delay, 31, null, false, 0, 1, -0.0515, null);
		test.run(Which.cau, "2020-03-01T00:00:00Z", "2019-01-01T00:00:00Z");				
	}
	
	@Test
	public void testConfigCredit() throws Exception {
		Line line = new Line(LineItemType.Credit, "us-west-2", "", awsConfig, "USW2-ConfigurationItemRecorded", "", "AWS Config rules- credits to support pricing model change TT: 123456789012", PricingTerm.none, "2019-08-01T00:00:00Z", "2019-08-01T01:00:01Z", "0.0000000000", "-0.00492", "");
		String[] tag = new String[] { "us-west-2", null, "Config", "Credit - None", "ConfigurationItemRecorded" };
		ProcessTest test = new ProcessTest(line, tag, null, Result.delay, 31, null, false, 0, 1, -0.00492, null);
		test.run(Which.cau, "2019-08-01T00:00:00Z", "2019-01-01T00:00:00Z");				
	}
	
	@Test
	public void testSavingsPlanRecurringFee() throws Exception {
		// Test No Upfront with 50% usage
		Line line = new Line(LineItemType.SavingsPlanRecurringFee, "global", "", "Savings Plans for AWS Compute usage", "ComputeSP:1yrNoUpfront", "", "1 year No Upfront Compute Savings Plan", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.12", "");
		line.setSavingsPlanRecurringFeeFields("0", "0.12", "2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.12", "0.06", "NoUpfront");
		String[] tag = new String[] { "global", null, "Savings Plans for AWS Compute usage", "SavingsPlan Unused - No Upfront", "ComputeSP:1yrNoUpfront" };
		
		// Should produce one cost item for the unused recurring portion of the plan.
		ProcessTest test = new ProcessTest(line, tag, null, Result.hourlyTruncate, 31, 0.0, true, 0, 1, 0.0, 0.06);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");

		
		// Test Partial Upfront with 50% usage
		line = new Line(LineItemType.SavingsPlanRecurringFee, "global", "", "Savings Plans for AWS Compute usage", "ComputeSP:1yrPartialUpfront", "", "1 year No Upfront Compute Savings Plan", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.12", "");
		line.setSavingsPlanRecurringFeeFields("0.07", "0.05", "2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.12", "0.06", "PartialUpfront");
		tag = new String[] { "global", null, "Savings Plans for AWS Compute usage", "SavingsPlan Unused - Partial Upfront", "ComputeSP:1yrPartialUpfront" };
		
		// Should produce two cost items for the unused recurring and amortized portions of the plan.
		test = new ProcessTest(line, tag, null, Result.hourlyTruncate, 31, 0.035, true, 0, 2, 0.0, 0.025);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		
		// Test All Upfront with 50% usage
		line = new Line(LineItemType.SavingsPlanRecurringFee, "global", "", "Savings Plans for AWS Compute usage", "ComputeSP:1yrAllUpfront", "", "1 year No Upfront Compute Savings Plan", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.12", "");
		line.setSavingsPlanRecurringFeeFields("0.12", "0", "2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.12", "0.06", "AllUpfront");
		tag = new String[] { "global", null, "Savings Plans for AWS Compute usage", "SavingsPlan Unused - All Upfront", "ComputeSP:1yrAllUpfront" };
		
		// Should produce one cost item for the unused amortized portion of the plan.
		test = new ProcessTest(line, tag, null, Result.hourlyTruncate, 31, 0.06, true, 0, 1, 0.0, 0.0);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");		
	}
	
	@Test
	public void testSavingsPlanCoveredUsage() throws Exception {
		// Should produce two cost items and one usage item for each case.
		// The cost items should have the effective cost, not the unblended OnDemand cost and the savings.
		// Bonus operations will be split apart by the savings plan processor.

		// No Upfront
		Line line = new Line(LineItemType.SavingsPlanCoveredUsage, "us-east-1", "us-east-1a", "Amazon Elastic Compute Cloud", "BoxUsage:t2.micro", "RunInstances", "$0.0116 per On Demand Linux t2.micro Instance Hour", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.0116", "");
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.0083", "NoUpfront", "0.0116");
		String[] tag = new String[] { "us-east-1", "us-east-1a", "EC2 Instance", "SavingsPlan Bonus - No Upfront", "t2.micro" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.0083, Result.hourly, 31, 0.0, 0.0033);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// Partial Upfront
		line = new Line(LineItemType.SavingsPlanCoveredUsage, "us-east-1", "us-east-1a", "Amazon Elastic Compute Cloud", "BoxUsage:t2.micro", "RunInstances", "$0.0116 per On Demand Linux t2.micro Instance Hour", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.0116", "");
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.0083", "PartialUpfront", "0.0116");
		tag = new String[] { "us-east-1", "us-east-1a", "EC2 Instance", "SavingsPlan Bonus - Partial Upfront", "t2.micro" };
		test = new ProcessTest(line, tag, 1.0, 0.0083, Result.hourly, 31, 0.0, 0.0033);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront
		line = new Line(LineItemType.SavingsPlanCoveredUsage, "us-east-1", "us-east-1a", "Amazon Elastic Compute Cloud", "BoxUsage:t2.micro", "RunInstances", "$0.0116 per On Demand Linux t2.micro Instance Hour", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.0", "");
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.0083", "AllUpfront", "0.0116");
		tag = new String[] { "us-east-1", "us-east-1a", "EC2 Instance", "SavingsPlan Bonus - All Upfront", "t2.micro" };
		test = new ProcessTest(line, tag, 1.0, 0.0083, Result.hourly, 31, 0.0, 0.0033);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront Lambda
		line = new Line(LineItemType.SavingsPlanCoveredUsage, "ap-northeast-1", "", "AWS Lambda", "APN1-Lambda-GB-Second", "Invoke", "AWS Lambda - Total Compute - Asia Pacific (Tokyo)", PricingTerm.onDemand, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "2.4", "0.00004", "");
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.000036", "NoUpfront", "0.00004");
		tag = new String[] { "ap-northeast-1", null, "Lambda", "SavingsPlan Bonus - No Upfront", "Lambda-GB-Second" };
		test = new ProcessTest(line, tag, 2.4, 0.000036, Result.hourly, 31, 0.0, 0.000004);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
	}
	
	@Test
	public void testOCBPremiumSupport() throws Exception {
		Line line = new Line(LineItemType.Fee, "global", "", "AWS Premium Support", "Dollar", "", "AWS Support (Enterprise)", PricingTerm.none, "2019-11-01T00:00:00Z", "2019-12-01T00:00:00Z", "1000000.00", "64500.00", "");
		line.setProductCode("OCBPremiumSupport");
		line.setBillType(BillType.Purchase);
		String[] tag = new String[] { "global", null, "Premium Support", "None", "Dollar" };
		ProcessTest test = new ProcessTest(line, tag, 1000000.0, 64500.0, Result.hourly, 30);
		test.run(Which.cau, "2019-11-01T00:00:00Z", "2019-01-01T00:00:00Z");				
	}
	@Test
	public void testOCBPremiumSupportRefund() throws Exception {
		Line line = new Line(LineItemType.Refund, "global", "", "AWS Premium Support", "Dollar", "", "Discount", PricingTerm.none, "2019-11-01T00:00:00Z", "2019-12-01T00:00:00Z", "0", "-645.00", "");
		line.setProductCode("OCBPremiumSupport");
		line.setBillType(BillType.Refund);
		String[] tag = new String[] { "global", null, "Premium Support", "None", "Dollar" };
		ProcessTest test = new ProcessTest(line, tag, 0.0, -645.0, Result.hourly, 30);
		test.run(Which.cau, "2019-11-01T00:00:00Z", "2019-01-01T00:00:00Z");				
	}
	
	@Test
	public void testTax() throws Exception {
		// Full month
		Line line = new Line(LineItemType.Tax, "", "", "Amazon EC2", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z", "1", "7.44", "");
		line.setTaxFields("GST", "Amazon Web Services, Inc.");
		String[] tag = new String[] { "us-east-1", null, "EC2", "Tax - GST", "HeavyUsage:c4.large" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 0.01, Result.delay, 31);
		test.run(Which.cau, "2020-01-01T00:00:00Z", "2019-01-01T00:00:00Z");				

		// Partial month with tax reported as partial month
		line = new Line(LineItemType.Tax, "", "", "Amazon EC2", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-19T05:00:01Z", "1", "4.37", "");
		line.setTaxFields("USSalesTax", "Amazon Web Services, Inc.");
		tag = new String[] { "us-east-1", null, "EC2", "Tax - USSalesTax", "HeavyUsage:c4.large" };
		test = new ProcessTest(line, tag, 1.0, 0.01, Result.delay, 31);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z", "2019-12-19T05:00:01Z");
		
		// Partial month with tax reported as full month
		line = new Line(LineItemType.Tax, "", "", "Amazon EC2", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2019-12-01T00:00:00Z", "2020-01-01T00:00:00Z", "1", "7.44", "");
		line.setTaxFields("USSalesTax", "Amazon Web Services, Inc.");
		tag = new String[] { "us-east-1", null, "EC2", "Tax - USSalesTax", "HeavyUsage:c4.large" };
		test = new ProcessTest(line, tag, 1.0, 0.01, Result.delay, 31);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z", "2019-12-19T05:00:01Z");
		
		// Zero tax - should ignore
		line = new Line(LineItemType.Tax, "", "", "Amazon EC2", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-19T05:00:01Z", "1", "0", "");
		line.setTaxFields("USSalesTax", "Amazon Web Services, Inc.");
		tag = new String[] { "us-east-1", null, "EC2", "Tax - USSalesTax", "HeavyUsage:c4.large" };
		test = new ProcessTest(line, tag, null, null, Result.ignore, 31);
		test.run(Which.cau, "2019-12-01T00:00:00Z", "2019-01-01T00:00:00Z");		
	}
	
	@Test
	public void testMonthlyVAT() throws Exception {
		Line line = new Line(LineItemType.Tax, "", "", "Amazon EC2", "", "", "Tax for product code AmazonEC2", PricingTerm.none, "2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z", "1", "744.0", "");
		line.setTaxFields("VAT", "AWS EMEA SARL");
		String[] tag = new String[] { "global", null, "EC2", "Tax - VAT", "Tax - AWS EMEA SARL" };
		ProcessTest test = new ProcessTest(line, tag, 1.0, 1.0, Result.delay, 31);
		test.run(Which.cau, "2020-01-01T00:00:00Z", "2019-01-01T00:00:00Z");				
	}
}
