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
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.CostAndUsageReportLineItemProcessor;
import com.netflix.ice.processor.LineItem;
import com.netflix.ice.processor.CostAndUsageReportLineItemProcessor.ReformedMetaData;
import com.netflix.ice.processor.CostAndUsageReportProcessor;
import com.netflix.ice.processor.CostAndUsageReport;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.Instances;
import com.netflix.ice.processor.LineItem.BillType;
import com.netflix.ice.processor.LineItemType;
import com.netflix.ice.processor.LineItemProcessor.Result;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.UserTagKey;

public class CostAndUsageLineItemProcessorTest {
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
    
    private static final String arn = "arn";
    private static final ReservationArn riArn = ReservationArn.get(arn);

	private final Product ec2Product = productService.getProduct(Product.Code.Ec2);
	private final Product ec2Instance = productService.getProduct(Product.Code.Ec2Instance);
	private final Product rdsInstance = productService.getProduct(Product.Code.RdsInstance);
	private final Product elasticsearch = productService.getProduct(Product.Code.Elasticsearch);
	private final Product elastiCache = productService.getProduct(Product.Code.ElastiCache);
	private final Product dynamoDB = productService.getProduct(Product.Code.DynamoDB);
	private final Product redshiftInstance = productService.getProduct(Product.Code.Redshift);
	private final Product config = productService.getProduct("AWSConfig", "AWSConfig");
	private final Product lambda = productService.getProduct(Product.Code.Lambda);
	
	private static Account a2;
	private static String account1 = "123456789012";
	private static String account2 = "234567890123";

    static final String[] dbrHeader = {
		"InvoiceID","PayerAccountId","LinkedAccountId","RecordType","RecordId","ProductName","RateId","SubscriptionId","PricingPlanId","UsageType","Operation","AvailabilityZone","ReservedInstance","ItemDescription","UsageStartDate","UsageEndDate","UsageQuantity","BlendedRate","BlendedCost","UnBlendedRate","UnBlendedCost"
    };


    public static AccountService accountService = null;
    private static ProductService productService = null;
    public static LineItem cauLineItem;
    public static ResourceService resourceService;

    @BeforeClass
	public static void beforeClass() throws Exception {
    	init(new BasicAccountService());
    }
    
    public static void init(AccountService as) throws Exception {
    	accountService = as;
    	a2 = accountService.getAccountById(account2);
    	
		productService = new BasicProductService();

		cauLineItem = newCurLineItem(manifest2017, null);
        
		String[] customTags = new String[]{ "Environment", "Email" };
		resourceService = new BasicResourceService(productService, customTags, false);
		
	}
    
    private static LineItem newCurLineItem(String manifestFilename, DateTime costAndUsageNetUnblendedStartDate) throws IOException {
		CostAndUsageReportProcessor cauProc = new CostAndUsageReportProcessor(null);
		File manifest = new File(resourcesDir, manifestFilename);
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
        CostAndUsageReport cauReport = new CostAndUsageReport(s3ObjectSummary, manifest, cauProc, "");
        return new LineItem(false, costAndUsageNetUnblendedStartDate, cauReport);
    }
    
    public CostAndUsageReportLineItemProcessor newLineItemProcessor() {
    	return newLineItemProcessor(cauLineItem, null);
    }
	
    public CostAndUsageReportLineItemProcessor newLineItemProcessor(LineItem lineItem, Reservation reservation) {
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, PurchaseOption.PartialUpfront);
		if (reservation != null)
			reservationService.injectReservation(reservation);
    	
    	resourceService.initHeader(lineItem.getResourceTagsHeader(), account1);
    	return new CostAndUsageReportLineItemProcessor(accountService, productService, reservationService, resourceService);
    }
    
    private ReformedMetaData testReform(Line line, PurchaseOption purchaseOption) throws IOException {
		LineItem lineItem = newCurLineItem(manifest2017, null);
		lineItem.setItems(line.getCauLine(lineItem));
		return newLineItemProcessor().reform(lineItem, purchaseOption);
    }
    
    @Test
    public void testGetRegion() throws IOException {
    	CostAndUsageReportLineItemProcessor lineItemProcessor = new CostAndUsageReportLineItemProcessor(accountService, productService, null, resourceService);
    	LineItem lineItem = newCurLineItem(manifest2017, null);
    	
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
			init(lineItemType, account2, region, zone, product, type, operation, 
				description, term, start, end, quantity, cost, purchaseOption, term == PricingTerm.reserved ? arn : "");
		}
		
		// For testing reform method in CostAndUsageReportLineItemProcessor
		public Line(String product, String operation, String type, String description, PricingTerm term, String cost, String purchaseOption) {
			init(null, null, null, null, product, type, operation, 
					description, term, null, null, null, cost, purchaseOption, term == PricingTerm.reserved ? arn : "");
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
		
		public String[] getCauLine(LineItem lineItem) {
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

	public class ProcessTest {
		public Line line;
		private Result result;
		private int daysInMonth;
		private Product product = null;
		private boolean delayed = false;
		private Reservation reservation = null;
		private String reportDate = new DateTime(DateTimeZone.UTC).toString();
		private String netUnblendedStart = "2019-01-01T00:00:00Z";
		
		public ProcessTest(Line line, Result result, int daysInMonth) {
			this.line = line;
			this.result = result;
			this.daysInMonth = daysInMonth;
		}
						
		public void setReportDate(String reportDate) {
			this.reportDate = reportDate;
		}
		
		public void setNetUnblendedStart(String netUnblendedStart) {
			this.netUnblendedStart = netUnblendedStart;
		}
		
		public void setDelayed() {
			this.delayed = true;
		}
		
		public void addReservation(Reservation res) {
			this.reservation = res;
		}
	
		public void run(Datum[] expected) throws Exception {
			run("2017-06-01T00:00:00Z", expected, null);
		}
		
		public void run(String start, Datum[] expected) throws Exception {
			run(start, expected, null);
		}
		
		// For testing taxes which have the full month for the interval, but need to only be split across the hours in the partial month
		public void run(String start, Datum[] expected, Datum expectedReservation) throws Exception {
			DateTime dt = new DateTime(start, DateTimeZone.UTC);
			long startMilli = dt.withDayOfMonth(1).getMillis();
			long reportMilli = new DateTime(reportDate, DateTimeZone.UTC).getMillis();
			
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

			LineItem lineItem = newCurLineItem(manifest, new DateTime(netUnblendedStart, DateTimeZone.UTC));
			lineItem.setItems(line.getCauLine(lineItem));
			runProcessTest(lineItem, startMilli, reportMilli, expected, expectedReservation);
		}
				
		private void check(CostAndUsageData costAndUsageData, Product product, Datum[] expected) {
			Map<TagGroup, CostAndUsage> hourData = costAndUsageData.get(product).getData(0);
			assertEquals("cost and usage size wrong", expected == null ? 0 : expected.length, hourData.size());
			if (expected == null)
				return;
			
			for (Datum datum: expected) {
				TagGroup tg = product == null ? datum.getTagGroupWithoutResources() : datum.tagGroup;
				CostAndUsage cau = hourData.get(tg);
				assertNotNull("should have tag group " + datum.tagGroup, cau);	
				assertEquals("wrong cost value for tag " + datum.tagGroup, datum.cau.cost, cau.cost, 0.001);
				assertEquals("wrong usage value for tag " + datum.tagGroup, datum.cau.usage, cau.usage, 0.001);
			}
		}
		
		private void checkReservations(CostAndUsageData costAndUsageData, Datum expected) {
			assertEquals("reservations size wrong", expected == null ? 0 : 1, costAndUsageData.getReservations().size());
			if (expected == null)
				return;
			
			Reservation r = costAndUsageData.getReservations().values().iterator().next();
			
			assertEquals("Tag is not correct", expected.tagGroup.withCostType(CostType.subscription), r.tagGroup);
			assertEquals("wrong reservation amortization", expected.cau.cost, r.hourlyFixedPrice * r.count, 0.001);
			assertEquals("wrong reservation recurring fee", expected.cau.usage, r.usagePrice * r.count, 0.001);
		}
		
		public void runProcessTest(LineItem lineItem, long startMilli, long reportMilli, Datum[] expected, Datum expectedReservation) throws Exception {
			Instances instances = null;
			CostAndUsageData costAndUsageData = new CostAndUsageData(startMilli, null, Lists.<UserTagKey>newArrayList(), TagCoverage.none, accountService, productService);
			
			CostAndUsageReportLineItemProcessor lineItemProc = newLineItemProcessor(lineItem, reservation);
			
			if (delayed) {
				// Make sure we have one hour of cost and usage data so monthly fees get tallied properly
				costAndUsageData.get(null).getData(0);
			}
			Result result = lineItemProc.process("", reportMilli, delayed, "", lineItem, costAndUsageData, instances, 0.0);
			assertEquals("Incorrect result", this.result, result);
			
			if (result == Result.delay) {
				// Expand the data by number of hours in month
				costAndUsageData.get(null).getData(daysInMonth * 24 - 1);
				result = lineItemProc.process("", reportMilli, true, "", lineItem, costAndUsageData, instances, 0.0);
			}
			
			// Check cost and usage data
			logger.info("Test:");
			check(costAndUsageData, null, expected);				
			if (product != null)
				check(costAndUsageData, product, expected);				
								
			// Check reservations if any
			checkReservations(costAndUsageData, expectedReservation);
		}
				
	}
	
	@Test
	public void testReservedAllUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "");
		
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "c4.2xlarge.windows", null, riArn, 0, 1),
		};
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		test.run(expected);
		
		// Test 2017 manifest which doesn't support amortization. Savings will come back as full price
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "3.0");
		test = new ProcessTest(line, Result.hourly, 30);
		test.run(expected);
		
		// Test 2018 which does support amortization
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2018-06-01T00:00:00Z", "2018-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "3.0");
		test = new ProcessTest(line, Result.hourly, 30);

		expected = new Datum[]{
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "c4.2xlarge.windows", null, riArn, 0, 1),
				new Datum(CostType.amortization, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.amortizedAllUpfront, "c4.2xlarge.windows", null, riArn, 1.5, 0),
				new Datum(CostType.savings, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.savingsAllUpfront, "c4.2xlarge.windows", null, riArn, 1.5, 0),
		};
		test.run("2018-06-01T00:00:00Z", expected);
	}
	
	@Test
	public void testReservedNoUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.45 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.45", "No Upfront");
		line.setDiscountedUsageFields("0", "0.45", "0.60");
		ProcessTest test = new ProcessTest(line, Result.hourly, 31);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesNoUpfront, "c4.2xlarge.windows", null, riArn, 0.45, 1),
				new Datum(CostType.savings, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.savingsNoUpfront, "c4.2xlarge.windows", null, riArn, 0.15, 0),
		};
		test.run("2019-01-01T00:00:00Z", expected);
		
		// Test with resource tags
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String rg = "Prod,john.doe@foobar.com";
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesNoUpfront, "c4.2xlarge.windows", rg, riArn, 0.45, 1),
				new Datum(CostType.savings, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.savingsNoUpfront, "c4.2xlarge.windows", rg, riArn, 0.15, 0),
		};
		test.run("2019-01-01T00:00:00Z", expected);
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamilyRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", rds, "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0002", "MySQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "0.5", "0.0", "Partial Upfront");
		line.setDiscountedUsageFields("0.00739", "0.00902", "0.026");
		ProcessTest test = new ProcessTest(line, Result.hourly, 31);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, rdsInstance, Operation.bonusReservedInstancesPartialUpfront, "db.t2.micro.mysql", null, riArn, 0.00902, 0.5),
				new Datum(CostType.savings, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, rdsInstance, Operation.savingsPartialUpfront, "db.t2.micro.mysql", null, riArn, 0.00959, 0),
				new Datum(CostType.amortization, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, rdsInstance, Operation.amortizedPartialUpfront, "db.t2.micro.mysql", null, riArn, 0.00739, 0),
			};
		test.run("2019-01-01T00:00:00Z", expected);
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.2xlarge.windows", "Prod,john.doe@foobar.com", riArn, 0.34, 1.0),
			};
		test.run(expected);
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.large.windows", null, riArn, 0.34, 1.0),
			};
		test.run(expected);
		
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("0.32", "0.36", "1.02");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.savings, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.savingsPartialUpfront, "c4.large.windows", null, riArn, 0.34, 0),
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.large.windows", null, riArn, 0.36, 1.0),
				new Datum(CostType.amortization, a2, Region.AP_SOUTHEAST_2, Datum.ap_southeast_2a, ec2Instance, Operation.amortizedPartialUpfront, "c4.large.windows", null, riArn, 0.32, 0),
			};
		test.run("2018-01-01T00:00:00Z", expected);

		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("0.32", "0.36", "1.02");
		test = new ProcessTest(line, Result.hourly, 31);
		test.run("2019-01-01T00:00:00Z", expected);
	}
	
	@Test
	public void testRIFeeNoUpfront() throws Exception {
		// Test Cost and Usage Prior to Jan 1, 2018 (Uses Reservations pulled by Capacity Poller, so we inject one)
		Line line = new Line(LineItemType.RIFee, "eu-west-1", "", ec2, "EU-HeavyUsage:t2.small", "RunInstances", "USD 0.0146 hourly fee per Linux/UNIX (Amazon VPC), t2.small instance", PricingTerm.none, "2017-11-01T00:00:00Z", "2017-12-01T00:00:00Z", "18600", "271.56", "");
		line.setRIFeeFields("", "", "", "", "25", "", "");
		ProcessTest test = new ProcessTest(line, Result.monthly, 31);
		TagGroupRI tg = TagGroupRI.get("Recurring", "234567890123", "eu-west-1", null, "EC2 Instance", "Bonus RIs - No Upfront", "t2.small", "hours", null, "arn", accountService, productService);
		Reservation r = new Reservation(tg, 25, 0, 0, PurchaseOption.NoUpfront, 0.0, 0.028);
		test.addReservation(r);
		test.setDelayed();
		test.run("2017-11-01T00:00:00Z", null);
		
		// Test Cost and Usage After Jan 1, 2018 (Uses RIFee data in CUR)
		// 25 RIs at 0.0146/hr recurring
		line = new Line(LineItemType.RIFee, "eu-west-1", "", ec2, "EU-HeavyUsage:t2.small", "RunInstances", "USD 0.0146 hourly fee per Linux/UNIX (Amazon VPC), t2.small instance", PricingTerm.none, "2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z", "18600", "271.56", "");
		line.setRIFeeFields("0", "0", "0", "0", "25", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		Datum expected = new Datum(CostType.recurring, a2, Region.EU_WEST_1, null, ec2Instance, Operation.reservedInstancesNoUpfront, "t2.small", ",", ReservationArn.get(""), 0.0, 0.365);

		test.run("2019-01-01T00:00:00Z", null, expected);		
	}
	
	@Test
	public void testRIFeePartialUpfront() throws Exception {
		// Test case before we had support for Amortization
		// 1 RI with 1.0/hr recurring and 2.0/hr upfront
		Line line = new Line(LineItemType.RIFee, "ap-southeast-2", "", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "720.0", "");
		line.setRIFeeFields("1440", "", "", "", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		ProcessTest test = new ProcessTest(line, Result.monthly, 30);
		test.setDelayed();
		test.run(null);

		// Test reservation/unused rates
		// 1 RI with 1.0/hr recurring and 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "720", "720.0", "");
		line.setRIFeeFields("1440", "0", "0", "0", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setNetUnblendedStart("2019-02-01T00:00:00Z");
		test.setDelayed();
		Datum expected = new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, null, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.2xlarge.windows", ",", ReservationArn.get(""), 2.0, 1.0);
		test.run("2019-06-01T00:00:00Z", null, expected);
	}
	
	@Test
	public void testReservedMonthlyFeeRDS() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "1440.0", "");
		line.setRIFeeFields("720.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		ProcessTest test = new ProcessTest(line, Result.ignore, 30);
		test.setDelayed();
		Datum expected = new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.reservedInstancesPartialUpfront, "db.t2.micro.postgres", ",", ReservationArn.get(""), 1.0, 2.0);
		test.run("2019-07-01T00:00:00Z", null, expected);

		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "0", "");
		line.setRIFeeFields("2880.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		test = new ProcessTest(line, Result.ignore, 30);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.reservedInstancesAllUpfront, "db.t2.micro.postgres", ",", ReservationArn.get(""), 4.0, 0.0);
		test.run("2019-07-01T00:00:00Z", null, expected);

		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "2160", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		test = new ProcessTest(line, Result.ignore, 30);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.reservedInstancesNoUpfront, "db.t2.micro.postgres", ",", ReservationArn.get(""), 0, 3);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
		// Partial Upfront Multi-AZ 1 RI at 2.0/hr recurring and 1.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "720", "1440.0", "");
		line.setRIFeeFields("720.0", "0", "0", "0", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("1");
		test = new ProcessTest(line, Result.ignore, 30);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.reservedInstancesPartialUpfront, "db.t2.micro.multiaz.postgres", ",", ReservationArn.get(""), 1, 2);
		test.run("2019-07-01T00:00:00Z", null, expected);
	}
	
	@Test
	public void testReservedPartialUpfrontHourlyUsageRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "", rds, "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0014", "PostgreSQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "Partial Upfront");
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.bonusReservedInstancesPartialUpfront, "db.t2.micro.postgres", null, riArn, 0, 1.0),
			};
		test.run(expected);
	}
	
	@Test
	public void testRIPurchase() throws Exception {
		Line line = new Line(LineItemType.Fee, "ap-southeast-2", "", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "Sign up charge for subscription: 647735683, planId: 2195643", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2018-06-01T00:00:00Z", "150.0", "9832.500000", "Partial Upfront");
		line.setBillType(BillType.Purchase);
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.subscription, a2, Region.AP_SOUTHEAST_2, null, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.2xlarge.windows", ",", riArn, 9832.5, 150.0),
		};
		test.run(expected);
	}
	
	@Test
	public void testSpot() throws Exception {
		Line line = new Line(LineItemType.Usage, "ap-northeast-2", "", ec2, "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "");
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_NORTHEAST_2, null, ec2Instance, Operation.spotInstances, "c4.xlarge", 0.349, 1.0),
			};
		test.run(expected);
	}
	@Test
	public void testSpotWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.Usage, "ap-northeast-2", "", ec2, "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.AP_NORTHEAST_2, null, ec2Instance, Operation.spotInstances, "c4.xlarge", "Prod,john.doe@foobar.com", 0.349, 1.0),
			};
		test.run(expected);
	}
	
	@Test
	public void testReservedMonthlyFeeElasticsearch() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		ProcessTest test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		Datum expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elasticsearch, Operation.reservedInstancesPartialUpfront, "r4.xlarge.elasticsearch", ",", ReservationArn.get(""), 1.0, 2.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "0", "");
		line.setRIFeeFields("2976.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elasticsearch, Operation.reservedInstancesAllUpfront, "r4.xlarge.elasticsearch", ",", ReservationArn.get(""), 4.0, 0.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "2232.0", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elasticsearch, Operation.reservedInstancesNoUpfront, "r4.xlarge.elasticsearch", ",", ReservationArn.get(""), 0.0, 3.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
	}
	
	@Test
	public void testReservedElasticsearch() throws Exception {
		// Partial Upfront
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Partial Upfront");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		ProcessTest test = new ProcessTest(line, Result.hourly, 31);
		Datum[] expected = {
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, elasticsearch, Operation.amortizedPartialUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.25, 0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elasticsearch, Operation.savingsPartialUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.09, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elasticsearch, Operation.bonusReservedInstancesPartialUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.32, 1.0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
		
		// All Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("0.30", "0", "0.66");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, elasticsearch, Operation.amortizedAllUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.3, 0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elasticsearch, Operation.savingsAllUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.36, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elasticsearch, Operation.bonusReservedInstancesAllUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.0, 1.0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
		
		// No Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "No Upfront");
		line.setDiscountedUsageFields("0", "0.34", "0.66");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elasticsearch, Operation.savingsNoUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.32, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elasticsearch, Operation.bonusReservedInstancesNoUpfront, "r4.xlarge.elasticsearch", null, riArn, 0.34, 1.0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
	}
	
	@Test
	public void testReservedDynamoDB() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", "Amazon DynamoDB", "WriteCapacityUnit-Hrs", "CommittedThroughput", "DynamoDB, Reserved Write Capacity used this month", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Heavy Utilization");
		line.setDiscountedUsageFields("0.00028082", "0.00020992", "0.0013");
		// TODO: support DynamoDB amortization
		ProcessTest test = new ProcessTest(line, Result.hourly, 31);
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, dynamoDB, Operation.getOperation("CommittedThroughput"), "WriteCapacityUnit-Hrs", 0, 1.0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
	}
		
	@Test
	public void testReservedMonthlyFeeElastiCache() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		ProcessTest test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		Datum expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesPartialUpfront, "cache.m5.medium.redis", ",", ReservationArn.get(""), 1.0, 2.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "0", "");
		line.setRIFeeFields("2976.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesAllUpfront, "cache.m5.medium.redis", ",", ReservationArn.get(""), 4.0, 0.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "2232.0", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesNoUpfront, "cache.m5.medium.redis", ",", ReservationArn.get(""), 0.0, 3.0);
		test.run("2019-07-01T00:00:00Z", null, expected);

		// Heavy Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesHeavy, "cache.m3.medium.redis", ",", ReservationArn.get(""), 1.0, 2.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
		// Medium Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "MediumUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesMedium, "cache.m3.medium.redis", ",", ReservationArn.get(""), 1.0, 2.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
		// Light Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "LightUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesLight, "cache.m3.medium.redis", ",", ReservationArn.get(""), 1.0, 2.0);
		test.run("2019-07-01T00:00:00Z", null, expected);

		// Heavy Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront --- test with different region
		line = new Line(LineItemType.RIFee, "us-west-2", "", ec, "USW2-HeavyUsage:cache.t2.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		test = new ProcessTest(line, Result.ignore, 31);
		test.setDelayed();
		expected = new Datum(CostType.recurring, a2, Region.US_WEST_2, null, elastiCache, Operation.reservedInstancesHeavy, "cache.t2.medium.redis", ",", ReservationArn.get(""), 1.0, 2.0);
		test.run("2019-07-01T00:00:00Z", null, expected);
		
	}
	
	@Test
	public void testReservedElastiCache() throws Exception {
		// Partial Upfront
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Partial Upfront");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		ProcessTest test = new ProcessTest(line, Result.hourly, 31);
		Datum[] expected = {
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, elastiCache, Operation.amortizedPartialUpfront, "cache.m3.medium.redis", null, riArn, 0.25, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesPartialUpfront, "cache.m3.medium.redis", null, riArn, 0.32, 1.0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elastiCache, Operation.savingsPartialUpfront, "cache.m3.medium.redis", null, riArn, 0.09, 0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
		
		// All Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("0.30", "0", "0.66");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, elastiCache, Operation.amortizedAllUpfront, "cache.m3.medium.redis", null, riArn, 0.3, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesAllUpfront, "cache.m3.medium.redis", null, riArn, 0, 1.0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elastiCache, Operation.savingsAllUpfront, "cache.m3.medium.redis", null, riArn, 0.36, 0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
		
		// No Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "No Upfront");
		line.setDiscountedUsageFields("0", "0.34", "0.66");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesNoUpfront, "cache.m3.medium.redis", null, riArn, 0.34, 1.0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elastiCache, Operation.savingsNoUpfront, "cache.m3.medium.redis", null, riArn, 0.32, 0),
			};
		test.run("2018-01-01T00:00:00Z", expected);

		// Heavy Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Heavy Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, elastiCache, Operation.amortizedHeavy, "cache.m3.medium.redis", null, riArn, 0.25, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesHeavy, "cache.m3.medium.redis", null, riArn, 0.32, 1.0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elastiCache, Operation.savingsHeavy, "cache.m3.medium.redis", null, riArn, 0.09, 0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
		
		// Medium Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Medium Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, elastiCache, Operation.amortizedMedium, "cache.m3.medium.redis", null, riArn, 0.25, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesMedium, "cache.m3.medium.redis", null, riArn, 0.32, 1.0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elastiCache, Operation.savingsMedium, "cache.m3.medium.redis", null, riArn, 0.09, 0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
		
		// Light Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Light Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, elastiCache, Operation.amortizedLight, "cache.m3.medium.redis", null, riArn, 0.25, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesLight, "cache.m3.medium.redis", null, riArn, 0.32, 1.0),
				new Datum(CostType.savings, a2, Region.US_EAST_1, null, elastiCache, Operation.savingsLight, "cache.m3.medium.redis", null, riArn, 0.09, 0),
			};
		test.run("2018-01-01T00:00:00Z", expected);
		
	}
	
	@Test
	public void testEC2Credit() throws Exception {
		Line line = new Line(LineItemType.Credit, "us-east-1", "", ec2, "HeavyUsage:m4.large", "RunInstances", "MB - Pricing Adjustment", PricingTerm.reserved, "2019-08-01T00:00:00Z", "2019-09-01T00:00:00Z", "0.0000000000", "-38.3100000000", "");
		ProcessTest test = new ProcessTest(line, Result.delay, 31);
		Datum[] expected = {
				new Datum(CostType.credit, a2, Region.US_EAST_1, null, ec2Instance, Operation.reservedInstancesCredits, "m4.large", -0.0515, 0),
			};
		test.run("2019-08-01T00:00:00Z", expected);				
	}
		
	@Test
	public void testRedshiftCredit() throws Exception {
		// Credits sometimes end one second into next month, so make sure we deal with that
		Line line = new Line(LineItemType.Credit, "us-east-1", "", redshift, "Node:ds2.xlarge", "RunComputeNode:0001", "AWS Credit", PricingTerm.onDemand, "2020-03-01T00:00:00Z", "2020-04-01T00:00:01Z", "0.0000000000", "-38.3100000000", "");
		ProcessTest test = new ProcessTest(line, Result.delay, 31);
		Datum[] expected = {
				new Datum(CostType.credit, a2, Region.US_EAST_1, null, redshiftInstance, Operation.ondemandInstances, "ds2.xlarge", -0.0515, 0),
			};
		test.run("2020-03-01T00:00:00Z", expected);				
	}
	
	@Test
	public void testConfigCredit() throws Exception {
		Line line = new Line(LineItemType.Credit, "us-west-2", "", awsConfig, "USW2-ConfigurationItemRecorded", "", "AWS Config rules- credits to support pricing model change TT: 123456789012", PricingTerm.none, "2019-08-01T00:00:00Z", "2019-08-01T01:00:01Z", "0.0000000000", "-0.00492", "");
		ProcessTest test = new ProcessTest(line, Result.delay, 31);
		Datum[] expected = {
				new Datum(CostType.credit, a2, Region.US_WEST_2, null, config, Operation.getOperation("None"), "ConfigurationItemRecorded", -0.00492, 0),
			};
		test.run("2019-08-01T00:00:00Z", expected);				
	}
	
	@Test
	public void testSavingsPlanRecurringFee() throws Exception {
		// Test No Upfront with 50% usage
		Line line = new Line(LineItemType.SavingsPlanRecurringFee, "global", "", "Savings Plans for AWS Compute usage", "ComputeSP:1yrNoUpfront", "", "1 year No Upfront Compute Savings Plan", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.12", "");
		line.setSavingsPlanRecurringFeeFields("0", "0.12", "2019-11-08T11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.12", "0.06", "NoUpfront");
		
		// Should produce one cost item for the unused recurring portion of the plan.
		ProcessTest test = new ProcessTest(line, Result.hourlyTruncate, 31);
		test.setDelayed();
		Product savingsPlans = productService.getProductByServiceCode("Savings Plans for AWS Compute usage");
		Datum[] expected = {
				new Datum(CostType.recurring, a2, Region.GLOBAL, null, savingsPlans, Operation.savingsPlanUnusedNoUpfront, "ComputeSP:1yrNoUpfront", 0.06, 0),
			};
		test.run("2019-12-01T00:00:00Z", expected);
		
		// Test Partial Upfront with 50% usage
		line = new Line(LineItemType.SavingsPlanRecurringFee, "global", "", "Savings Plans for AWS Compute usage", "ComputeSP:1yrPartialUpfront", "", "1 year No Upfront Compute Savings Plan", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.12", "");
		line.setSavingsPlanRecurringFeeFields("0.07", "0.05", "2019-11-08T11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.12", "0.06", "PartialUpfront");
		
		// Should produce two cost items for the unused recurring and amortized portions of the plan.
		test = new ProcessTest(line, Result.hourlyTruncate, 31);
		test.setDelayed();
		expected = new Datum[]{
				new Datum(CostType.amortization, a2, Region.GLOBAL, null, savingsPlans, Operation.savingsPlanUnusedAmortizedPartialUpfront, "ComputeSP:1yrPartialUpfront", 0.035, 0),
				new Datum(CostType.recurring, a2, Region.GLOBAL, null, savingsPlans, Operation.savingsPlanUnusedPartialUpfront, "ComputeSP:1yrPartialUpfront", 0.025, 0),
			};
		test.run("2019-12-01T00:00:00Z", expected);
		
		
		// Test All Upfront with 50% usage
		line = new Line(LineItemType.SavingsPlanRecurringFee, "global", "", "Savings Plans for AWS Compute usage", "ComputeSP:1yrAllUpfront", "", "1 year No Upfront Compute Savings Plan", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.12", "");
		line.setSavingsPlanRecurringFeeFields("0.12", "0", "2019-11-08T11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.12", "0.06", "AllUpfront");
		
		// Should produce one cost item for the unused amortized portion of the plan.
		test = new ProcessTest(line, Result.hourlyTruncate, 31);
		test.setDelayed();
		expected = new Datum[]{
				new Datum(CostType.amortization, a2, Region.GLOBAL, null, savingsPlans, Operation.savingsPlanUnusedAmortizedAllUpfront, "ComputeSP:1yrAllUpfront", 0.06, 0),
			};
		test.run("2019-12-01T00:00:00Z", expected);		
	}
	
	@Test
	public void testSavingsPlanCoveredUsage() throws Exception {
		// Should produce two cost items and one usage item for each case.
		// The cost items should have the effective cost, not the unblended OnDemand cost and the savings.
		// Bonus operations will be split apart by the savings plan processor.

		// No Upfront
		Line line = new Line(LineItemType.SavingsPlanCoveredUsage, "us-east-1", "us-east-1a", "Amazon Elastic Compute Cloud", "BoxUsage:t2.micro", "RunInstances", "$0.0116 per On Demand Linux t2.micro Instance Hour", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.0116", "");
		String arn = "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555";
		SavingsPlanArn spArn = SavingsPlanArn.get(arn);
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", arn, "0.0083", "NoUpfront", "0.0116");
		ProcessTest test = new ProcessTest(line, Result.hourly, 31);
		Datum[] expected = {
				new Datum(CostType.savings, a2, Region.US_EAST_1, Datum.us_east_1a, ec2Instance, Operation.savingsPlanSavingsNoUpfront, "t2.micro", 0.0033, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, Datum.us_east_1a, ec2Instance, Operation.savingsPlanBonusNoUpfront, "t2.micro", null, spArn, 0.0083, 1.0),
			};
		test.run("2019-12-01T00:00:00Z", expected);

		// Partial Upfront
		line = new Line(LineItemType.SavingsPlanCoveredUsage, "us-east-1", "us-east-1a", "Amazon Elastic Compute Cloud", "BoxUsage:t2.micro", "RunInstances", "$0.0116 per On Demand Linux t2.micro Instance Hour", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.0116", "");
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.0083", "PartialUpfront", "0.0116");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.savings, a2, Region.US_EAST_1, Datum.us_east_1a, ec2Instance, Operation.savingsPlanSavingsPartialUpfront, "t2.micro", 0.0033, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, Datum.us_east_1a, ec2Instance, Operation.savingsPlanBonusPartialUpfront, "t2.micro", null, spArn, 0.0083, 1.0),
			};
		test.run("2019-12-01T00:00:00Z", expected);
		
		// All Upfront
		line = new Line(LineItemType.SavingsPlanCoveredUsage, "us-east-1", "us-east-1a", "Amazon Elastic Compute Cloud", "BoxUsage:t2.micro", "RunInstances", "$0.0116 per On Demand Linux t2.micro Instance Hour", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "1", "0.0", "");
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.0083", "AllUpfront", "0.0116");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.savings, a2, Region.US_EAST_1, Datum.us_east_1a, ec2Instance, Operation.savingsPlanSavingsAllUpfront, "t2.micro", 0.0033, 0),
				new Datum(CostType.recurring, a2, Region.US_EAST_1, Datum.us_east_1a, ec2Instance, Operation.savingsPlanBonusAllUpfront, "t2.micro", null, spArn, 0.0083, 1.0),
			};
		test.run("2019-12-01T00:00:00Z", expected);
		
		// No Upfront Lambda
		line = new Line(LineItemType.SavingsPlanCoveredUsage, "ap-northeast-1", "", "AWS Lambda", "APN1-Lambda-GB-Second", "Invoke", "AWS Lambda - Total Compute - Asia Pacific (Tokyo)", PricingTerm.onDemand, "2019-12-01T00:00:00Z", "2019-12-01T01:00:00Z", "2.4", "0.00004", "");
		line.setSavingsPlanCoveredUsageFields("2019-11-08T00:11:15:04.000Z", "2020-11-07T11:15:03.000Z", "arn:aws:savingsplans::123456789012:savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555", "0.000036", "NoUpfront", "0.00004");
		test = new ProcessTest(line, Result.hourly, 31);
		expected = new Datum[]{
				new Datum(CostType.recurring, a2, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanBonusNoUpfront, "Lambda-GB-Second", null, spArn, 0.000036, 2.4),
				new Datum(CostType.savings, a2, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanSavingsNoUpfront, "Lambda-GB-Second", 0.000004, 0),
			};
		test.run("2019-12-01T00:00:00Z", expected);
		
	}
	
	@Test
	public void testOCBPremiumSupport() throws Exception {
		Line line = new Line(LineItemType.Fee, "global", "", "AWS Premium Support", "Dollar", "", "AWS Support (Enterprise)", PricingTerm.none, "2019-11-01T00:00:00Z", "2019-12-01T00:00:00Z", "1000000.00", "64500.00", "");
		line.setProductCode("OCBPremiumSupport");
		line.setBillType(BillType.Purchase);
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.subscription, a2, Region.GLOBAL, null, productService.getProduct("AWS Premium Support", "OCBPremiumSupport"), Operation.getOperation("None"), "Dollar", 64500.0, 1000000.0),
			};
		test.run("2019-11-01T00:00:00Z", expected);				
	}
	@Test
	public void testOCBPremiumSupportRefund() throws Exception {
		Line line = new Line(LineItemType.Refund, "global", "", "AWS Premium Support", "Dollar", "", "Discount", PricingTerm.none, "2019-11-01T00:00:00Z", "2019-12-01T00:00:00Z", "0", "-645.00", "");
		line.setProductCode("OCBPremiumSupport");
		line.setBillType(BillType.Refund);
		ProcessTest test = new ProcessTest(line, Result.hourly, 30);
		Datum[] expected = {
				new Datum(CostType.refund, a2, Region.GLOBAL, null, productService.getProduct("AWS Premium Support", "OCBPremiumSupport"), Operation.getOperation("None"), "Dollar", -645.0, 0.0),
			};
		test.run("2019-11-01T00:00:00Z", expected);				
	}
	
	@Test
	public void testTax() throws Exception {
		// Full month
		Line line = new Line(LineItemType.Tax, "", "", "Amazon Elastic Compute Cloud", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z", "1", "7.44", "");
		line.setTaxFields("GST", "Amazon Web Services, Inc.");
		ProcessTest test = new ProcessTest(line, Result.delay, 31);
		Datum[] expected = {
				new Datum(CostType.tax, a2, Region.US_EAST_1, null, ec2Product, Operation.ondemandInstances, "c4.large", 0.01, 0.001344),
			};
		test.run("2020-01-01T00:00:00Z", expected);				

		// Partial month with tax reported as partial month
		line = new Line(LineItemType.Tax, "", "", "Amazon Elastic Compute Cloud", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-19T05:00:01Z", "1", "4.37", "");
		line.setTaxFields("USSalesTax", "Amazon Web Services, Inc.");
		test = new ProcessTest(line, Result.delay, 31);
		test.setReportDate("2019-12-19T05:00:01Z");
		expected = new Datum[]{
				new Datum(CostType.tax, a2, Region.US_EAST_1, null, ec2Product, Operation.getOperation("Tax - USSalesTax"), "HeavyUsage:c4.large", 0.01, 0.00229),
			};
		test.run("2019-12-01T00:00:00Z", expected);
		
		// Partial month with tax reported as full month
		line = new Line(LineItemType.Tax, "", "", "Amazon Elastic Compute Cloud", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2019-12-01T00:00:00Z", "2020-01-01T00:00:00Z", "1", "7.44", "");
		line.setTaxFields("USSalesTax", "Amazon Web Services, Inc.");
		test = new ProcessTest(line, Result.delay, 31);
		test.setReportDate("2019-12-19T05:00:01Z");
		expected = new Datum[]{
				new Datum(CostType.tax, a2, Region.US_EAST_1, null, ec2Product, Operation.getOperation("Tax - USSalesTax"), "HeavyUsage:c4.large", 0.01, 0.001344),
			};
		test.run("2019-12-01T00:00:00Z", expected);
		
		// Zero tax - should ignore
		line = new Line(LineItemType.Tax, "", "", "Amazon Elastic Compute Cloud", "HeavyUsage:c4.large", "RunInstances", "Tax for product code AmazonEC2 usage type HeavyUsage:c4.large operation RunInstances", PricingTerm.none, "2019-12-01T00:00:00Z", "2019-12-19T05:00:01Z", "1", "0", "");
		line.setTaxFields("USSalesTax", "Amazon Web Services, Inc.");
		test = new ProcessTest(line, Result.ignore, 31);
		test.run("2019-12-01T00:00:00Z", null);		
	}
	
	@Test
	public void testTaxRefund() throws Exception {
		Line line = new Line(LineItemType.Tax, "", "", "Amazon Elastic Compute Cloud", "NatGateway-Hours", "NatGateway", "Tax refund line item for refundLineItem : abcdefg", PricingTerm.none, "2021-01-01T00:00:00Z", "2021-02-01T00:00:00Z", "1", "7.44", "");
		line.setBillType(BillType.Refund);
		line.setTaxFields("USSalesTax", "Amazon Web Services, Inc.");
		ProcessTest test = new ProcessTest(line, Result.delay, 31);
		Datum[] expected = {
				new Datum(CostType.tax, a2, Region.US_EAST_1, null, ec2Product, Operation.getOperation("NatGateway"), "NatGateway-Hours", 0.01, 0.001344),
			};
		test.run("2020-01-01T00:00:00Z", expected);				
	}
	
	@Test
	public void testMonthlyVAT() throws Exception {
		Line line = new Line(LineItemType.Tax, "", "", "Amazon Elastic Compute Cloud", "", "", "Tax for product code AmazonEC2", PricingTerm.none, "2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z", "1", "744.0", "");
		line.setTaxFields("VAT", "AWS EMEA SARL");
		ProcessTest test = new ProcessTest(line, Result.delay, 31);
		Datum[] expected = {
				new Datum(CostType.tax, a2, Region.GLOBAL, null, ec2Product, Operation.getOperation("Tax - VAT"), "Tax - AWS EMEA SARL", 1.0, 0.001344),
			};
		test.run("2020-01-01T00:00:00Z", expected);				
	}
}
