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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.IceOptions;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.WorkBucketDataConfig;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Zone.BadZone;

public class BillingFileProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final String resourcesDir = "src/test/resources/";
    private static final String resourcesReportDir = resourcesDir + "report/";
    private static final String cauReportDir = resourcesReportDir + "Oct2017/";
	private static PriceListService priceListService = null;
	private static Properties properties;
	
	public static final String separator = "|";
	public static final String separatorReplacement = "~";
	private static final String separatorRegex = "\\|";
	

    private static void init(String propertiesFilename) throws Exception {
		ReservationProcessorTest.init();
		priceListService = new PriceListService(resourcesDir, null, null);
		priceListService.init();
        properties = getProperties(propertiesFilename);        
		
		// Add all the zones we need for our test data		
		Region.AP_SOUTHEAST_2.getZone("ap-southeast-2a");
    }
    
    
	private static Properties getProperties(String propertiesFilename) throws IOException {
		Properties prop = new Properties();
		File file = new File(propertiesFilename);
        InputStream is = new FileInputStream(file);
		prop.load(is);
	    is.close();
	    
		return prop;	
	}
	
	
	interface ReportTest {
		public long Process(ProcessorConfig config, DateTime start,
				CostAndUsageData costAndUsageData,
				Instances instances) throws Exception;
		
		public ReservationProcessor getReservationProcessor();
	}
	class CostAndUsageTest implements ReportTest {
		private ReservationProcessor reservationProcessor = null;
		
		public long Process(ProcessorConfig config, DateTime start,
				CostAndUsageData costAndUsageData,
				Instances instances) throws Exception {
			
			CostAndUsageReportProcessor cauProcessor = new CostAndUsageReportProcessor(config);
			reservationProcessor = cauProcessor.getReservationProcessor();
			File manifest = new File(cauReportDir, "hourly-cost-and-usage-Manifest.json");
			S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
			s3ObjectSummary.setLastModified(new Date());
			CostAndUsageReport report = new CostAndUsageReport(s3ObjectSummary, manifest, cauProcessor, "");
			
	    	List<File> files = Lists.newArrayList();
	    	for (String key: report.getReportKeys()) {
				String prefix = key.substring(0, key.lastIndexOf("/") + 1);
				String filename = key.substring(prefix.length());
	    		files.add(new File(cauReportDir, filename));
	    	}
	        Long startMilli = report.getStartTime().getMillis();
	        if (startMilli != start.getMillis()) {
	        	logger.error("Data file start time doesn't match config");
	        	return 0L;
	        }
	        return cauProcessor.processReport(report.getStartTime(), report, files,
	        		costAndUsageData, instances, "123456789012");
		}
		
		public ReservationProcessor getReservationProcessor() {
			return reservationProcessor;
		}
	}
	
	public void testFileData(ReportTest reportTest, String prefix, ProductService productService) throws Exception {
        ReservationPeriod reservationPeriod = ReservationPeriod.valueOf(properties.getProperty(IceOptions.RESERVATION_PERIOD, "oneyear"));
        PurchaseOption reservationPurchaseOption = PurchaseOption.valueOf(properties.getProperty(IceOptions.RESERVATION_PURCHASE_OPTION, "PartialUpfront"));
		BasicReservationService reservationService = new BasicReservationService(reservationPeriod, reservationPurchaseOption);
		
		class TestProcessorConfig extends ProcessorConfig {
			public TestProcessorConfig(
		            Properties properties,
		            ProductService productService,
		            ReservationService reservationService,
		            PriceListService priceListService) throws Exception {
				super(properties, null, productService, reservationService, priceListService);
			}
			
			@Override
			protected void initZones() {
				
			}
			@Override
		    protected Map<String, AccountConfig> getAccountsFromOrganizations() {
				return Maps.newHashMap();
			}
			
			@Override
		    protected void processBillingDataConfig(Map<String, AccountConfig> accountConfigs) {
			
			}
			
			@Override
			protected WorkBucketDataConfig downloadWorkBucketDataConfig(boolean force) {
				return null;
			}
		}
		
		ResourceService resourceService = new BasicResourceService(productService, new String[]{}, false);
		
		ProcessorConfig config = new TestProcessorConfig(
										properties,
										productService,
										reservationService,
										priceListService);
		Long startMilli = config.startDate.getMillis();
		BillingFileProcessor bfp = ProcessorConfig.billingFileProcessor;
		bfp.init(startMilli);
		
		// Debug settings
		//bfp.reservationProcessor.setDebugHour(0);
		//bfp.reservationProcessor.setDebugFamily("c4");
    	
		CostAndUsageData costAndUsageData = new CostAndUsageData(startMilli, null, null, TagCoverage.none, null, productService);
		costAndUsageData.enableTagGroupCache(true);
        Instances instances = new Instances(null, null, null);
        
		Map<ReservationKey, CanonicalReservedInstances> reservations = ReservationCapacityPoller.readReservations(new File(resourcesReportDir, "reservation_capacity.csv"));
		ReservationCapacityPoller rcp = new ReservationCapacityPoller(config);
		rcp.updateReservations(reservations, config.accountService, startMilli, productService, resourceService, reservationService);
				
		Long endMilli = reportTest.Process(config, config.startDate, costAndUsageData, instances);
		    
        int hours = (int) ((endMilli - startMilli)/3600000L);
        logger.info("cut hours to " + hours);
        costAndUsageData.cutData(hours);
        		
		// Initialize the price lists
    	Map<Product, InstancePrices> prices = Maps.newHashMap();
    	Product p = productService.getProduct(Product.Code.Ec2Instance);
    	prices.put(p, priceListService.getPrices(config.startDate, ServiceCode.AmazonEC2));
    	p = productService.getProduct(Product.Code.RdsInstance);
    	if (reservationService.hasReservations(p))
    		prices.put(p, priceListService.getPrices(config.startDate, ServiceCode.AmazonRDS));
    	p = productService.getProduct(Product.Code.Redshift);
    	if (reservationService.hasReservations(p))
    		prices.put(p, priceListService.getPrices(config.startDate, ServiceCode.AmazonRedshift));

        reportTest.getReservationProcessor().process(config.reservationService, costAndUsageData, null, config.startDate, prices);
        
        logger.info("Finished processing reports, ready to compare results on " + 
        		costAndUsageData.get(null).getTagGroups().size() + " tags");
        
		// Read the file with tags to ignore if present
        File ignoreFile = new File(resourcesReportDir, "ignore.csv");
        Set<TagGroup> ignore = null;
        if (ignoreFile.exists()) {
    		try {
    			BufferedReader in = new BufferedReader(new FileReader(ignoreFile));
    			ignore = deserializeTagGroupsCsv(config.accountService, productService, in);
    			in.close();
    		} catch (Exception e) {
    			logger.error("Error reading ignore tags file " + e);
    		}
        }
                
        File expected = new File(resourcesReportDir, prefix+"cau.csv");
        if (!expected.exists()) {
        	// Comparison file doesn't exist yet, write out our current results
        	logger.info("Saving reference data...");
            writeData(costAndUsageData.get(null), expected);
        }
        else {
        	// Compare results against the expected data
        	logger.info("Comparing against reference usage data...");
        	compareData(costAndUsageData.get(null), expected, config.accountService, productService, ignore);
        }
	}
		
	private void writeData(DataSerializer data, File outputFile) {
		FileWriter out;
		try {
			out = new FileWriter(outputFile);
	        data.serializeCsv(out, null);
	        out.close();
		} catch (Exception e) {
			logger.error("Error writing file " + e);
		}
	}

	private void compareData(DataSerializer data, File expectedFile, AccountService accountService, ProductService productService, Set<TagGroup> ignore) {
		// Read in the expected data
		DataSerializer expectedData = new DataSerializer(0);
		
		// Will print out tags that have the following usage type family. Set to null to disable.
		String debugFamily = null; // "t2";
		
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(expectedFile));
			expectedData.deserializeCsv(accountService, productService, in);
			in.close();
		} catch (Exception e) {
			logger.error("Error reading expected data file " + e);
		}
		
		
		// See that number of hours matches
		assertEquals("Number of hours doesn't match", expectedData.getNum(), data.getNum());
		// For each hour see that the length and entries match
		for (int i = 0; i < data.getNum(); i++) {
			Map<TagGroup, CostAndUsage> expected = expectedData.getData(i);
			Map<TagGroup, CostAndUsage> got = Maps.newHashMap();
			for (Entry<TagGroup, CostAndUsage> entry: data.getData(i).entrySet()) {
				TagGroup tg = entry.getKey();
				// Convert any TagGroupRIs to TagGroups since the RI version isn't reconstituted from file
				if (tg instanceof TagGroupRI) {
					tg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup);
				}
				CostAndUsage cau = entry.getValue();
				if (!cau.isZero())
					got.put(tg, cau);
			}
	        Set<TagGroup> keys = Sets.newTreeSet();
	        keys.addAll(got.keySet());
			int gotLen = keys.size();

			// Count all the tags found vs. not found and output the error printouts in sorted order
			int numFound = 0;
			int numNotFound = 0;
			Set<TagGroup> notFound = Sets.newTreeSet();
			int expectedLen = 0;
			for (Entry<TagGroup, CostAndUsage> entry: expected.entrySet()) {
				CostAndUsage expectedValue = entry.getValue();
				if (!expectedValue.isZero())
					expectedLen++;
				
				CostAndUsage gotValue = got.get(entry.getKey());
				if (gotValue == null && !expectedValue.isZero()) {
					if (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily))
						notFound.add(entry.getKey());
					numNotFound++;
				}
				else
					numFound++;
			}
			
	        if (expectedLen != gotLen)
	        	logger.info("Number of items for hour " + i + " doesn't match, expected " + expectedLen + ", got " + gotLen);
			
			
			int numPrinted = 0;
			for (TagGroup tg: notFound) {
				logger.info("Tag not found: " + tg + ", value: " + expected.get(tg));
				if (numPrinted++ > 1000)
					break;
			}
				
			// Scan for values in got but not in expected
			int numExtra = 0;
			Set<TagGroup> extras = Sets.newTreeSet();
			for (Entry<TagGroup, CostAndUsage> entry: got.entrySet()) {
				CostAndUsage expectedValue = expected.get(entry.getKey());
				if (expectedValue == null) {
					if (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily))
						extras.add(entry.getKey());
					numExtra++;
				}
			}
			numPrinted = 0;
			for (TagGroup tg: extras) {
				logger.info("Extra tag found: " + tg + ", value: " + got.get(tg));
				if (numPrinted++ > 1000)
					break;
			}
			if (numNotFound > 0 || numExtra > 0) {
				logger.info("Hour "+i+" Tags not found: " + numNotFound + ", found " + numFound + ", extra " + numExtra);
//				for (Product a: productService.getProducts()) {
//					logger.info(a.name + ": " + a.hashCode() + ", " + System.identityHashCode(a) + ", " + System.identityHashCode(a.name));
//				}
			}
			
			// Compare the values on found tags
			int numMatches = 0;
			int numMismatches = 0;
			if (numFound > 0) {
				for (Entry<TagGroup, CostAndUsage> entry: got.entrySet()) {
					if (ignore != null && ignore.contains(entry.getKey()))
						continue;
					
					CostAndUsage gotValue = entry.getValue();
					CostAndUsage expectedValue = expected.get(entry.getKey());
					if (expectedValue != null) {
						if (Math.abs(expectedValue.cost - gotValue.cost) < 0.001 &&
							Math.abs(expectedValue.usage - gotValue.usage) < 0.001) {
							numMatches++;
						}
						else {
							if (numMismatches < 100 && (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily)))
								logger.info("Non-matching entry for hour " + i + " with tag " + entry.getKey() + ", expected " + expectedValue + ", got " + gotValue);
							numMismatches++;				
						}
					}
				}
				if (numMismatches > 0)
					logger.info("Hour "+i+" has " + numMatches + " matches and " + numMismatches + " mismatches");
				assertEquals("Hour "+i+" has " + numMismatches + " incorrect data values", 0, numMismatches);
			}
			assertEquals("Hour "+i+" has " + numNotFound + " tags that were not found", 0, numNotFound);
			assertEquals("Number of items for hour " + i + " doesn't match, expected " + expectedLen + ", got " + gotLen, expectedLen, gotLen);			
		}
	}
	
	private Set<TagGroup> deserializeTagGroupsCsv(AccountService accountService, ProductService productService, BufferedReader in) throws IOException, BadZone, ResourceException {
        Set<TagGroup> result = Sets.newTreeSet();

        String line;
        
        // skip the header
        in.readLine();

        while ((line = in.readLine()) != null) {
        	String[] items = line.split(",");        	
        	TagGroup tag = TagGroup.getTagGroup(items[0], items[1], items[2], items[3], items[4], items[5],
        			items.length > 6 ? items[6] : "", 
        			items.length > 7 ? items[7].split(separatorRegex, -1) : null, 
        			accountService, productService);
            result.add(tag);
        }

        return result;
    }

	
	@Test
	public void testCostAndUsageReport() throws Exception {
		init(cauReportDir + "ice.properties");
		ProductService productService = new BasicProductService();
		testFileData(new CostAndUsageTest(), "cau-", productService);
	}
	
	
}
