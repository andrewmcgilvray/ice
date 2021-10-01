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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsRequest;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesResult;
import com.amazonaws.services.ec2.model.ReservedInstances;
import com.amazonaws.services.ec2.model.ReservedInstancesModification;
import com.amazonaws.services.ec2.model.ReservedInstancesModificationResult;
import com.amazonaws.services.elasticache.AmazonElastiCache;
import com.amazonaws.services.elasticache.AmazonElastiCacheClientBuilder;
import com.amazonaws.services.elasticache.model.AmazonElastiCacheException;
import com.amazonaws.services.elasticache.model.DescribeReservedCacheNodesResult;
import com.amazonaws.services.elasticache.model.ReservedCacheNode;
import com.amazonaws.services.elasticsearch.AWSElasticsearch;
import com.amazonaws.services.elasticsearch.AWSElasticsearchClientBuilder;
import com.amazonaws.services.elasticsearch.model.AWSElasticsearchException;
import com.amazonaws.services.elasticsearch.model.DescribeReservedElasticsearchInstancesRequest;
import com.amazonaws.services.elasticsearch.model.DescribeReservedElasticsearchInstancesResult;
import com.amazonaws.services.elasticsearch.model.ReservedElasticsearchInstance;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.AmazonRDSException;
import com.amazonaws.services.rds.model.DescribeReservedDBInstancesResult;
import com.amazonaws.services.rds.model.ReservedDBInstance;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.AmazonRedshiftException;
import com.amazonaws.services.redshift.model.DescribeReservedNodesResult;
import com.amazonaws.services.redshift.model.ReservedNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.common.Poller;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.pricelist.InstancePrices.LeaseContractLength;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.Rate;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.InstanceCache;
import com.netflix.ice.tag.InstanceDb;
import com.netflix.ice.tag.InstanceOs;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class ReservationCapacityPoller extends Poller {
    protected ProcessorConfig config;
    private static final String archiveFilename = "reservation_capacity.csv";
    private long archiveLastModified = 0;
    
    private static final String ec2 		= "ec2";
    private static final String rds 		= "rds";
    private static final String redshift 	= "redshift";
    private static final String ec			= "ec";
    private static final String es			= "es";
    
	static Map<String, Double> instanceSizeMap = Maps.newHashMap();
	static String[] sizes = new String[]{
		"nano",
		"micro",
		"small",
		"medium",
		"large",
		"xlarge",
	};
	{
		// Load the multiplier map
		for (int i = 0; i < sizes.length; i++)
			instanceSizeMap.put(sizes[i], (double) (1 << i));
	}

	ReservationCapacityPoller(ProcessorConfig config) {
    	this.config = config;		
	}
	
    public void init() throws Exception {
    	poll();
    	start(3600, 3600, false);
    }

    @Override
    protected void poll() throws Exception {
        Map<ReservationKey, CanonicalReservedInstances> reservations = readArchive(config);
        
        if (archiveLastModified < DateTime.now().minusHours(6).getMillis()) {
        	pullReservations(reservations);
        }
        updateReservations(reservations, config.accountService, config.startDate.getMillis(), config.productService, config.resourceService, config.reservationService);
    }
    
    private long getEffectiveReservationTime(Date d) {
    	Calendar c = new GregorianCalendar();
    	c.setTime(d);
    	c.set(Calendar.MINUTE, 0);
    	c.set(Calendar.SECOND, 0);
    	c.set(Calendar.MILLISECOND, 0);
    	return c.getTime().getTime();
    }

    private void pullReservations(Map<ReservationKey, CanonicalReservedInstances> reservations) {
        
        AmazonEC2ClientBuilder ec2Builder = AmazonEC2ClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);
        AmazonRDSClientBuilder rdsBuilder = AmazonRDSClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);
        AmazonRedshiftClientBuilder redshiftBuilder = AmazonRedshiftClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);
        AWSElasticsearchClientBuilder esBuilder = AWSElasticsearchClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);
        AmazonElastiCacheClientBuilder ecBuilder = AmazonElastiCacheClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);
        
        for (Entry<Account, Set<String>> entry: config.accountService.getReservationAccounts().entrySet()) {
            Account account = entry.getKey();
            Set<String> products = entry.getValue();
            
        	try {
                String assumeRole = config.accountService.getReservationAccessRoles().get(account);
                String externalId = config.accountService.getReservationAccessExternalIds().get(account);
                if (assumeRole != null && assumeRole.isEmpty())
                	assumeRole = null;

                logger.info("Get reservations for account: " + account.getIceName() + ", role: " + assumeRole + ", externalId: " + externalId);

                AWSCredentialsProvider credentialsProvider = AwsUtils.awsCredentialsProvider;
                if (assumeRole != null) {
                    credentialsProvider = AwsUtils.getAssumedCredentialsProvider(account.getId(), assumeRole, externalId);
                }
                

          	    AmazonEC2 ec2Client = ec2Builder.withRegion(Region.US_EAST_1.name).withCredentials(credentialsProvider).build();
          	    DescribeRegionsResult regionResult = ec2Client.describeRegions();
          	    ec2Client.shutdown();
            	   
                if (products.contains(ec2)) {
                   for (com.amazonaws.services.ec2.model.Region r: regionResult.getRegions()) {
                	   Region region = Region.getRegionByName(r.getRegionName());
                	   ec2Client = ec2Builder.withRegion(region.name).withCredentials(credentialsProvider).build();
                	   Map<ReservationKey, CanonicalReservedInstances> ec2Reservations = Maps.newTreeMap();
                	   
                	   // Start by getting any reservation modifications so that we can later use them to track down
                	   // the fixed price of modified Partial Upfront or All Upfront reservations. AWS doesn't carry
                	   // the fixed price to the modified reservation, but we need that to compute amortization.
                	   List<ReservedInstancesModification> modifications = Lists.newArrayList();
                	   try {
                		   DescribeReservedInstancesModificationsResult modResult = ec2Client.describeReservedInstancesModifications();
                		   
                		   modifications.addAll(modResult.getReservedInstancesModifications());
                		   while (modResult.getNextToken() != null) {
                			   modResult = ec2Client.describeReservedInstancesModifications(new DescribeReservedInstancesModificationsRequest().withNextToken(modResult.getNextToken()));
                    		   modifications.addAll(modResult.getReservedInstancesModifications());
                		   }
                	   }
                       catch(AmazonEC2Exception e) {
                    	   logger.info("could not get EC2 reservation modifications for " + region + " " + account.getIceName() + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedInstancesModifications for " + region.name + " " + account.getIceName(), e);
                       }
                	   Ec2Mods mods = new Ec2Mods(modifications);
                       try {
                    	   
                           DescribeReservedInstancesResult result = ec2Client.describeReservedInstances();
                           for (ReservedInstances reservation: result.getReservedInstances()) {
                        	   //logger.info("*** Reservation: " + reservation.getReservedInstancesId());
                               ReservationKey key = new ReservationKey(account.getId(), region.name, reservation.getReservedInstancesId());
                               
                               CanonicalReservedInstances cri = new CanonicalReservedInstances(
                            		   account.getId(), region.name, reservation, 
                            		   mods.getModResId(reservation.getReservedInstancesId()));
                               ec2Reservations.put(key, cri);
                           }
                       }
                       catch(AmazonEC2Exception e) {
                    	   logger.info("could not get EC2 reservations for " + region + " " + account.getIceName() + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedInstances for " + region.name + " " + account.getIceName(), e);
                       }
                       ec2Client.shutdown();
                       handleEC2Modifications(ec2Reservations, mods, region, config.priceListService);
                       reservations.putAll(ec2Reservations);
                   }
               }
               	
   
               if (products.contains(rds)) {
                   for (com.amazonaws.services.ec2.model.Region r: regionResult.getRegions()) {
                	   Region region = Region.getRegionByName(r.getRegionName());
                       AmazonRDS rdsClient = rdsBuilder.withRegion(region.name).withCredentials(credentialsProvider).build();
                       try {
                           DescribeReservedDBInstancesResult result = rdsClient.describeReservedDBInstances();
                           for (ReservedDBInstance reservation: result.getReservedDBInstances()) {
                        	   ReservationKey key = new ReservationKey(account.getId(), region.name, reservation.getReservedDBInstanceId());
                               CanonicalReservedInstances cri = new CanonicalReservedInstances(account.getId(), region.name, reservation);
                               reservations.put(key, cri);
                           }
                       }
                       catch(AmazonRDSException e) {
                    	   logger.info("could not get RDS reservations for " + region + " " + account.getIceName() + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedDBInstances for " + region.name + " " + account.getIceName(), e);
                       }
                       rdsClient.shutdown();
                   }
               }
            	   
               if (products.contains(redshift)) {
                   for (com.amazonaws.services.ec2.model.Region r: regionResult.getRegions()) {
                	   Region region = Region.getRegionByName(r.getRegionName());
                       AmazonRedshift redshiftClient = redshiftBuilder.withRegion(region.name).withCredentials(credentialsProvider).build();
                       	
	                   try {
	                        DescribeReservedNodesResult result = redshiftClient.describeReservedNodes();
	                        for (ReservedNode reservation: result.getReservedNodes()) {
	                            ReservationKey key = new ReservationKey(account.getId(), region.name, reservation.getReservedNodeId());
	                            CanonicalReservedInstances cri = new CanonicalReservedInstances(account.getId(), region.name, reservation);
	                            reservations.put(key, cri);
	                        }
	                   }
                       catch(AmazonRedshiftException e) {
                    	   logger.info("could not get Redshift reservations for " + region + " " + account.getIceName() + ", " + e.getErrorMessage());
                       }
	                   catch (Exception e) {
	                        logger.error("error in describeReservedNodes for " + region.name + " " + account.getIceName(), e);
	                   }
	                   redshiftClient.shutdown();
                   }
	           }

               if (products.contains(es)) {
                   for (com.amazonaws.services.ec2.model.Region r: regionResult.getRegions()) {
                	   Region region = Region.getRegionByName(r.getRegionName());
                	   AWSElasticsearch elasticsearch = esBuilder.withRegion(region.name).withCredentials(credentialsProvider).build();
                	   
                	   try {
                		   DescribeReservedElasticsearchInstancesRequest request = new DescribeReservedElasticsearchInstancesRequest();
                		   DescribeReservedElasticsearchInstancesResult page = null;                		   
                		   
                           do {
                               if (page != null)
                                   request.setNextToken(page.getNextToken());
                               
                               page = elasticsearch.describeReservedElasticsearchInstances(request);
                               for (ReservedElasticsearchInstance reservation: page.getReservedElasticsearchInstances()) {
                            	   ReservationKey key = new ReservationKey(account.getId(), region.name, reservation.getReservedElasticsearchInstanceId());
                            	   CanonicalReservedInstances cri = new CanonicalReservedInstances(account.getId(), region.name, reservation);
                            	   reservations.put(key, cri);
                               }
                           } while (page.getNextToken() != null);
                	   }
                       catch(AWSElasticsearchException e) {
                    	   logger.info("could not get Elasticsearch reservations for " + region + " " + account.getIceName() + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                            logger.error("error in describeReservedElasticsearchInstances for " + region.name + " " + account.getIceName(), e);
                       }
                       elasticsearch.shutdown();
                   }
               }
               
               if (products.contains(ec)) {
                   for (com.amazonaws.services.ec2.model.Region r: regionResult.getRegions()) {
                	   Region region = Region.getRegionByName(r.getRegionName());
                	   AmazonElastiCache elastiCache = ecBuilder.withRegion(region.name).withCredentials(credentialsProvider).build();
                	   
                	   try {
                		   DescribeReservedCacheNodesResult result = elastiCache.describeReservedCacheNodes();                		   
                           for (ReservedCacheNode reservation: result.getReservedCacheNodes()) {
                        	   ReservationKey key = new ReservationKey(account.getId(), region.name, reservation.getReservedCacheNodeId());
                        	   CanonicalReservedInstances cri = new CanonicalReservedInstances(account.getId(), region.name, reservation);
                        	   reservations.put(key, cri);
                           }
                	   }
                       catch(AmazonElastiCacheException e) {
                    	   logger.info("could not get ElastiCache reservations for " + region + " " + account.getIceName() + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                            logger.error("error in describeReservedCacheNodes for " + region.name + " " + account.getIceName(), e);
                       }
                	   elastiCache.shutdown();
                   }
               }
               
            }
            catch (Exception e) {
                logger.error("Error in describeReservedInstances for " + account.getIceName(), e);
            }
        }
        archive(config, reservations);
    }
    protected void handleEC2Modifications(Map<ReservationKey, CanonicalReservedInstances> ec2Reservations, Ec2Mods mods, Region region, PriceListService pls) {
    	for (ReservationKey key: ec2Reservations.keySet()) {
    		CanonicalReservedInstances reservedInstances = ec2Reservations.get(key);
    		
	        double fixedPrice = reservedInstances.getFixedPrice();
	        PurchaseOption purchaseOption = PurchaseOption.get(reservedInstances.getOfferingType());
	
	        logger.debug("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + ", fixedPrice: " + fixedPrice);
	        
	        if (fixedPrice == 0.0 && 
	        		(purchaseOption == PurchaseOption.AllUpfront ||
	        				purchaseOption == PurchaseOption.PartialUpfront))  {
	        	// Reservation was likely modified and AWS doesn't carry forward the fixed price from the parent reservation
	        	// Get the reservation modification for this RI
	        	String parentReservationId = mods.getOriginalReservationId(reservedInstances.getReservationId());
	        	if (parentReservationId.equals(reservedInstances.getReservationId())) {
	        		if (reservedInstances.getOfferingClass().equals("standard")) {
	        			logger.error("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + ", No parent reservation found " +
	        					mods.getMod(reservedInstances.getReservationId()) + ", getting fixed price from price list");
	        		}
	        		double newFixedPrice = getFixedPrice(reservedInstances, region, pls);
	        		reservedInstances.setFixedPrice(newFixedPrice);
	        		continue;
	        	}
	        	ReservationKey parentKey = new ReservationKey(key.account, key.region, parentReservationId);
	        	CanonicalReservedInstances parentRI = ec2Reservations.get(parentKey);
	        	if (parentRI == null) {
	        		logger.error("Reservation: " + parentKey + " Not found in reservation list");
	        		continue;
	        	}
	        	/*
	        	 * If reservation instance size changed, adjust the price accordingly.
	        	 */
	        	double adjustedPrice = adjustPrice(parentRI.getInstanceType(), reservedInstances.getInstanceType(), parentRI.getFixedPrice());
	        	reservedInstances.setFixedPrice(adjustedPrice);
	        }
    	}
    }
    
    protected double getFixedPrice(CanonicalReservedInstances ri, Region region, PriceListService pls) {
    	double fixedPrice = 0.0;
		try {
			InstancePrices prices = pls.getPrices(new DateTime(ri.getStart()), ServiceCode.AmazonEC2);
			LeaseContractLength lcl = ri.getDuration() > 24 * 365 ? LeaseContractLength.threeyear : LeaseContractLength.oneyear;
			PurchaseOption po = PurchaseOption.get(ri.getOfferingType());
			OfferingClass oc = OfferingClass.valueOf(ri.getOfferingClass());
			UsageType ut = UsageType.getUsageType(ri.getInstanceType(), "hours");
			Rate rate = prices.getReservationRate(region, ut, lcl, po, oc);
			if (rate != null) {
				fixedPrice = rate.fixed;
			}
			else {
				logger.error("No rate for " + region + "," + ut + "," + lcl + "," + po + "," + oc + " in AmazonEC2 pricelist " + prices.getVersionId());
			}
		} catch (Exception e) {
			logger.error("Error trying to get pricing for reservation:" + e);
			e.printStackTrace();
		}
		return fixedPrice;
    }
    
    private double adjustPrice(String fromInstanceType, String toInstanceType, double price) {
    	String fromSize = fromInstanceType.split("\\.")[1];
    	String toSize = toInstanceType.split("\\.")[1];
    	return price * multiplier(toSize) / multiplier(fromSize);
    }
    
	double multiplier(String size) {
		Double m = instanceSizeMap.get(size);
		if (m == null) {
			if (size.endsWith("xlarge")) {
				double mult = Double.parseDouble(size.substring(0, size.lastIndexOf("xlarge")));
				return instanceSizeMap.get("xlarge") * mult;
			}
			logger.error("Cannot find multiplier for size: %s", size);
			return 1.0;
		}
		return m;
	}
    
    private Map<ReservationKey, CanonicalReservedInstances> readArchive(ProcessorConfig config) {
    	WorkBucketConfig workBucketConfig = config.workBucketConfig;
        File file = new File(workBucketConfig.localDir, archiveFilename);
        
        // read from s3 if not exists
        if (!file.exists()) {
            logger.info("downloading " + file + "...");
            AwsUtils.downloadFileIfNotExist(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix + file.getName(), file);
            logger.info("downloaded " + file);
        }
        
        archiveLastModified = file.lastModified();

        Map<ReservationKey, CanonicalReservedInstances> reservations = readReservations(file);
        logger.info("read " + reservations.size() + " reservations.");
        return reservations;
    }
    
    public static Map<ReservationKey, CanonicalReservedInstances> readReservations(File file) {
        // read from file
        Map<ReservationKey, CanonicalReservedInstances> reservations = Maps.newTreeMap();
        if (file.exists()) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String line;
                
                // skip the header
                reader.readLine();

                while ((line = reader.readLine()) != null) {
                	CanonicalReservedInstances reservation = new CanonicalReservedInstances(line);
                	
                    reservations.put(new ReservationKey(reservation.getAccountId(), reservation.getRegion(), reservation.getReservationId()), reservation);
                }
            }
            catch (Exception e) {
            	Logger logger = LoggerFactory.getLogger(ReservationService.class);
            	logger.error("error in reading " + file, e);
            }
            finally {
                if (reader != null)
                    try {reader.close();} catch (Exception e) {}
            }
        }
        return reservations;
    }
    
    private void archive(ProcessorConfig config, Map<ReservationKey, CanonicalReservedInstances> reservations) {
    	WorkBucketConfig workBucketConfig = config.workBucketConfig;
        File file = new File(workBucketConfig.localDir, archiveFilename);

        // archive to disk
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(file));
            writer.write(CanonicalReservedInstances.header());
            writer.newLine();
            for (ReservationKey key: reservations.keySet()) {
                CanonicalReservedInstances reservation = reservations.get(key);
                writer.write(reservation.toString());
                writer.newLine();
            }
        }
        catch (Exception e) {
            logger.error("",  e);
        }
        finally {
            if (writer != null)
                try {writer.close();} catch (Exception e) {}
        }
        logger.info("archived " + reservations.size() + " reservations.");

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix + file.getName(), file);
        logger.info("uploaded " + file);
    }

    protected void updateReservations(Map<ReservationKey, CanonicalReservedInstances> reservationsFromApi, AccountService accountService, long startMillis, ProductService productService, ResourceService resourceService, ReservationService reservationService) throws Exception {
        Map<PurchaseOption, Map<TagGroup, List<Reservation>>> reservationMap = Maps.newTreeMap();
        for (PurchaseOption purchaseOption: PurchaseOption.values()) {
            reservationMap.put(purchaseOption, Maps.<TagGroup, List<Reservation>>newHashMap());
        }
        Map<ReservationArn, Reservation> reservationsByArn = Maps.newHashMap();

        for (ReservationKey key: reservationsFromApi.keySet()) {
            CanonicalReservedInstances reservedInstances = reservationsFromApi.get(key);
            if (reservedInstances.getInstanceCount() <= 0) {
            	//logger.info("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + " has no instances");
                continue;
            }

            Account account = accountService.getAccountById(key.account);

            PurchaseOption utilization = PurchaseOption.get(reservedInstances.getOfferingType());
            
            long startTime = 0;
            long endTime = 0;
        	// AWS Reservations start being applied at the beginning of the hour in which the reservation was purchased.
            // The reservations stop being applied at the beginning of the hour in which the reservation expires.
            startTime = getEffectiveReservationTime(reservedInstances.getStart());
            endTime = getEffectiveReservationTime(reservedInstances.getEnd());
            
            // Modified reservations don't have updated durations
            endTime = Math.min(endTime, startTime + reservedInstances.getDuration() * 1000);
                        
            if (endTime <= startMillis) {
            	//logger.info("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + " has expired");
                continue;
            }
            
            // usage price is the sum of the usage price and the recurring hourly charge
            double usagePrice = reservedInstances.getUsagePrice() + reservedInstances.getRecurringHourlyCharges();
            double fixedPrice = reservedInstances.getFixedPrice();

            // logger.info("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + ", fixedPrice: " + fixedPrice);
            
            double hourlyFixedPrice = fixedPrice / (reservedInstances.getDuration() / 3600); // duration is in seconds, we need hours

            UsageType usageType;
            Product product;
            Zone zone = null;
            Region region = Region.getRegionByName(reservedInstances.getRegion());
            
            if (reservedInstances.isProduct(Product.Code.Ec2.serviceCode)) {
            	if (reservedInstances.getScope().equals("Availability Zone")) {
	                zone = region.getZone(reservedInstances.getAvailabilityZone());
	                if (zone == null)
	                    logger.error("Not able to find zone for EC2 reserved instances " + reservedInstances.getAvailabilityZone());
            	}
            	else if (!reservedInstances.getScope().equals("Region")) {
            		logger.error("Unknown scope value for reservation: " + reservedInstances.getReservationId() + ", scope: " + reservedInstances.getScope());
            	}
                String osStr = reservedInstances.getProductDescription();
                InstanceOs os = InstanceOs.withDescription(osStr);
                usageType = UsageType.getUsageType(reservedInstances.getInstanceType() + os.usageType, "hours");
                product = productService.getProduct(Product.Code.Ec2Instance);
            }
            else if (reservedInstances.isProduct(Product.Code.Rds.serviceCode)) {
            	InstanceDb db = InstanceDb.withDescription(reservedInstances.getProductDescription());
            	String multiAZ = reservedInstances.getMultiAZ() ? UsageType.multiAZ : "";
            	usageType = UsageType.getUsageType(reservedInstances.getInstanceType() + multiAZ + db.usageType, "hours");
            	product = productService.getProduct(Product.Code.RdsInstance);
            }
            else if (reservedInstances.isProduct(Product.Code.Redshift.serviceCode)){
            	usageType = UsageType.getUsageType(reservedInstances.getInstanceType(), "hours");
            	product = productService.getProduct(Product.Code.Redshift);
            }
            else if (reservedInstances.isProduct(Product.Code.Elasticsearch.serviceCode)){
            	usageType = UsageType.getUsageType("es." + reservedInstances.getInstanceType(), "hours");
            	product = productService.getProduct(Product.Code.Elasticsearch);
            }
            else if (reservedInstances.isProduct(Product.Code.ElastiCache.serviceCode)){
            	InstanceCache cache = InstanceCache.withDescription(reservedInstances.getProductDescription());
            	usageType = UsageType.getUsageType(reservedInstances.getInstanceType() + cache.usageType, "hours");
            	product = productService.getProduct(Product.Code.ElastiCache);
            }
            else {
            	logger.error("Unknown reserved instance type: " + reservedInstances.getProduct() + ", " + reservedInstances.toString());
            	continue;
            }
            
            ResourceGroup resourceGroup = null;
            if (resourceService != null)
            	resourceGroup = resourceService.getResourceGroup(account, product, reservedInstances.getTags(), startTime);
            ReservationArn arn = ReservationArn.get(account, region, product, reservedInstances.getReservationId());
            TagGroupRI reservationKey = TagGroupRI.get(CostType.subscription, account, region, zone, product, Operation.getReservedInstances(utilization), usageType, resourceGroup, arn);
            Reservation reservation = new Reservation( reservationKey, reservedInstances.getInstanceCount(), startTime, endTime, utilization, hourlyFixedPrice, usagePrice);

            List<Reservation> reservations = reservationMap.get(utilization).get(reservationKey);
            if (reservations == null) {
                reservationMap.get(utilization).put(reservationKey, Lists.<Reservation>newArrayList(reservation));
            }
            else {
                reservations.add(reservation);
            }
            reservationsByArn.put(arn, reservation);

            //logger.info("Add reservation " + utilization.name() + " for key " + reservationKey.toString());

        }

        reservationService.setReservations(reservationMap, reservationsByArn);
   }
    
	public class Ec2Mods {
		private List<ReservedInstancesModification> mods;
		
		Ec2Mods(List<ReservedInstancesModification> mods) {
			this.mods = mods;
		}
		
		public ReservedInstancesModification getMod(String reservationId) {
			for (ReservedInstancesModification mod: mods) {
				if (mod.getStatus().equals("failed"))
					continue;
				
				for (ReservedInstancesModificationResult result: mod.getModificationResults()) {
					if (result.getReservedInstancesId() == null) {
						logger.error("getReservedInstancesId for " + mod.getReservedInstancesIds() + " return null: " + result);
						continue;
					}
					if (result.getReservedInstancesId().equals(reservationId))
						return mod;
				}
			}
			return null;
		}
		
		public String getModResId(String reservationId) {
			ReservedInstancesModification mod = getMod(reservationId);
			return mod == null ? "" : mod.getReservedInstancesIds().get(0).getReservedInstancesId();
		}
		
		public String getOriginalReservationId(String reservationId) {
			String childId = reservationId;
			for (ReservedInstancesModification modification = getMod(childId); modification != null; modification = getMod(childId)) {
				childId = modification.getReservedInstancesIds().get(0).getReservedInstancesId();
				logger.debug("Reservation " + reservationId + " has parent " + childId);
			}
			return childId;
		}
		
		public String info() {
			int fulfilled = 0;
			int failed = 0;
			
			for (ReservedInstancesModification mod: mods) {
				if (mod.getStatus().equals("failed"))
					failed++;
				if (mod.getStatus().equals("fulfilled"))
					fulfilled++;
			}
			return "Modifications: " + fulfilled + " fulfilled, " + failed + " failed, " + mods.size();
		}
	}
    
}
