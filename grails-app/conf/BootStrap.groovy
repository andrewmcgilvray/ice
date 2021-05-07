import com.netflix.ice.common.AwsUtils;

/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.StopInstancesRequest;

import com.google.common.collect.Lists
import com.google.common.collect.Maps

import com.netflix.ice.basic.BasicManagers
import com.netflix.ice.basic.BasicProductService
import com.netflix.ice.basic.BasicReservationService
import com.netflix.ice.common.Config;
import com.netflix.ice.common.IceOptions
import com.netflix.ice.common.ProductService
import com.netflix.ice.common.PurchaseOption
import com.netflix.ice.processor.ProcessorConfig
import com.netflix.ice.processor.ReservationService
import com.netflix.ice.processor.pricelist.PriceListService
import com.netflix.ice.JSONConverter
import com.netflix.ice.reader.ReaderConfig
import com.netflix.ice.tag.Account
import com.netflix.ice.tag.Region

import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringUtils
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.slf4j.Logger
import org.slf4j.LoggerFactory


class BootStrap {
    private static boolean initialized = false;
    private static Logger logger = LoggerFactory.getLogger(BootStrap.class);

    private Config readerConfig;
    private ProcessorConfig processorConfig;

    def init = { servletContext ->
        if (initialized) {
            return;
        }

        InputStream is = null;
		boolean processOnce = false;
        AWSCredentialsProvider credentialsProvider = null;
		
        try {

            logger.info('Starting ice...');
			
            logger.info('Read ice.properties ...');
			Properties prop = getProperties("ice.propertiesfile", "ice.properties");			
			
            if (StringUtils.isEmpty(System.getProperty("ice.s3AccessKeyId")) || StringUtils.isEmpty(System.getProperty("ice.s3SecretKey")))
                credentialsProvider = new DefaultAWSCredentialsProviderChain();
            else
                credentialsProvider = new AWSCredentialsProvider() {
                        public AWSCredentials getCredentials() {
                            if (StringUtils.isEmpty(System.getProperty("ice.s3AccessToken")))
                                return new AWSCredentials() {
                                    public String getAWSAccessKeyId() {
                                        return System.getProperty("ice.s3AccessKeyId");
                                    }

                                    public String getAWSSecretKey() {
                                        return System.getProperty("ice.s3SecretKey");
                                    }
                                };
                            else
                                return new BasicSessionCredentials(System.getProperty("ice.s3AccessKeyId"), System.getProperty("ice.s3SecretKey"),
                                        System.getProperty("ice.s3AccessToken"));
                        }

                        public void refresh() {
                        }
                    };

			JSONConverter.register();
				
            Properties properties = new Properties();
            properties.setProperty(IceOptions.WORK_S3_BUCKET_NAME, prop.getProperty(IceOptions.WORK_S3_BUCKET_NAME));
            properties.setProperty(IceOptions.WORK_S3_BUCKET_REGION, prop.getProperty(IceOptions.WORK_S3_BUCKET_REGION));
            properties.setProperty(IceOptions.WORK_S3_BUCKET_PREFIX, prop.getProperty(IceOptions.WORK_S3_BUCKET_PREFIX));
			if (prop.getProperty(IceOptions.PROCESSOR_THREADS) != null)
				properties.setProperty(IceOptions.PROCESSOR_THREADS, prop.getProperty(IceOptions.PROCESSOR_THREADS));				
			properties.setProperty(IceOptions.TAG_COVERAGE, prop.getProperty(IceOptions.TAG_COVERAGE, ""));
			if (prop.getProperty(IceOptions.HOURLY_DATA) != null)
				properties.setProperty(IceOptions.HOURLY_DATA, prop.getProperty(IceOptions.HOURLY_DATA));

			// Stash any debug properties
			for (String name: prop.stringPropertyNames()) {
				if (name.startsWith(IceOptions.DEBUG + ".")) {
					properties.setProperty(name, prop.getProperty(name));
				}
			}
			
			ProductService productService = new BasicProductService();

            if ("true".equals(prop.getProperty("ice.processor"))) {
				if (!StringUtils.isEmpty(prop.getProperty(IceOptions.START_MONTH))) {
	                properties.setProperty(IceOptions.START_MONTH, prop.getProperty(IceOptions.START_MONTH));
	            }
	            else {
					DateTime now = new DateTime(DateTimeZone.UTC);
	                properties.setProperty(IceOptions.START_MONTH, "" + now.year().get() + "-" + now.monthOfYear().get());
	            }

			    if (prop.getProperty(IceOptions.PROCESS_ONCE) != null) {
                	properties.setProperty(IceOptions.PROCESS_ONCE, prop.getProperty(IceOptions.PROCESS_ONCE));
					if ("true".equals(prop.getProperty(IceOptions.PROCESS_ONCE))) {
						properties.setProperty(IceOptions.PROCESSOR_REGION, getInstanceRegion());
						properties.setProperty(IceOptions.PROCESSOR_INSTANCE_ID, getInstanceId());
						processOnce = true;
					}
                }

                properties.setProperty(IceOptions.LOCAL_DIR, prop.getProperty("ice.processor.localDir", "/mnt/ice"));
                properties.setProperty(IceOptions.BILLING_S3_BUCKET_NAME, prop.getProperty(IceOptions.BILLING_S3_BUCKET_NAME));
                properties.setProperty(IceOptions.BILLING_S3_BUCKET_REGION, prop.getProperty(IceOptions.BILLING_S3_BUCKET_REGION));
                properties.setProperty(IceOptions.BILLING_S3_BUCKET_PREFIX, prop.getProperty(IceOptions.BILLING_S3_BUCKET_PREFIX, ""));
                properties.setProperty(IceOptions.BILLING_PAYER_ACCOUNT_ID, prop.getProperty(IceOptions.BILLING_PAYER_ACCOUNT_ID, ""));
                properties.setProperty(IceOptions.BILLING_ACCESS_ROLENAME, prop.getProperty(IceOptions.BILLING_ACCESS_ROLENAME, ""));
                properties.setProperty(IceOptions.BILLING_ACCESS_EXTERNALID, prop.getProperty(IceOptions.BILLING_ACCESS_EXTERNALID, ""));
				properties.setProperty(IceOptions.BILLING_ICE_CONFIG_ACCOUNTS_BASENAME, prop.getProperty(IceOptions.BILLING_ICE_CONFIG_ACCOUNTS_BASENAME, ""));
				properties.setProperty(IceOptions.BILLING_ICE_CONFIG_TAGS_BASENAME, prop.getProperty(IceOptions.BILLING_ICE_CONFIG_TAGS_BASENAME, ""));
				properties.setProperty(IceOptions.BILLING_ICE_CONFIG_POSTPROC_BASENAME, prop.getProperty(IceOptions.BILLING_ICE_CONFIG_POSTPROC_BASENAME, ""));
                properties.setProperty(IceOptions.ROOT_NAME, prop.getProperty(IceOptions.ROOT_NAME, ""));
				properties.setProperty(IceOptions.COST_AND_USAGE_NET_UNBLENDED_START_DATE, prop.getProperty(IceOptions.COST_AND_USAGE_NET_UNBLENDED_START_DATE, ""));
				properties.setProperty(IceOptions.EDP_DISCOUNTS, prop.getProperty(IceOptions.EDP_DISCOUNTS, ""));
				properties.setProperty(IceOptions.REPORT_SUB_PREFIX, prop.getProperty(IceOptions.REPORT_SUB_PREFIX, ""));
				
				// Grab all the account properties
				for (String name: prop.stringPropertyNames()) {
					if (name.startsWith("ice.account.") || name.startsWith("ice.owneraccount.")) {
						properties.setProperty(name, prop.getProperty(name));
					}
				}

				ReservationService.ReservationPeriod reservationPeriod =
                    ReservationService.ReservationPeriod.valueOf(prop.getProperty(IceOptions.RESERVATION_PERIOD, "threeyear"));
				
                PurchaseOption reservationPurchaseOption = PurchaseOption.valueOf(prop.getProperty(IceOptions.RESERVATION_PURCHASE_OPTION, "AllUpfront"));
				properties.setProperty(IceOptions.RESERVATION_CAPACITY_POLLER, prop.getProperty(IceOptions.RESERVATION_CAPACITY_POLLER, "false"));
					
                properties.setProperty(IceOptions.CUSTOM_TAGS, prop.getProperty(IceOptions.CUSTOM_TAGS, ""));
				properties.setProperty(IceOptions.RESERVATION_ID_TAGS, prop.getProperty(IceOptions.RESERVATION_ID_TAGS, "false"));
				
                properties.setProperty(IceOptions.RESOURCE_GROUP_COST, prop.getProperty(IceOptions.RESOURCE_GROUP_COST, "modeled"));
				properties.setProperty(IceOptions.WRITE_JSON_FILES, prop.getProperty(IceOptions.WRITE_JSON_FILES, ""));
				
				
				ReservationService reservationService = new BasicReservationService(reservationPeriod, reservationPurchaseOption);
				PriceListService priceListService = new PriceListService(
					properties.getProperty(IceOptions.LOCAL_DIR), 
					properties.getProperty(IceOptions.WORK_S3_BUCKET_NAME), 
					properties.getProperty(IceOptions.WORK_S3_BUCKET_PREFIX));
				
				
                processorConfig = new ProcessorConfig(
                        properties,
                        credentialsProvider,
                        productService,
                        reservationService,
						priceListService)
				processorConfig.start();
            }

            if ("true".equals(prop.getProperty("ice.reader"))) {
                properties.setProperty(IceOptions.LOCAL_DIR, prop.getProperty("ice.reader.localDir", "/mnt/ice"));
                if (prop.getProperty(IceOptions.MONTHLY_CACHE_SIZE) != null)
                    properties.setProperty(IceOptions.MONTHLY_CACHE_SIZE, prop.getProperty(IceOptions.MONTHLY_CACHE_SIZE));
                if (prop.getProperty(IceOptions.CURRENCY_RATE) != null)
                    properties.setProperty(IceOptions.CURRENCY_RATE, prop.getProperty(IceOptions.CURRENCY_RATE));
                if (prop.getProperty(IceOptions.CURRENCY_SIGN) != null)
                    properties.setProperty(IceOptions.CURRENCY_SIGN, prop.getProperty(IceOptions.CURRENCY_SIGN));
                if (prop.getProperty(IceOptions.HIGHSTOCK_URL) != null)
                    properties.setProperty(IceOptions.HIGHSTOCK_URL, prop.getProperty(IceOptions.HIGHSTOCK_URL));
				if (prop.getProperty(IceOptions.COMPANY_NAME) != null)
					properties.setProperty(IceOptions.COMPANY_NAME, prop.getProperty(IceOptions.COMPANY_NAME));
                if (prop.getProperty(IceOptions.DASHBOARD_NOTICE) != null)
                    properties.setProperty(IceOptions.DASHBOARD_NOTICE, prop.getProperty(IceOptions.DASHBOARD_NOTICE));
                if (prop.getProperty(IceOptions.DASHBOARD_LINKS) != null)
                    properties.setProperty(IceOptions.DASHBOARD_LINKS, prop.getProperty(IceOptions.DASHBOARD_LINKS));
				properties.setProperty(IceOptions.ENABLE_REPROCESS_REQUESTS, prop.getProperty(IceOptions.ENABLE_REPROCESS_REQUESTS, "false"));
					
                readerConfig = new ReaderConfig(
                        properties,
                        credentialsProvider,
                        new BasicManagers(true),
                        productService,
                        null
                );
                readerConfig.start();
            }

            initialized = true;

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Startup failed", e);
			if (processOnce && credentialsProvider != null) {
				// Stop this EC2 instance
				stopInstance(credentialsProvider);
			}
            System.exit(0);
        }
    }

    def destroy = {
        logger.info("Shutting down ice...");

        try {
            if (processorConfig != null)
                processorConfig.shutdown();
            if (readerConfig != null)
                readerConfig.shutdown();
        }
        catch (Exception e) {
            logger.error("Failed to shut down...", e);
        }

        logger.info("Shut down complete.");
        initialized = false;
    }

    private static String getCurrentRole() throws IOException {
        String role = httpGet("http://169.254.169.254/latest/meta-data/iam/security-credentials/");
        logger.info("Found role: " + role);
        return role;
    }
    private static String getInstanceRegion() throws IOException {
        String region = httpGet("http://169.254.169.254/latest/meta-data/placement/availability-zone");
		region = region.substring(0, region.length()-1);
        logger.info("Found region: " + region);
        return region;
    }
    private static String getInstanceId() throws IOException {
        String instanceId = httpGet("http://169.254.169.254/latest/meta-data/instance-id");
        logger.info("Found instance ID: " + instanceId);
        return instanceId;
    }
	private static String httpGet(String url) throws IOException {
		URL reqUrl = new URL(url);
		URLConnection urlConnection = reqUrl.openConnection();
		InputStream input = urlConnection.getInputStream();
		String resp = IOUtils.toString(input).trim();
		input.close();
		return resp;
	}
	
	private void stopInstance(AWSCredentialsProvider credentialsProvider) {
		ClientConfiguration clientConfig = new ClientConfiguration();
		String proxyHost = System.getProperty("https.proxyHost");
		String proxyPort = System.getProperty("https.proxyPort");
		if (proxyHost != null && proxyPort != null) {
			clientConfig.setProxyHost(proxyHost);
			clientConfig.setProxyPort(Integer.parseInt(proxyPort));
		}

		AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
						.withRegion(getInstanceRegion())
						.withCredentials(credentialsProvider)
						.withClientConfiguration(clientConfig)
						.build();
		
		try {
			String[] instanceIds = new String[1];
			instanceIds[0] = getInstanceId();
			StopInstancesRequest request = new StopInstancesRequest().withInstanceIds(instanceIds);
			ec2.stopInstances(request);
		}
		catch (Exception e) {
			logger.error("error in stopInstances", e);
		}
		ec2.shutdown();
	}
	
	private Properties getProperties(String key, String defaultValue) {
		Properties prop = new Properties();
        InputStream is = null;
		try {
			is = getClass().getClassLoader().getResourceAsStream(System.getProperty(key, defaultValue));
			if (is == null) {
				if (System.getenv().get("ICE_HOME") == null)
					throw new IllegalArgumentException("ICE_HOME is not set.");
				is = new FileInputStream(new File(System.getenv().get("ICE_HOME"), System.getProperty(key, defaultValue)));
			}
			prop.load(is);
        }
        finally {
            if (is != null)
                is.close();
        }
		return prop;
	}
}
