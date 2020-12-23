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

public class IceOptions {

    /**
     * Data start date in YYYY-MM format.
     */
    public static final String START_MONTH = "ice.startMonth";

    /**
     * Property for company name. It must be specified in ReaderConfig.
     */
    public static final String COMPANY_NAME = "ice.companyName";

    /**
     * Property for currency sign. The default value is "$".
     */
    public static final String CURRENCY_SIGN = "ice.currencySign";

    /**
     * Property for currency rate. The default value is "1".
     */
    public static final String CURRENCY_RATE = "ice.currencyRate";

    /**
     * The URL of highstock.js. The default value is the Highcharts CDN; change this if you need to
     * serve it from somewhere else (for example, if you need HTTPS).
     */
    public static final String HIGHSTOCK_URL = "ice.highstockUrl";

    /**
     * s3 bucket name where billing files are located. For multiple payer accounts, multiple bucket names can be specified delimited by comma ",".
     * Only read permission is needed. It must be specified in Config.
     */
    public static final String BILLING_S3_BUCKET_NAME = "ice.billing_s3bucketname";

    /**
     * Region for billing s3 bucket. It should be specified for buckets using v4 validation ",".
     * It must be specified in Config.
     */
    public static final String BILLING_S3_BUCKET_REGION = "ice.billing_s3bucketregion";

    /**
     * Prefix of billing files in billing s3 bucket. For multiple payer accounts, multiple bucket prefixes can be specified delimited by comma ",".
     * It must be specified in Config.
     */
    public static final String BILLING_S3_BUCKET_PREFIX = "ice.billing_s3bucketprefix";

    /**
     * Payer account id. Must be specified if across-accounts role is used to access billing files. For multiple payer accounts, acocunt ids can
     * be specified delimited by comma ",".
     */
    public static final String BILLING_PAYER_ACCOUNT_ID = "ice.billing_payerAccountId";

    /**
     * Billing file access role name to assume. Must be specified if across-accounts role is used to access billing files. For multiple payer accounts,
     * role names can be specified delimited by comma ",".
     */
    public static final String BILLING_ACCESS_ROLENAME = "ice.billing_accessRoleName";

    /**
     * Billing file access external ID. It is optional. Specify it if cross-accounts role is used to access billing files and external id is needed.
     * For multiple payer accounts, external ids can be specified delimited by comma ",".
     */
    public static final String BILLING_ACCESS_EXTERNALID = "ice.billing_accessExternalId";
    
    /**
     * Billing data config file basenames. These optional parameter allows you to override the default basenames for the billing bucket configurations files.
     * Both ".json" and ".yml" extensions are supported.
     */
    public static final String BILLING_ICE_CONFIG_ACCOUNTS_BASENAME = "ice.billingConfigAccountsBasename";
    public static final String BILLING_ICE_CONFIG_TAGS_BASENAME = "ice.billingConfigTagsBasename";
    public static final String BILLING_ICE_CONFIG_POSTPROC_BASENAME = "ice.billingConfigPostProcBasename";
    
    /**
     * Root name to be used in parent paths for an account. The path expresses where the account is positioned in the organization hierarchy.
     * For multiple payer accounts, root names can be specified delimited by comma ",".
     */
    public static final String ROOT_NAME = "ice.rootName";

    /**
     * Start date for use of Enterprise Discount Program NetUnblended rates and costs.
     */
    public static final String COST_AND_USAGE_NET_UNBLENDED_START_DATE = "ice.costAndUsageNetUnblendedStartDate";
    
    /**
     * Enterprise discount program discounts
     */
    public static final String EDP_DISCOUNTS = "ice.edpDiscounts";
    
    /**
     * User can configure their custom tags.
     */
    public static final String CUSTOM_TAGS = "ice.customTags";

    /**
     * Boolean flag to indicated whether to include reservation IDs as an additional custom tag.
     */
    public static final String RESERVATION_ID_TAGS = "ice.reservationIdTags";

    /**
     * Boolean Flag whether to use blended or Unblended Costs.  Default is UnBlended Cost(false)
     */
    public static final String USE_BLENDED = "ice.use_blended";

    /**
     * s3 bucket name where output files are to be store. Both read and write permissions are needed. It must be specified in Config.
     */
    public static final String WORK_S3_BUCKET_NAME = "ice.work_s3bucketname";

     /**
     * Region for output files s3 bucket. It should be specified for buckets using v4 validation.
     * It must be specified in Config.
     */
    public static final String WORK_S3_BUCKET_REGION = "ice.work_s3bucketregion";

    /**
     * Prefix of output files in output s3 bucket. It must be specified in Config.
     */
    public static final String WORK_S3_BUCKET_PREFIX = "ice.work_s3bucketprefix";

    /**
     * Local directory. It must be specified in Config.
     */
    public static final String LOCAL_DIR = "ice.localDir";

    /**
     * Monthly data cache size for reader. Default is 12.
     */
    public static final String MONTHLY_CACHE_SIZE = "ice.monthlycachesize";

    /**
     * url prefix, e.g. http://ice.netflix.com/
     */
    public static final String URL_PREFIX = "ice.urlPrefix";

    /**
    * What pricing data ice should use when calculating usage costs for resource groups
    */
    public static final String RESOURCE_GROUP_COST = "ice.resourceGroupCost";

    /**
     * enable single-pass run of billing file processor. Will shut down EC2 instance when pass completes.
     */
    public static final String PROCESS_ONCE = "ice.processOnce";
    
    /**
     * enable reader to set reprocess flags and start processor
     */
    public static final String ENABLE_REPROCESS_REQUESTS = "ice.enableReprocessRequests";
    
    /**
     * AWS region where this processor instance is running. Used to shut down EC2 instance when pass completes if processOnce is true.
     */
    public static final String PROCESSOR_REGION = "ice.processorRegion";

    /**
     * AWS EC2 instance ID of this processor. Used to shut down EC2 instance when pass completes if processOnce is true.
     */
    public static final String PROCESSOR_INSTANCE_ID = "ice.processorInstanceId";
    
    /**
     * Number of threads to use when processing cost and usage reports.
     */
    public static final String PROCESSOR_THREADS = "ice.numthreads";
    
    /**
     * default reservation period, possible values are oneyear, threeyear
     */
    public static final String RESERVATION_PERIOD = "ice.reservationPeriod";
    
    /**
     * default reservation utilization, possible values are HEAVY and HEAVY_PARTIAL.
     */
    public static final String RESERVATION_PURCHASE_OPTION = "ice.reservationPurchaseOption";
    
    /**
     * Reservation capacity poller: whether or not to start reservation capacity poller
     */
    public static final String RESERVATION_CAPACITY_POLLER = "ice.reservationCapacityPoller";

    /**
     * write JSON data files for ingest into services such as ElasticSearch
     */
    public static final String WRITE_JSON_FILES = "ice.writeJsonFiles";
    
    /**
     * enable tag coverage metrics: none, basic, withUserTags
     */
    public static final String TAG_COVERAGE = "ice.tagCoverage";
    
    /**
     * enable hourly data (default is true)
     */
    public static final String HOURLY_DATA = "ice.hourlyData";
    
    /**
     * debug flags
     */
    public static final String DEBUG = "ice.debug";
    
    /**
     * dashboard notice
     */
    public static final String DASHBOARD_NOTICE = "ice.notice";
    
    /**
     * post processor report sub-prefix - all report S3 keys will have this value inserted
     * between the bucket prefix and filename if a value is specified. 
     * Can be used to keep separate processor instances from writing over each other.
     */
    public static final String REPORT_SUB_PREFIX = "ice.reportSubPrefix";
}
