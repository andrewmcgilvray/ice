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
     * Root name to be used in parent paths for an account. The path expresses where the account is positioned in the organization hierarchy.
     * For multiple payer accounts, root names can be specified delimited by comma ",".
     */
    public static final String ROOT_NAME = "ice.rootName";

    /**
     * s3 bucket name where kubernetes files are located. For multiple payer accounts, multiple bucket names can be specified delimited by comma ",".
     * Only read permission is needed. It must be specified in Config.
     */
    public static final String KUBERNETES_S3_BUCKET_NAME = "ice.kubernetes_s3bucketname";

    /**
     * Region for kubernetes s3 bucket. It should be specified for buckets using v4 validation ",".
     * It must be specified in Config.
     */
    public static final String KUBERNETES_S3_BUCKET_REGION = "ice.kubernetes_s3bucketregion";

    /**
     * Prefix of kubernetes files in billing s3 bucket. For multiple payer accounts, multiple bucket prefixes can be specified delimited by comma ",".
     * It must be specified in Config.
     */
    public static final String KUBERNETES_S3_BUCKET_PREFIX = "ice.kubernetes_s3bucketprefix";

    /**
     * Account id. Must be specified if across-accounts role is used to access kubernetes files. For multiple accounts, acocunt ids can
     * be specified delimited by comma ",".
     */
    public static final String KUBERNETES_ACCOUNT_ID = "ice.kubernetes_accountId";

    /**
     * Kubernetes file access role name to assume. Must be specified if across-accounts role is used to access kubernetes files. For multiple payer accounts,
     * role names can be specified delimited by comma ",".
     */
    public static final String KUBERNETES_ACCESS_ROLENAME = "ice.kubernetes_accessRoleName";

    /**
     * Kubernetes file access external ID. It is optional. Specify it if cross-accounts role is used to access kubernetes files and external id is needed.
     * For multiple payer accounts, external ids can be specified delimited by comma ",".
     */
    public static final String KUBERNETES_ACCESS_EXTERNALID = "ice.kubernetes_accessExternalId";
    
    /**
     * Start date for transition to Cost and Usage Reports in YYYY-MM format.
     */
    public static final String COST_AND_USAGE_START_DATE = "ice.costAndUsageStartDate";
    
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
     * User can configure their additional tags to add to custom tags displayed in code coverage dashboard.
     */
    public static final String ADDITIONAL_TAGS = "ice.additionalTags";

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
    public static final String RESERVATION_UTILIZATION = "ice.reservationUtilization";
    
    /**
     * Reservation capacity poller: whether or not to start reservation capacity poller
     */
    public static final String RESERVATION_CAPACITY_POLLER = "ice.reservationCapacityPoller";

    /**
     * Reservation capacity poller: whether or not to start reservation capacity poller
     */
    public static final String FAMILY_RI_BREAKOUT = "ice.breakoutFamilyReservationUsage";

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
}
