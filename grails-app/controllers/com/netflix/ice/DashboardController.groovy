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

package com.netflix.ice

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import grails.converters.JSON

import com.netflix.ice.tag.ConsolidatedOperation
import com.netflix.ice.tag.CostType
import com.netflix.ice.tag.FamilyTag
import com.netflix.ice.tag.Product
import com.netflix.ice.tag.Account
import com.netflix.ice.tag.Region
import com.netflix.ice.tag.UserTag
import com.netflix.ice.tag.UserTagKey
import com.netflix.ice.tag.Zone
import com.netflix.ice.tag.UsageType
import com.netflix.ice.tag.OrganizationalUnit
import com.netflix.ice.tag.Operation
import com.netflix.ice.tag.ResourceGroup
import com.netflix.ice.tag.Tag
import com.netflix.ice.tag.TagType

import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone
import org.joda.time.DateTime
import org.joda.time.Interval

import com.netflix.ice.processor.Instances
import com.netflix.ice.processor.TagCoverageMetrics
import com.netflix.ice.reader.*;
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.google.common.collect.Maps

import org.json.JSONObject

import com.netflix.ice.basic.TagCoverageDataManager
import com.netflix.ice.common.ConsolidateType
import com.netflix.ice.common.Instance
import com.netflix.ice.common.TagConfig

import org.joda.time.Hours
import org.slf4j.Logger;
import org.slf4j.LoggerFactory
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.StopWatch;

import com.netflix.ice.common.AwsUtils


class DashboardController {
    private static Logger logger = LoggerFactory.getLogger(DashboardController.class);

	private static ReaderConfig config = ReaderConfig.getInstance();
    private static Managers managers = config == null ? null : config.managers;
    private static DateTimeFormatter dateFormatterForDownload = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC);
    private static DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd hha").withZone(DateTimeZone.UTC);
    private static DateTimeFormatter dayFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);

	static allowedMethods = [
		index: "GET",
		getReservationOps: "GET",
		getSavingsPlanOps: "GET",
		getUtilizationOps: "GET",
		getTagConfigs: "GET",
		getAccounts: "GET",
		getRegions: "POST",
		getZones: "POST",
		getProducts: "POST",
		userTagValues: "POST",
		getOperations: "POST",
		getUsageTypes: "POST",
		tags: "GET",
		getData: "POST",
		readerStats: "GET",
		getTimeSpan: "GET",
		instance: "GET",
		summary: "GET",
		detail: "GET",
		tagcoverage: "GET",
		reservation: "GET",
		savingsplans: "GET",
		utilization: "GET",
		getProcessorStatus: "GET",
		getProcessorState: "GET",
		setReprocess: "POST",
		startProcessor: "POST",
		getSubscriptions: "GET",
		getMonths: "GET",
		getPostProcessorStats: "GET",
	];
			
    private static ReaderConfig getConfig() {
        if (config == null) {
            config = ReaderConfig.getInstance();
        }
        return config;
    }

    private static Managers getManagers() {
        if (managers == null) {
            managers = ReaderConfig.getInstance().managers;
        }
        return managers;
    }

    def index = {
        redirect(action: "summary")
    }
	
	def getReservationOps = {
		boolean showLent = params.containsKey("showLent") ? params.getBoolean("showLent") : false;
		
		def data = [];
		for (Operation op: Operation.getReservationOperations(showLent)) {
			data.add(op.name);
		}		
		
        def result = [status: 200, data: data]
        render result as JSON
	}
	
	def getSavingsPlanOps = {
		boolean showLent = params.containsKey("showLent") ? params.getBoolean("showLent") : false;
		
		def data = [];
		for (Operation op: Operation.getSavingsPlanOperations(showLent)) {
			data.add(op.name);
		}
		def result = [status: 200, data: data]
		render result as JSON
	}
	
	def getUtilizationOps = {
		List<Operation> resOps = Operation.getReservationOperations(false);
		def data = [];
		for (Operation op: resOps) {
			if (op.isOnDemand() || op.isSpot() || op.isUsed() || op.isBorrowed()) {
				data.add(op.name);
			}
		}
		
		def result = [status: 200, data: data]
		render result as JSON
	}

    def getAccounts = {
        def result = [status: 200, data: doGetAccounts()]
		render result as JSON
    }

    def getRegions = {
        def text = request.reader.text;
        JSONObject query = (JSONObject)JSON.parse(text);
        List<Account> accounts = getConfig().accountService.getAccounts(listParams(query, "account"));

        TagGroupManager tagGroupManager = getManagers().getTagGroupManager(null);
        Collection<Region> data = tagGroupManager == null ? [] : tagGroupManager.getRegions(new TagLists(accounts));

        def result = [status: 200, data: data]
        render result as JSON
    }

    def getZones = {
        def text = request.reader.text;
        JSONObject query = (JSONObject)JSON.parse(text);
        List<Account> accounts = getConfig().accountService.getAccounts(listParams(query, "account"));
        List<Region> regions = Region.getRegions(listParams(query, "region"));

        TagGroupManager tagGroupManager = getManagers().getTagGroupManager(null);
        Collection<Zone> data = tagGroupManager == null ? [] : tagGroupManager.getZones(new TagLists(accounts, regions));

        def result = [status: 200, data: data]
        render result as JSON
    }

    def getProducts = {
        def text = request.reader.text;
        JSONObject query = (JSONObject)JSON.parse(text);
        List<Account> accounts = getConfig().accountService.getAccounts(listParams(query, "account"));
        List<Region> regions = Region.getRegions(listParams(query, "region"));
        List<Zone> zones = Zone.getZones(listParams(query, "zone"));
        List<Operation> operations = Operation.getOperations(listParams(query,"operation"));
        List<Product> products = getConfig().productService.getProducts(listParams(query,"product"));
        boolean resources = params.getBoolean("resources");
        boolean showZones = params.getBoolean("showZones");
        if (showZones && (zones == null || zones.size() == 0)) {
            zones = Lists.newArrayList(getManagers().getTagGroupManager(null).getZones(new TagLists(accounts)));
        }

        Collection<Product> data;
        if (resources) {
            data = Sets.newTreeSet();
            for (Product product: getManagers().getProducts()) {
                if (product == null)
                    continue;

                TagGroupManager tagGroupManager = getManagers().getTagGroupManager(product);
                Collection<Product> tmp = tagGroupManager.getProducts(new TagLists(accounts, regions, zones));
                data.addAll(tmp);
            }
        }
        else {
            TagGroupManager tagGroupManager = getManagers().getTagGroupManager(null);
            data = tagGroupManager == null ? [] : tagGroupManager.getProducts(new TagLists(accounts, regions, zones, products, operations));
        }

        def result = [status: 200, data: data]
        render result as JSON
    }
		
	def userTagValues = {
        def text = request.reader.text;
        JSONObject query = (JSONObject)JSON.parse(text);
        List<Account> accounts = getConfig().accountService.getAccounts(listParams(query, "account"));
        List<Region> regions = Region.getRegions(listParams(query, "region"));
        List<Zone> zones = Zone.getZones(listParams(query, "zone"));
        Collection<Product> products = getConfig().productService.getProducts(listParams(query, "product"));
		int index = query.getInt("index");

		// If no products specified, get them all
		if (products.empty)
			products = getManagers().getProducts();
			
		Collection<UserTag> data = getManagers().getUserTagValues(accounts, regions, zones, products, index);

		def result = [status: 200, data: data]
		render result as JSON
	}

    def getOperations = {
        def text = request.reader.text;
        JSONObject query = (JSONObject)JSON.parse(text);
        List<Account> accounts = getConfig().accountService.getAccounts(listParams(query, "account"));
        List<Region> regions = Region.getRegions(listParams(query, "region"));
        List<Zone> zones = Zone.getZones(listParams(query, "zone"));
        List<Product> products = getConfig().productService.getProducts(listParams(query, "product"));
        List<Operation> operations = Operation.getOperations(listParams(query, "operation"));
        boolean resources = query.has("resources") ? query.getBoolean("resources") : false;
        boolean forReservation = query.has("forReservation") ? query.getBoolean("forReservation") : false;
		boolean forSavingsPlans = query.has("forSavingsPlans") ? query.getBoolean("forSavingsPlans") : false;
		boolean showLent = query.has("showLent") ? query.getBoolean("showLent") : false;
		boolean isCost = query.has("usage_cost") ? query.getString("usage_cost").equals("cost") : false;

		List<Operation.Identity.Value> exclude = Operation.exclude(listParams(query, "exclude"), showLent, isCost, forReservation || forSavingsPlans);
		
        Collection<Operation> data = getManagers().getOperations(new TagLists(accounts, regions, zones, products, operations, null, null), products, exclude, resources);
		
        def result = [status: 200, data: data]
        render result as JSON
    }

    def getUsageTypes = {
        def text = request.reader.text;
        JSONObject query = (JSONObject)JSON.parse(text);
        List<Account> accounts = getConfig().accountService.getAccounts(listParams(query, "account"));
        List<Region> regions = Region.getRegions(listParams(query, "region"));
        List<Zone> zones = Zone.getZones(listParams(query, "zone"));
        List<Product> products = getConfig().productService.getProducts(listParams(query, "product"));
        List<Operation> operations = Operation.getOperations(listParams(query, "operation"));
        boolean resources = query.has("resources") ? query.getBoolean("resources") : false;
		
        Collection<Product> data;
        if (resources) {
            data = Sets.newTreeSet();
            if (products.size() == 0) {
                products = Lists.newArrayList(getManagers().getProducts());
            }
            for (Product product: products) {
                if (product == null)
                    continue;

                TagGroupManager tagGroupManager = getManagers().getTagGroupManager(product);
                Collection<UsageType> result = tagGroupManager.getUsageTypes(new TagLists(accounts, regions, zones, null, operations, null, null));
                data.addAll(result);
            }
        }
        else {
            TagGroupManager tagGroupManager = getManagers().getTagGroupManager(null);
            data = tagGroupManager == null ? [] : tagGroupManager.getUsageTypes(new TagLists(accounts, regions, zones, products, operations, null, null));
        }

        def result = [status: 200, data: data]
        render result as JSON
    }
	
	def tags = {
		def result = [status: 200, data: config.userTagKeys]
		render result as JSON		
	}
	
	def getTagConfigs = {
		Map<String, Map<String, TagConfig>> tagConfigs = config.tagConfigs;
		Map<String, Map<String, TagConfig>> data = Maps.newHashMap();
		for (String payerId: tagConfigs.keySet()) {
			Account payerAccount = config.accountService.getAccountById(payerId);
			String payerStr = payerAccount.getIceName() + "(" + payerAccount.getId() + ")";
			data.put(payerStr, tagConfigs.get(payerId));
		}
		def result = [status: 200, data: data]
		render result as JSON		
	}

    def download = {
		String dashboard = params.containsKey("dashboard") ? params.get("dashboard") : "";
		
		if (dashboard.equals("accounts"))
			download_accounts();
		else if (dashboard.equals("subscriptions"))
			download_subscriptions();
		else
			download_data();
    }
	
	private download_accounts() {
		String body = getConfig().accountService.getAccountsReport();
		
		response.setContentType("application/octet-stream;");
		response.setContentLength(body.length());		
        response.setHeader("Content-disposition", "attachment;filename=aws-accounts.csv");
        response.outputStream << body;
        response.outputStream.flush();
		return;
	}
	
	private Collection<Account> doGetAccounts() {
		boolean all = params.getBoolean("all");
		Collection<Account> data = null;
		if (all) {
			data = getConfig().accountService.getAccounts();
		}
		else {
			TagGroupManager tagGroupManager = getManagers().getTagGroupManager(null);
			data = tagGroupManager == null ? [] : tagGroupManager.getAccounts(new TagLists());
		}
		return data;
	}
	
	private download_subscriptions() {
		Managers.SubscriptionType subType = Managers.SubscriptionType.valueOf(params.get("type"));
		String month = params.get("month");
		String body = getManagers().getSubscriptionsReport(subType, month);
		
		response.setContentType("application/octet-stream;");
		response.setContentLength(body.length());
		response.setHeader("Content-disposition", "attachment;filename=aws-subscriptions-" + subType.toString().toLowerCase() + ".csv");
		response.outputStream << body;
		response.outputStream.flush();
		return;
	}
		
	private download_data() {
		JSONObject query = new JSONObject();
		for (Map.Entry entry: params.entrySet()) {
			query.put(entry.getKey(), entry.getValue());
		}

        def result = doGetData(query);

        File file = File.createTempFile("aws", "csv");

        BufferedWriter bwriter = new BufferedWriter(new FileWriter(file));
        Long start = result.start;
        int num = result.data.size() > 0 ? result.data.values().iterator().next().length : 0;

        String[] record = new String[result.data.size() + 1];
        record[0] = "Time";
        int index = 1;
        for (Map.Entry entry: result.data) {
            record[index++] = entry.getKey().name;
        }
        bwriter.write(StringUtils.join(record, ","));
        bwriter.newLine();

        ConsolidateType consolidateType = ConsolidateType.valueOf(query.getString("consolidate"));
        for (int timeIndex = 0; timeIndex < num; timeIndex++) {
            record[0] = dateFormatterForDownload.print(start);
            index = 1;
            for (Map.Entry entry: result.data) {
                double[] values = entry.getValue();
                record[index++] = values[timeIndex];
            }
            bwriter.write(StringUtils.join(record, ","));
            bwriter.newLine();
            if (consolidateType != ConsolidateType.monthly)
                start += result.interval;
            else
                start = new DateTime(start, DateTimeZone.UTC).plusMonths(1).getMillis()
        }
        bwriter.close();

        response.setHeader("Content-Type","application/octet-stream;")
        response.setHeader("Content-Length", "${file.size()}")
        response.setHeader("Content-disposition", "attachment;filename=aws.csv")

        FileInputStream input = new FileInputStream(file);
        response.outputStream << input;
        response.outputStream.flush();
        input.close();
        file.delete();
        return;
    }

    def getData = {
        def text = request.reader.text;
        JSONObject query = (JSONObject)JSON.parse(text);
		
        def result = doGetData(query);
        render result as JSON
    }
	
	def readerStats = {
        Object o = params;
        boolean csv = params.getBoolean("csv");
		render getManagers().getStatistics(csv);
	}

    def getTimeSpan = {
        int spans = Integer.parseInt(params.spans);
        DateTime end = dateFormatter.parseDateTime(params.end);
        ConsolidateType consolidateType = ConsolidateType.valueOf(params.consolidate);

        DateTime start;
        if (consolidateType == ConsolidateType.daily) {
            end = end.plusDays(1).withMillisOfDay(0);
            start = end.minusDays(spans);
        }
        else if (consolidateType == ConsolidateType.hourly) {
            start = end.minusHours(spans);
        }
        else if (consolidateType == ConsolidateType.weekly) {
            end = end.plusDays(1).withMillisOfDay(0);
            end = end.plusDays( (8-end.dayOfWeek) % 7 );
            start = end.minusWeeks(spans);
        }
        else if (consolidateType == ConsolidateType.monthly) {
            end = end.plusDays(1).withMillisOfDay(0).plusMonths(1).withDayOfMonth(1);
            start = end.minusMonths(spans);
        }

        def result = [status: 200, start: dateFormatter.print(start), end: dateFormatter.print(end)];
        render result as JSON
    }
	
	def instance = {
		Collection<Instance> instances = getManagers().getInstances(params.id);
		if (instances != null) {
			def result = [];
			for (Instance i: instances) {
				def zone = (i.zone == null) ? null : i.zone.name;
				result += [id: i.id, type: i.type, accountId: i.account.id, accountName: i.account.name, region: i.region.name, zone: zone, product: i.product.name, tags: i.tags];
			}
						
			response.status = 200;
			render result as JSON
		}
		else {
			response.status = 404;
		}
	}
	
	def getProcessorStatus = {		
        def result = [status: 200, data: getManagers().getProcessorStatus()];
        render result as JSON
	}
	
	def getProcessorState = {		
        def result = [status: 200, data: getManagers().getProcessorState()];
        render result as JSON
	}
	
	def setReprocess = {
		if (getConfig().enableReprocessRequests) {
			def text = request.reader.text;
			JSONObject query = (JSONObject)JSON.parse(text);
			String month = query.getString("month");
			if (month != null) {
				boolean state = query.has("state") ? query.getBoolean("state") : true;
				getManagers().reprocess(month, state);
				response.status = 200;
			}
			else {
				response.status = 400;
			}
		}
		else {
			response.status = 403;
		}
		def result = [status: response.status];
		render result as JSON
	}
	
	def startProcessor = {
		if (getConfig().enableReprocessRequests) {
			if (getManagers().startProcessor())
				response.status = 200;
			else
				response.status = 500;
		}
		else {
			response.status = 403;
		}
		def result = [status: response.status];
		render result as JSON
	}
	
	def getSubscriptions = {
		Managers.SubscriptionType subType = Managers.SubscriptionType.valueOf(params.get("type"));
		String month = params.get("month");
		def result = [status: 200, data: getManagers().getSubscriptions(subType, month)];
		render result as JSON
	}
		
	def getMonths = {
		def result = [status: 200, data: getManagers().getMonths()];
		render result as JSON
	}
	
	def getPostProcessorStats = {
		String month = params.get("month");
		def result = [status: 200, data: getManagers().getPostProcessorStats(month)];
		render result as JSON
	}

    def summary = {}

    def detail = {}

    def tagcoverage = {}

    def reservation = {}

    def savingsplans = {}

    def utilization = {}

	def resourceinfo = {}
	
	def accounts = {}
	
	def tagconfigs = {}
	
	def statistics = {}
	
	def processorstatus = {}
	
	def subscriptions = {}
	
    private Map doGetData(JSONObject query) {
		logger.debug("******** doGetData: called");

		TagGroupManager tagGroupManager = getManagers().getTagGroupManager(null);
        if (tagGroupManager == null) {
            return [status: 200, start: 0, data: [:], stats: [:], groupBy: "None"];
        }
		
		StopWatch sw = new StopWatch();
		sw.start();

		TagType groupBy = query.has("groupBy") ? (query.getString("groupBy").equals("None") ? null : TagType.valueOf(query.getString("groupBy"))) : null;
		boolean groupByOrgUnit = groupBy == TagType.OrgUnit;
		if (groupByOrgUnit)
			groupBy = TagType.Account;
		boolean groupByCostType = groupBy == TagType.CostType;
		if (groupByCostType)
			groupBy = TagType.Operation;
		
        boolean isCost = query.has("isCost") ? query.getBoolean("isCost") : true;
        boolean breakdown = query.has("breakdown") ? query.getBoolean("breakdown") : false;
        boolean showsps = query.has("showsps") ? query.getBoolean("showsps") : false;
        boolean factorsps = query.has("factorsps") ? query.getBoolean("factorsps") : false;
		UsageUnit usageUnit = UsageUnit.Instances;
		if (!isCost) {
			if (query.has("usageUnit") && !query.getString("usageUnit").isEmpty())
				usageUnit = UsageUnit.valueOf(query.getString("usageUnit"));
		}
        AggregateType aggregate = query.has("aggregate") ? AggregateType.valueOf(query.getString("aggregate")) : AggregateType.none;
        List<Account> accounts = getConfig().accountService.getAccounts(listParams(query, "account"));
        List<Region> regions = Region.getRegions(listParams(query, "region"));
        List<Zone> zones = Zone.getZones(listParams(query, "zone"));
        List<Product> products = getConfig().productService.getProducts(listParams(query, "product"));
        List<Operation> operations = Operation.getOperations(listParams(query, "operation"));
        List<UsageType> usageTypes = UsageType.getUsageTypes(listParams(query, "usageType"));
        DateTime end = query.has("spans") ? dayFormatter.parseDateTime(query.getString("end")) : dateFormatter.parseDateTime(query.getString("end"));
        ConsolidateType consolidateType = query.has("consolidate") ? ConsolidateType.valueOf(query.getString("consolidate")) : ConsolidateType.hourly;
        boolean forReservation = query.has("forReservation") ? query.getBoolean("forReservation") : false;
        boolean forSavingsPlans = query.has("forSavingsPlans") ? query.getBoolean("forSavingsPlans") : false;
		boolean showLent = query.has("showLent") ? query.getBoolean("showLent") : false;
        boolean elasticity = query.has("elasticity") ? query.getBoolean("elasticity") : false;
        boolean showZones = query.has("showZones") ? query.getBoolean("showZones") : false;
		boolean consolidateGroups = query.has("consolidateGroups") ? query.getBoolean("consolidateGroups") : false;
		List<Operation.Identity.Value> exclude = Operation.exclude(listParams(query, "exclude"), showLent, isCost, forReservation || forSavingsPlans);
		
		// Still support the old name "showResourceGroupTags" for new name showUserTags
        boolean showResourceGroupTags = query.has("showResourceGroupTags") ? query.getBoolean("showResourceGroupTags") : false;
        boolean showUserTags = query.has("showUserTags") ? query.getBoolean("showUserTags") : false;
		showUserTags = showUserTags || showResourceGroupTags;
		
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		int userTagGroupByIndex = 0;
		if (showUserTags) {
			String groupByTag = query.optString("groupByTag");			
			List<UserTagKey> keys = config.userTagKeys;
			for (int i = 0; i < keys.size(); i++) {
				if (groupByTag != null && keys.get(i).name.equals(groupByTag)) {
					userTagGroupByIndex = i;
				}
				userTagLists.add(UserTag.getUserTags(listParams(query, "tag-" + keys[i])));
			}
		}
		
        if (showZones && (zones == null || zones.size() == 0)) {
            zones = Lists.newArrayList(tagGroupManager.getZones(new TagLists(accounts)));
        }
		// Tag Coverage parameters
		boolean tagCoverage = query.has("tagCoverage") ? query.getBoolean("tagCoverage") : false;
		List<UserTag> tagKeys = UserTag.getUserTags(listParams(query, "tagKey"));
		
		if (elasticity) {
			// elasticity is computed per day based on hourly data
			consolidateType = ConsolidateType.hourly;
		}
		else if (tagCoverage) {
			// tagCoverage not aggregated hourly
			consolidateType = consolidateType == ConsolidateType.hourly ? ConsolidateType.daily : consolidateType;
		}
		
		if (consolidateType == ConsolidateType.hourly && !getConfig().hourlyData && !(forReservation || forSavingsPlans))
			consolidateType = ConsolidateType.daily;

        DateTime start;
        if (query.has("spans")) {
            int spans = query.getInt("spans");
            if (consolidateType == ConsolidateType.daily)
                start = end.minusDays(spans);
            else if (consolidateType == ConsolidateType.weekly)
                start = end.minusWeeks(spans);
            else if (consolidateType == ConsolidateType.monthly)
                start = end.minusMonths(spans);
        }
        else
            start = dateFormatter.parseDateTime(query.getString("start"));

        Interval interval = new Interval(start, end);
        Interval overlap_interval = tagGroupManager.getOverlapInterval(interval);
        if (overlap_interval != null) {
            interval = overlap_interval
        }
		interval = truncateInterval(interval, consolidateType);
        interval = roundInterval(interval, consolidateType);
		
        Map<Tag, double[]> data;
		if (tagCoverage) {
			logger.debug("tagCoverage: groupBy=" + groupBy + ", aggregate=" + aggregate + ", tagKeys=" + tagKeys);
			if (showUserTags) {
				if (products.size() == 0) {
					Set productSet = Sets.newTreeSet();
					for (Product product: getManagers().getProducts()) {
						if (product == null)
							continue;
	
						Collection<Product> tmp = getManagers().getTagGroupManager(product).getProducts(new TagLists(accounts, regions, zones));
						productSet.addAll(tmp);
					}
					products = Lists.newArrayList(productSet);
				}
				Map<Tag, TagCoverageMetrics[]> rawMetrics = Maps.newHashMap();
				for (Product product: products) {
					if (product == null)
						continue;
	                TagCoverageDataManager dataManager = (TagCoverageDataManager) getManagers().getTagCoverageManager(product, consolidateType);
					if (dataManager == null) {
						continue;
					}
					TagLists tagLists;
	                Map<Tag, TagCoverageMetrics[]> dataOfProduct = dataManager.getRawData(
	                    interval,
	                    new TagListsWithUserTags(accounts, regions, zones, Lists.newArrayList(product), operations, usageTypes, userTagLists),
	                    groupBy,
                        aggregate,
						userTagGroupByIndex
	                );
	               	logger.debug("  product: " + product + ", tags:" + dataOfProduct.keySet());      
					mergeTagCoverage(dataOfProduct, rawMetrics);
				}
				data = TagCoverageDataManager.processResult(rawMetrics, groupBy, aggregate, tagKeys, config.userTagKeys);
			}
			else {
				TagCoverageDataManager dataManager = (TagCoverageDataManager) getManagers().getTagCoverageManager(null, consolidateType);
				data = dataManager.getData(
					interval,
					new TagLists(accounts, regions, zones, products, operations, usageTypes),
					groupBy,
                    aggregate,
					userTagGroupByIndex,
					tagKeys
				);			
			}
			logger.debug("groupBy: " + groupBy + (groupBy == TagType.Tag ? ":" + config.userTagKeys.get(userTagGroupByIndex) : "") + ", tags = " + data.keySet());
		}
        else if (showUserTags) {
            data = getManagers().getData(
				interval,
				accounts,
				regions,
				zones,
				products,
				operations,
				usageTypes,
				isCost,
				consolidateType,
				groupBy,
				aggregate,
				exclude,
				usageUnit,
				userTagLists,
				userTagGroupByIndex);			
        }
        else {
			logger.debug("doGetData: " + operations + ", forReservation: " + (forReservation || forSavingsPlans));
			
            DataManager dataManager = getManagers().getDataManager(null, consolidateType);
            data = dataManager.getData(
				isCost,
                interval,
                new TagLists(accounts, regions, zones, products, operations, usageTypes),
                groupBy,
                aggregate,
				exclude,
				usageUnit,
				userTagGroupByIndex
            );
		
			logger.debug("  -- tags: " + data.keySet());
        }
		
		if (groupByOrgUnit)
			data = consolidateAccounts(data);
		else if (groupByCostType)
			data = consolidateToCostTypes(data);
		else if (consolidateGroups) {
			if (groupBy == TagType.UsageType)
				data = consolidateFamilies(data);
			else if (groupBy == TagType.Operation)
				data = consolidateOperations(data);
		}
			
		def stats = [:];
		if (elasticity) {
			// consolidate the data to daily
			data = reduceToDailyElasticity(data, stats);
			consolidateType = ConsolidateType.daily;
		}
		else {
	        stats = getStats(data);
		}
		
		if (aggregate == AggregateType.stats && data.size() > 1)
			data.remove(Tag.aggregated);

        def result = [status: 200, start: interval.getStartMillis(), data: data, stats: stats, groupBy: groupBy == null ? "None" : groupBy.name()]
		
		if (!tagCoverage) {
	        if (breakdown && data.size() > 0 && data.values().iterator().next().length > 0) {
	            result.time = new IntRange(0, data.values().iterator().next().length - 1).collect {
	                if (consolidateType == ConsolidateType.daily)
	                    interval.getStart().plusDays(it).getMillis()
	                else if (consolidateType == ConsolidateType.weekly)
	                    interval.getStart().plusWeeks(it).getMillis()
	                else if (consolidateType == ConsolidateType.monthly)
	                    interval.getStart().plusMonths(it).getMillis()
	            }
	            result.hours = new IntRange(0, result.time.size() - 1).collect {
	                int hours;
	                if (consolidateType == ConsolidateType.daily)
	                    hours = 24
	                else if (consolidateType == ConsolidateType.weekly)
	                    hours = 24*7
	                else if (consolidateType == ConsolidateType.monthly)
	                    hours = interval.getStart().plusMonths(it).dayOfMonth().getMaximumValue() * 24;
	
	                if (it == result.time.size() - 1) {
	                    DateTime period = new DateTime(result.time.get(result.time.size() - 1), DateTimeZone.UTC);
	                    DateTime periodEnd = consolidateType == ConsolidateType.daily ? period.plusDays(1) : (consolidateType == ConsolidateType.weekly ? period.plusWeeks(1) : period.plusMonths(1));
	                    DateTime month = period.withMillisOfDay(0).withDayOfMonth(1);
	                    int dataHours;
						if (getConfig().hourlyData) {
							dataHours = getManagers().getDataManager(null, ConsolidateType.hourly).getDataLength(month);
						}
						else {
							int daysInYear = getManagers().getDataManager(null, ConsolidateType.daily).getDataLength(month.withDayOfYear(1));
							dataHours = (daysInYear - month.getDayOfYear()-1) * 24;
						}
	                    DateTime dataEnd = month.plusHours(dataHours);
	
	                    if (dataEnd.isBefore(periodEnd)) {
	                        hours - Hours.hoursBetween(dataEnd, periodEnd).getHours()
	                    }
	                    else {
	                        hours
	                    }
	                }
	                else {
	                    hours
	                }
	
	            }
	
	            result.data = data.sort {-it.getValue()[it.getValue().length-1]}
	        }
	
	        if (showsps || factorsps) {
	            result.sps = config.throughputMetricService.getData(interval, consolidateType);
	        }
	
	        if (factorsps) {
	            double[] consolidatedSps = result.sps;
	            double multiply = config.throughputMetricService.getFactoredCostMultiply();
	            for (Tag tag: result.data.keySet()) {
	                double[] values = result.data.get(tag);
	                for (int i = 0; i < values.length; i++) {
	                    double sps = i < consolidatedSps.length ? consolidatedSps[i] : 0.0;
	                    if (sps == 0.0)
	                        values[i] = 0.0;
	                    else
	                        values[i] = values[i] / sps * multiply;
	                }
	            }
	        }

	        if (isCost && config.currencyRate != 1) {
	            for (Tag tag: result.data.keySet()) {
	                double[] values = result.data.get(tag);
	                for (int i = 0; i < values.length; i++) {
	                    values[i] = values[i] * config.currencyRate;
	                }
	            }
	
	            for (Tag tag: result.stats.keySet()) {
	                Map<String, Double> stat = result.stats.get(tag);
	                for (Map.Entry<String, Double> entry: stat.entrySet()) {
	                    entry.setValue(entry.getValue() * config.currencyRate);
	                }
	            }
	        }
		}

		if (consolidateType != ConsolidateType.monthly) {
            result.interval = consolidateType.millis;
        }
        else {
			if (data.size() > 0) {
            	result.time = new IntRange(0, data.values().iterator().next().length - 1).collect { interval.getStart().plusMonths(it).getMillis() }
			}
        }
		logger.info("doGetData elapsed time: " + sw);
        return result;
    }
	
    private void merge(Map<Tag, double[]> from, Map<Tag, double[]> to) {
        for (Map.Entry<Tag, double[]> entry: from.entrySet()) {
            Tag tag = entry.getKey();
            double[] newValues = entry.getValue();
            if (to.containsKey(tag)) {
                double[] oldValues = to.get(tag);
                for (int i = 0; i < newValues.length; i++) {
                    oldValues[i] += newValues[i];
                }
            }
            else {
                to.put(tag, newValues);
            }
        }
    }
	
    private void mergeTagCoverage(Map<Tag, TagCoverageMetrics[]> from, Map<Tag, TagCoverageMetrics[]> to) {
        for (Map.Entry<Tag, TagCoverageMetrics[]> entry: from.entrySet()) {
            Tag tag = entry.getKey();
            TagCoverageMetrics[] newValues = entry.getValue();
            if (to.containsKey(tag)) {
                TagCoverageMetrics[] oldValues = to.get(tag);
                for (int i = 0; i < newValues.length; i++) {
					if (oldValues[i] == null)
						oldValues[i] = newValues[i];
					else if (newValues[i] != null)
                    	oldValues[i].add(newValues[i]);
                }
            }
            else {
                to.put(tag, newValues);
            }
        }
    }
	
	private Map<Tag, double[]> reduceToDailyElasticity(Map<Tag, double[]> data, Map<Tag, Map> stats) {
		// Run through the hourly data reducing to a daily elasticity value
		Map<Tag, double[]> result = Maps.newTreeMap();
		for (Map.Entry<Tag, double[]> entry: data.entrySet()) {
			Tag tag = entry.getKey();
			def days = entry.getValue().length / 24;
			double[] dailyDataForKey = new double[days];
			def hourIndex = 0;
			def dayIndex = 0;
			double dailyMin = 1000000000; // arbitrary large number
			double dailyMax = 0;
			double avgDailyMin = 0;
			double avgDailyMax = 0;
  
			for (double v: entry.getValue()) {
				if (v < dailyMin) {
					dailyMin = v;
				}
				if (v > dailyMax) {
					dailyMax = v;
				}
				hourIndex++;
				if (hourIndex == 24) {
					double elasticity = 1;
					if (dailyMax > 0)
						elasticity = 1 - dailyMin / dailyMax;
					dailyDataForKey[dayIndex++] = elasticity * 100;
					avgDailyMin += dailyMin;
					avgDailyMax += dailyMax;
					
					// reset accumlators for next day
					dailyMin = 1000000000;
					dailyMax = 0;
					hourIndex = 0;
				}
			}
			avgDailyMin /= days;
			avgDailyMax /= days;
			result.put(tag, dailyDataForKey);
			double elasticity = 1;
			if (avgDailyMax > 0)
				elasticity = 1 - avgDailyMin / avgDailyMax;
			stats[tag] = [avgDailyMin: avgDailyMin, avgDailyMax: avgDailyMax, elasticity: elasticity * 100];
		}
		return result;
	}
	
	private Map<Tag, double[]> consolidateFamilies(Map<Tag, double[]> data) {
		// Run through the data reducing EC2 Linux Instance Types and appropriate RDS Instances to a Family Type
		// Also consolidate CloudFront Edge regions
		Map<Tag, double[]> result = Maps.newTreeMap();
		for (Map.Entry<Tag, double[]> entry: data.entrySet()) {
			if (entry.getKey() == Tag.aggregated) {
				// Don't mess with the aggregated data series.
				result[entry.getKey()] = entry.getValue();
				continue;
			}
			
            Tag familyTag = new FamilyTag(entry.getKey().name);
            double[] values = entry.getValue();
			double[] consolidated = result[familyTag];
			if (consolidated == null) {
				result[familyTag] = values;
			}
			else {
				for (int i = 0; i < consolidated.length; i++)
					consolidated[i] += values[i];
			}
		}
		return result;
	}
	
	private Map<Tag, double[]> consolidateOperations(Map<Tag, double[]> data) {
		// Run through the data reducing Reserved Instance Operation Types to a single category:
		//	Amortization = AmortizedRIs - *
		//  Used RIs = Used RIs - * + Borrowed RIs - *
		//  Unused RIs = Unused RIs - *
		//	Lent RIs = Lent RIs - *
		//  Savings = Savings - *
		Map<Tag, double[]> result = Maps.newTreeMap();
		for (Map.Entry<Tag, double[]> entry: data.entrySet()) {
			if (entry.getKey() == Tag.aggregated) {
				// Don't mess with the aggregated data series.
				result[entry.getKey()] = entry.getValue();
				continue;
			}
			
			Tag riTag = new ConsolidatedOperation(entry.getKey().name);
			double[] values = entry.getValue();
			double[] consolidated = result[riTag];
			if (consolidated == null) {
				result[riTag] = values;
			}
			else {
				for (int i = 0; i < consolidated.length; i++)
					consolidated[i] += values[i];
			}
		}
		return result;
	}
	
	// consolidate accounts down into organization units
	private Map<Tag, double[]> consolidateAccounts(Map<Tag, double[]> data) {
		Map<Tag, double[]> result = Maps.newTreeMap();
		for (Map.Entry<Tag, double[]> entry: data.entrySet()) {
			if (entry.getKey() == Tag.aggregated) {
				// Don't mess with the aggregated data series.
				result[entry.getKey()] = entry.getValue();
				continue;
			}
			
			Tag ou = OrganizationalUnit.get(((Account)entry.getKey()).getParents());
			double[] values = entry.getValue();
			double[] consolidated = result[ou];
			if (consolidated == null) {
				result[ou] = values;
			}
			else {
				for (int i = 0; i < consolidated.length; i++)
					consolidated[i] += values[i];
			}
		}
		return result;
	}
	
	// consolidate operations down to cost types
	private Map<Tag, double[]> consolidateToCostTypes(Map<Tag, double[]> data) {
		// Run through the data reducing Operation Types to their cost types
		Map<Tag, double[]> result = Maps.newTreeMap();
		for (Map.Entry<Tag, double[]> entry: data.entrySet()) {
			if (entry.getKey() == Tag.aggregated) {
				// Don't mess with the aggregated data series.
				result[entry.getKey()] = entry.getValue();
				continue;
			}
			CostType costType = CostType.other;
			Tag t = entry.getKey();
			if (t instanceof Operation) {
				Operation op = (Operation) t;
				costType = CostType.getCostType(op);
			}
			double[] values = entry.getValue();
			double[] consolidated = result[costType];
			if (consolidated == null) {
				result[costType] = values;
			}
			else {
				for (int i = 0; i < consolidated.length; i++)
					consolidated[i] += values[i];
			}
		}
		return result;
	}
		
    private Map<Tag, Map> getStats(Map<Tag, double[]> data) {
        def result = [:];

        for (Map.Entry<Tag, double[]> entry: data.entrySet()) {
            Tag tag = entry.getKey();
            double[] values = entry.getValue();
            double max = 0;
			double min = 0;
            double total = 0;

            if (values.length == 0)
                continue;

			// Set the first min
			min = values[0];
			
            for (double v: values) {
                if (v > max)
                    max = v;
				if (v < min)
					min = v;
                total += v;
            }
            result[tag] = [min: min, max: max, total: total, average: total / values.length];
        }

        return result;
    }

    private List<String> listParams(String name) {
        if (params.containsKey(name)) {
            String value = params.get(name);
            return Lists.newArrayList(value.split(","));
        }
        else {
            return Lists.newArrayList();
        }
    }

    private List<String> listParams(JSONObject params, String name) {
        if (params.has(name)) {
            String value = params.getString(name);
            return Lists.newArrayList(value.split(","));
        }
        else {
            return Lists.newArrayList();
        }
    }
	
	private Interval truncateInterval(Interval interval, ConsolidateType consolidateType) {
		// if end is in current month don't go beyond the time of last data we have.
		DateTime curMonth = new DateTime(DateTimeZone.UTC).withDayOfMonth(1).withMillisOfDay(0);
		
		if (!interval.getEnd().isAfter(curMonth))
			return interval;
		
		// Reader may not have hourly data enabled so handle hourly separate from others
        if (consolidateType == ConsolidateType.hourly) {
			int hoursOfData = getManagers().getDataManager(null, ConsolidateType.hourly).getDataLength(curMonth);
			if (interval.getEnd().getHourOfDay() + (interval.getEnd().getDayOfMonth()-1) * 24 > hoursOfData) {
				interval = new Interval(interval.getStart(), curMonth.plusHours(hoursOfData));
			}
        }
		else {
			DateTime curYear = curMonth.withDayOfYear(1);
			int daysOfData = getManagers().getDataManager(null, ConsolidateType.daily).getDataLength(curYear);
			if (interval.getEnd().getDayOfYear() > daysOfData) {
				DateTime start = interval.getStart();
				DateTime dataEndDay = curMonth.withDayOfYear(1).plusDays(daysOfData);
				interval = new Interval(start, dataEndDay.isBefore(start) ? start : dataEndDay);
			}
		}
		return interval;	
	}

    private Interval roundInterval(Interval interval, ConsolidateType consolidateType) {
        DateTime start = interval.getStart();
        DateTime end = interval.getEnd();

        if (consolidateType == ConsolidateType.daily) {
            start = start.minusHours(1).withHourOfDay(0).plusDays(1);
            //end = end.withHourOfDay(0);
        }
        else if (consolidateType == ConsolidateType.weekly) {
            start = start.withHourOfDay(0).minusDays(1).withDayOfWeek(1).plusWeeks(1);
            //end = end.withHourOfDay(0).withDayOfWeek(1);
        }
        else if (consolidateType == ConsolidateType.monthly) {
            start = start.withHourOfDay(0).minusDays(1).withDayOfMonth(1).plusMonths(1);
            //end = end.withHourOfDay(0).withDayOfMonth(1);
        }
		if (start.isAfter(end)) {
			logger.warn("Start is after end: start=" + start + ", end=" + end);
			end = start;
		}

        return new Interval(start, end);
    }
}
