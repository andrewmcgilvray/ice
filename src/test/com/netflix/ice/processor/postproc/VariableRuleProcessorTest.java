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
package com.netflix.ice.processor.postproc;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.kubernetes.KubernetesReport;
import com.netflix.ice.processor.postproc.TagGroupSpec.DataType;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTagKey;

public class VariableRuleProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
	private static final String resourceDir = "src/test/resources/";
	private static final int testDataHour = 395;
    
    static private ProductService ps;
	static private AccountService as;
	static private String a1 = "111111111111";
	static private String a2 = "222222222222";
	static final String productCode = Product.Code.CloudFront.serviceCode;
	static final String ec2Instance = Product.Code.Ec2Instance.serviceCode;
	static final String ebs = Product.Code.Ebs.serviceCode;
	static final String cloudWatch = Product.Code.CloudWatch.serviceCode;

	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
		ps.getProduct(Product.Code.CloudFront);
		ps.getProduct(Product.Code.CloudWatch);
	}
	    
	private class TestVariableRuleProcessor extends VariableRuleProcessor {
		private AllocationReport ar;
		
		public TestVariableRuleProcessor(Rule rule, CostAndUsageData outCauData, AllocationReport ar, ResourceService rs) {
			super(rule, outCauData, as, ps, rs, null);
			this.ar = ar;
		}
		
		@Override
		protected AllocationReport getAllocationReport(CostAndUsageData data) throws Exception {
			return ar;
		}
	}
	
    private void loadData(TagGroupSpec[] dataSpecs, CostAndUsageData data, int hour) throws Exception {
        for (TagGroupSpec spec: dataSpecs) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	if (spec.dataType == DataType.cost) {
        		ReadWriteData rwd = data.getCost(tg.product);
        		if (rwd == null) {
        			rwd = new ReadWriteData();
        			data.putCost(tg.product, rwd);
        		}
        		rwd.put(hour, tg, spec.value);
        	}
        	else {
        		ReadWriteData rwd = data.getUsage(tg.product);
        		if (rwd == null) {
        			rwd = new ReadWriteData();
        			data.putUsage(tg.product, rwd);
        		}
        		rwd.put(hour, tg, spec.value);
        	}
        }
    }
    	
	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	
	@Test
	public void testAllocationReport() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: allocation-report-test\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  userTags:\n" +
				"    Key2: compute\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 1000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 2000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 4000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 8000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterB", "compute"}, 16000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterB", "compute"}, 32000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "compute"}, 10000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterC", "compute"}, 20000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterC", "compute"}, 40000.0),
        };
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        loadData(dataSpecs, data, 0);
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), rule.config.isReport(), rs.getCustomTags());
		
		// First call with empty report to make sure it handles it
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		// Check that we have our original three EC2Instance records at hour 0
		Map<TagGroup, Double> hourData = data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(0);
		assertEquals("wrong number of output records", 3, hourData.keySet().size());
		// Make sure the original data is unchanged
        for (TagGroupSpec spec: dataSpecs) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, Double> costData = data.getCost(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value, costData.get(tg));
        }
		
		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,_Product,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterA,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		hourData = data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(0);
		assertEquals("wrong number of output records", 5, hourData.keySet().size());
		
        TagGroupSpec[] expected = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 50.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "twenty-five"}, 250.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "seventy"}, 700.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 2000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 4000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 8000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterB", "compute"}, 16000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterB", "compute"}, 32000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "compute"}, 10000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterC", "compute"}, 20000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterC", "compute"}, 40000.0),
        };
        
        for (TagGroupSpec spec: expected) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, Double> costData = data.getCost(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value, costData.get(tg));
        }
        
        // Process a report with duplicated entry
		String allocationYaml2 = "" +
				"name: k8s\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  region: eu-west-1\n" + 
				"  userTags:\n" +
				"    Key2: compute\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    _Product: _Product\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"";
		rule = new Rule(getConfig(allocationYaml2), as, ps, rs.getCustomTags());
		reportData = "" +
				"StartDate,EndDate,Allocation,_Product,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterC,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterC,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterC,seventy\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterC,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		hourData = data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(0);
		assertEquals("wrong number of output records", 7, hourData.keySet().size());
		
        expected = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "compute"}, -9000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "twenty-five"}, 5000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "seventy"}, 14000.0),
        };
        
        for (TagGroupSpec spec: expected) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, Double> costData = data.getCost(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value, costData.get(tg));
        }
        
        
	}

	@Test
	public void testAllocationReportTagMap() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2","Key3","Key4"}, false);
		String allocationYaml = "" +
				"name: k8s\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  userTags:\n" +
				"    Key2: compute\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"  tagMaps:\n" +
				"    Key3:\n" +
				"      maps:\n" +
				"        V25:\n" +
				"          Key2: [25,'re:twenty-.*']\n" +
				"        V70:\n" +
				"          Key2: [seventy]\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute", "", ""}, 1000.0),
        };
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        loadData(dataSpecs, data, 0);
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), rule.config.isReport(), rs.getCustomTags());
		
		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,_Product,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterA,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		assertEquals("wrong number of output records", 3, data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());
		
        TagGroupSpec[] expected = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute", "", ""}, 50.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "twenty-five", "V25", ""}, 250.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "seventy", "V70", ""}, 700.0),
         };
        
        for (TagGroupSpec spec: expected) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, Double> costData = data.getCost(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value, costData.get(tg));
        }
	}
	
	@Test
	public void testMonthlyReportByAccount() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"reports: [monthly]\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  filter:\n" + 
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [account]\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 200.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 400.0),
        };
		CostAndUsageData data = new CostAndUsageData(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, null, null, as, ps);
        loadData(dataSpecs, data, 0);
		PostProcessor pp = new PostProcessor(null, as, ps, rs, null);
		pp.debug = true;
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());
		
		CostAndUsageData outData = new CostAndUsageData(data, null);
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, null, rs);
		vrp.process(data);
		
		// Make sure source data wasn't changed
		assertEquals("wrong number of output records", 1, data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());
    	TagGroup tg = dataSpecs[0].getTagGroup(as, ps);
    	Map<TagGroup, Double> costData = data.getCost(tg.product).getData(0);
    	assertEquals("wrong data for spec " + dataSpecs[0].toString(), dataSpecs[0].value, costData.get(tg));
	}

	@Test
	public void testMonthlyReportWithAllocationReport() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"reports: [monthly]\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  filter:\n" + 
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [account]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 200.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 400.0),
        };
		CostAndUsageData data = new CostAndUsageData(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, null, null, as, ps);
        loadData(dataSpecs, data, 0);
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		List<String> userTagKeys = Lists.newArrayList(new String[]{"Key1","Key2"});
		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), rule.config.isReport(), userTagKeys);
		
		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,clusterA,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		
		CostAndUsageData outData = new CostAndUsageData(data, rs.getUserTagKeys());
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, ar, rs);
		vrp.process(data);
		
		// Make sure source data wasn't changed
		assertEquals("wrong number of input records", 1, data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());
    	TagGroup tg = dataSpecs[0].getTagGroup(as, ps);
    	Map<TagGroup, Double> costData = data.getCost(tg.product).getData(0);
    	assertEquals("wrong data for spec " + dataSpecs[0].toString(), dataSpecs[0].value, costData.get(tg));
		
    	Account a = as.getAccountById(a1);
        TagGroup[] expectedTg = new TagGroup[]{
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "compute"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "twenty-five"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "seventy"})),
         };
        Double[] expectedValues = new Double[]{ 5.0 + 10.0 + 20.0, 25.0 + 50.0 + 100.0, 70.0 + 140.0 + 280.0 };
        
        assertEquals("wrong number of output records", expectedTg.length, outData.getCost(null).getData(0).size());
        for (int i = 0; i < expectedTg.length; i++) {
        	tg = expectedTg[i];
        	costData = outData.getCost(null).getData(0);
        	assertEquals("wrong data for spec " + tg, expectedValues[i], costData.get(tg), 0.001);
        }
        
        Query in = rule.getIn();
		ReadWriteData rwData = outData.getCost(null);
        
		TestReportWriter writer = new TestReportWriter("test-report", data.getStart(), RuleConfig.DataType.cost, in.getGroupBy(), outData.getUserTagKeysAsStrings(), rwData);		
		writer.archive();
		
		String csv = writer.baos.toString();
		
		String expectedHeader = "Date,Cost,Account ID,Account Name,Key1,Key2";
		String[] expectedRows = new String[]{
			"2020-08-01T00:00:00Z,175.0,1111111111111,1111111111111,clusterA,twenty-five",
			"2020-08-01T00:00:00Z,35.0,1111111111111,1111111111111,clusterA,compute",
			"2020-08-01T00:00:00Z,490.0,1111111111111,1111111111111,clusterA,seventy",
		};
		
		checkReport(csv, 4, expectedHeader, expectedRows);
	}
	
	@Test
	public void testMonthlyReportWithAllocationReportAndEmptyInKeys() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"reports: [monthly]\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  filter:\n" + 
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [account]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    _account: 'Account ID'\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 1000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"",         "compute"}, 10000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 100000.0),
        };
		CostAndUsageData data = new CostAndUsageData(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, null, null, as, ps);
        loadData(dataSpecs, data, 0);
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		List<String> userTagKeys = Lists.newArrayList(new String[]{"Key1","Key2"});
		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), rule.config.isReport(), userTagKeys);
		
		// Process with a report that has both empty and non-empty Key1 values.
		// Should apply the allocations for specific non-empty values and then use
		// the empty allocation for all remaining values (including empty values)
		String reportData = "" +
				"StartDate,EndDate,Allocation,Account ID,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25," + a1 + ",clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70," + a1 + ",,seventy\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,1.0," + a2 + ",clusterB,one-hundred\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		
		CostAndUsageData outData = new CostAndUsageData(data, rs.getUserTagKeys());
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, ar, rs);
		vrp.process(data);
				
    	Account a = as.getAccountById(a1);
    	Account act2 = as.getAccountById(a2);
        TagGroup[] expectedTg = new TagGroup[]{
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "compute"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "twenty-five"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterB", "compute"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterB", "seventy"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"",         "compute"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"",         "seventy"})),
        		TagGroup.getTagGroup(act2, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterB", "one-hundred"})),
         };
        Double[] expectedValues = new Double[]{ 75.0, 25.0, 300.0, 700.0, 3000.0, 7000.0, 100000.0 };
        
        assertEquals("wrong number of output records", expectedTg.length, outData.getCost(null).getData(0).size());
        for (int i = 0; i < expectedTg.length; i++) {
        	TagGroup tg = expectedTg[i];
        	Map<TagGroup, Double> costData = outData.getCost(null).getData(0);
        	assertEquals("wrong data for spec " + tg, expectedValues[i], costData.get(tg), 0.001);
        }
        
        Query in = rule.getIn();
		ReadWriteData rwData = outData.getCost(null);
        
		TestReportWriter writer = new TestReportWriter("test-report", data.getStart(), RuleConfig.DataType.cost, in.getGroupBy(), outData.getUserTagKeysAsStrings(), rwData);		
		writer.archive();
		
		String csv = writer.baos.toString();
		
		String expectedHeader = "Date,Cost,Account ID,Account Name,Key1,Key2";
		String[] expectedRows = new String[]{
			"2020-08-01T00:00:00Z,100000.0,222222222222,222222222222,clusterB,one-hundred",
			"2020-08-01T00:00:00Z,25.0,111111111111,111111111111,clusterA,twenty-five",
			"2020-08-01T00:00:00Z,300.0,111111111111,111111111111,clusterB,compute",
			"2020-08-01T00:00:00Z,3000.0,111111111111,111111111111,,compute",
			"2020-08-01T00:00:00Z,700.0,111111111111,111111111111,clusterB,seventy",
			"2020-08-01T00:00:00Z,7000.0,111111111111,111111111111,,seventy",
			"2020-08-01T00:00:00Z,75.0,111111111111,111111111111,clusterA,compute",
		};
		
		checkReport(csv, 8, expectedHeader, expectedRows);
	}
	
	// Test report that has one or more output dimensions not in the source custom tags list
	@Test
	public void testMonthlyReportWithAllocationReportAndExtraTags() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"reports: [monthly]\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  filter:\n" + 
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [account]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"    Extra: Extra\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0),
        };
		CostAndUsageData data = new CostAndUsageData(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, null, null, as, ps);
        loadData(dataSpecs, data, 0);
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		List<String> reportUserTagKeys = Lists.newArrayList(new String[]{"Extra", "Key1", "Key2"});
		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), rule.config.isReport(), reportUserTagKeys);
		
		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,Key1,Key2,Extra\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,clusterA,twenty-five,extraOne\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,clusterA,seventy,extraTwo\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		
		CostAndUsageData outData = new CostAndUsageData(data, UserTagKey.getUserTagKeys(reportUserTagKeys));
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, ar, rs);
		vrp.process(data);
		
		// Make sure source data wasn't changed
		assertEquals("wrong number of output records", 1, data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());
    	TagGroup tg = dataSpecs[0].getTagGroup(as, ps);
    	Map<TagGroup, Double> costData = data.getCost(tg.product).getData(0);
    	assertEquals("wrong data for spec " + dataSpecs[0].toString(), dataSpecs[0].value, costData.get(tg));
		
    	Account a = as.getAccountById(a1);
        TagGroup[] expectedTg = new TagGroup[]{
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"", "clusterA", "compute"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extraOne", "clusterA", "twenty-five"})),
        		TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extraTwo", "clusterA", "seventy"})),
         };
        Double[] expectedValues = new Double[]{ 5.0, 25.0, 70.0 };
        
        for (int i = 0; i < expectedTg.length; i++) {
        	tg = expectedTg[i];
        	costData = outData.getCost(null).getData(0);
        	assertEquals("wrong data for spec " + tg, expectedValues[i], costData.get(tg));
        }
        
        Query in = rule.getIn();
		ReadWriteData rwData = outData.getCost(null);
        
		TestReportWriter writer = new TestReportWriter("report-test", data.getStart(), RuleConfig.DataType.cost, in.getGroupBy(), outData.getUserTagKeysAsStrings(), rwData);		
		writer.archive();
		
		String csv = writer.baos.toString();
		
		String expectedHeader = "Date,Cost,Account ID,Account Name,Extra,Key1,Key2";
		String[] expectedRows = new String[]{
			"2020-08-01T00:00:00Z,25.0,1111111111111,1111111111111,extraOne,clusterA,twenty-five",
			"2020-08-01T00:00:00Z,5.0,1111111111111,1111111111111,,clusterA,compute",
			"2020-08-01T00:00:00Z,70.0,1111111111111,1111111111111,extraTwo,clusterA,seventy",
		};
		
		checkReport(csv, 4, expectedHeader, expectedRows);
	}
	
	private void checkReport(String csv, int expectedNumLines, String expectedHeader, String[] eRows) {
		String[] rows = csv.split("\r\n");
		assertEquals("wrong number of lines", expectedNumLines, rows.length);
		
		List<String> rowsSorted = Lists.newArrayList();
		for (int i = 1; i < rows.length; i++)
			rowsSorted.add(rows[i]);
		Collections.sort(rowsSorted);

		List<String> expectedRows = Lists.newArrayList(eRows);
		
		int numCols = expectedHeader.split(",").length;
				
		// Check header
		assertEquals("wrong header", expectedHeader, rows[0]);
		for (int i = 0; i < rowsSorted.size(); i++) {
			String row = rowsSorted.get(i);
			String expected = expectedRows.get(i);
			assertEquals("wrong number of columns on row" + Integer.toString(i + 1), numCols, row.split(",").length);
			
			// Check for match on all but allocation number
			String[] cols = row.split(",");
			String[] eCols = expected.split(",");
			Double v = Double.parseDouble(cols[1]);
			cols[1] = "";
			Double e = Double.parseDouble(eCols[1]);
			eCols[1] = "";
			assertArrayEquals("wrong results for line " + Integer.toString(i), eCols, cols);
			assertEquals("wrong value for line " + Integer.toString(i), e, v, 0.0001);
		}
	}
	
	class TestReportWriter extends ReportWriter {
		public ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		public TestReportWriter(String name, DateTime month, RuleConfig.DataType type,
				List<Rule.TagKey> tagKeys, List<String> userTagKeys,
				ReadWriteData data) throws Exception {
			super(name, null, month, type, tagKeys, userTagKeys, data);
		}
		
		@Override
	    public void open() throws IOException {
	    	os = baos;
	    }
		
		@Override
	    public void close() throws IOException {
	    }
	}

	class TestKubernetesReport extends KubernetesReport {

		public TestKubernetesReport(AllocationConfig config, ResourceService resourceService) throws Exception {
			super(config, new DateTime("2019-01", DateTimeZone.UTC), resourceService);

			File file = new File(resourceDir, "kubernetes-2019-01.csv");
			readFile(file);			
		}
	}
	
	@Test
	public void testGenerateAllocationReport() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Cluster","Role","K8sNamespace","Environment","K8sType","K8sResource","UserTag1","UserTag2"}, false);
		String allocationYaml = "" +
				"name: k8s\n" + 
				"start: 2019-01\n" + 
				"end: 2022-11\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  userTags:\n" +
				"    Role: compute\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"    region: us-east-1\n" +
				"    accountId: 123456789012\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"    Cluster: Cluster\n" +
				"    Environment: inEnvironment\n" +
				"  out:\n" +
				"    K8sNamespace: K8sNamespace\n" +
				"    Environment: outEnvironment\n" +
				"  kubernetes:\n" +
				"    clusterNameFormulae: [Cluster]\n" +
				"    out:\n" +
				"      Namespace: K8sNamespace\n" +
				"";
		
		// Test the data for cluster "dev-usw2a"
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-west-2", "us-west-2a", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{"dev-usw2a", "compute", "", "Dev", "", "", "", ""}, 40.0),
        };
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        loadData(dataSpecs, data, testDataHour);

        RuleConfig rc = getConfig(allocationYaml);
		Rule rule = new Rule(rc, as, ps, rs.getCustomTags());
		KubernetesReport kr = new TestKubernetesReport(rc.getAllocation(), rs);
		Set<String> unprocessedClusters = Sets.newHashSet(kr.getClusters());
		Set<String> unprocessedAtgs = Sets.newHashSet();
		
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, null, rs);
		AllocationReport ar = vrp.generateAllocationReport(kr, data, unprocessedClusters, unprocessedAtgs);
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
				
		assertEquals("wrong number of hours", testDataHour+1, ar.getNumHours());
		assertEquals("wrong number of keys", 1, ar.getKeySet(testDataHour).size());
		
		AllocationReport.Key key = ar.getKeySet(testDataHour).iterator().next();
		List<AllocationReport.Value> values = ar.getData(testDataHour, key);
		assertEquals("wrong number of allocation items", 11, values.size());
		
		for (AllocationReport.Value v: values) {
			assertFalse("Empty namespace tag", v.getOutput("K8sNamespace").isEmpty());
		}
		
		StringWriter writer = new StringWriter();
		
		ar.writeCsv(kr.getMonth(), writer);
		String[] records = writer.toString().split("\r\n");
		List<String> expected = Lists.newArrayList(new String[]{"StartDate","EndDate","Allocation","_Product","Cluster","inEnvironment","K8sNamespace","outEnvironment"});
		Collections.sort(expected);
		List<String> got = Lists.newArrayList(records[0].split(","));
		Collections.sort(got);
		//logger.info("csv:\n" + writer.toString());
		assertArrayEquals("bad header", expected.toArray(new String[]{}), got.toArray(new String[]{}));
		assertEquals("wrong number of data fields", expected.size(), records[1].split(",").length);
	}
		
	@Test
	public void testProcessKubernetesReport() throws Exception {		
		// Test with three formulae and make sure we only process each cluster once.
		// The cluster names should match the forumlae as follows:
		//  "dev-usw2a" --> formula 1
		//  "k8s-dev-usw2a" --> formula 2
		//  "k8s-usw2a --> formula 3
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Cluster","Role","K8sNamespace","Environment","K8sType","K8sResource","UserTag1","UserTag2"}, false);
		String[] formulae = new String[]{
				"Cluster",											// 1. use the cluster name directly
				"Cluster.regex(\"k8s-(.*)\")",						// 2. Strip off the leading "k8s-"
				"Environment.toLower()+Cluster.regex(\"k8s(-.*)\")", // 3. Get the environment string and join with suffix of cluster
				"Cluster", 											// 4. repeat of formula 1 to verify we don't process cluster twice
			};
		String allocationYaml = "" +
				"name: k8s\n" + 
				"start: 2019-01\n" + 
				"end: 2022-11\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  filter:\n" + 
				"    account: [" + a1 + "]\n" + 
				"    userTags:\n" +
				"      Role: [compute]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"    region: us-east-1\n" +
				"    accountId: 123456789012\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"    Cluster: Cluster\n" +
				"    Environment: Environment\n" +
				"  out:\n" +
				"    K8sNamespace: K8sNamespace\n" +
				"    K8sType: K8sType\n" +
				"    K8sResource: K8sResource\n" +
				"  kubernetes:\n" +
				"    clusterNameFormulae: [" + String.join(",", formulae) + "]\n" +
				"    out:\n" +
				"      Namespace: K8sNamespace\n" +
				"      Type: K8sType\n" +
				"      Resource: K8sResource\n" +
				"";
		
		String[] clusterTags = new String[]{ "dev-usw2b", "k8s-prod-usw2a", "k8s-usw2a" };
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-west-2", "us-west-2b", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{clusterTags[0], "compute", "", "Dev", "", "", "", ""}, 40.0),
        		new TagGroupSpec(DataType.cost, a1, "us-west-2", "us-west-2a", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{clusterTags[1], "compute", "", "Dev", "", "", "", ""}, 40.0),
        		new TagGroupSpec(DataType.cost, a1, "us-west-2", "us-west-2a", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{clusterTags[2], "compute", "", "Dev", "", "", "", ""}, 40.0),
        };
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        loadData(dataSpecs, data, testDataHour);
										
        RuleConfig rc = getConfig(allocationYaml);
		Rule rule = new Rule(rc, as, ps, rs.getCustomTags());
		KubernetesReport kr = new TestKubernetesReport(rc.getAllocation(), rs);
		Set<String> unprocessedClusters = Sets.newHashSet(kr.getClusters());
		Set<String> unprocessedAtgs = Sets.newHashSet();
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, null, rs);
		AllocationReport ar = vrp.generateAllocationReport(kr, data, unprocessedClusters, unprocessedAtgs);
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		
		assertEquals("have wrong number of unprocessed clusters", 1, unprocessedClusters.size());
		assertTrue("wrong unprocessed cluster", unprocessedClusters.contains("stage-usw2a"));
		assertEquals("have unprocessed ATGs", 0, unprocessedAtgs.size());
		
		double[] expectedAllocatedCosts = new double[]{ 0.3934, 0.4324, 0.4133 };
		double[] expectedUnusedCosts = new double[]{ 11.4360, 11.9054, 20.9427 };
		int [] expectedAllocationCounts = new int[]{ 10, 8, 11};
		Map<TagGroup, Double> hourCostData = data.getCost(ps.getProduct(Product.Code.Ec2Instance)).getData(testDataHour);
		
		for (int i = 0; i < clusterTags.length; i++) {
			String clusterTag = clusterTags[i];
			TagGroup tg = dataSpecs[i].getTagGroup(as, ps);
						
			String[] atags = new String[]{ clusterTags[i], "compute", "kube-system", "Dev", "", "", "", "" };
			ResourceGroup arg = ResourceGroup.getResourceGroup(atags);
			TagGroup atg = tg.withResourceGroup(arg);
			
			Double allocatedCost = hourCostData.get(atg);
			assertNotNull("No allocated cost for kube-system namespace with cluster tag " + clusterTag, allocatedCost);
			assertEquals("Incorrect allocated cost with cluster tag " + clusterTag, expectedAllocatedCosts[i], allocatedCost, 0.0001);
			String[] unusedTags = new String[]{ clusterTag, "compute", "unused", "Dev", "unused", "unused", "", "" };
			TagGroup unusedTg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, ResourceGroup.getResourceGroup(unusedTags));
			Double unusedCost = hourCostData.get(unusedTg);
			assertEquals("Incorrect unused cost with cluster tag " + clusterTag, expectedUnusedCosts[i], unusedCost, 0.0001);
			
			int count = 0;
			for (TagGroup tg1: hourCostData.keySet()) {
				if (tg1.resourceGroup.getUserTags()[0].name.equals(clusterTags[i]))
					count++;
			}
			assertEquals("Incorrect number of cost entries for " + clusterTag, expectedAllocationCounts[i], count);
		}
				
		// Add up all the cost values to see if we get back to 120.0 (Three tagGroups with 40.0 each)
		double total = 0.0;
		for (double v: hourCostData.values())
			total += v;
		assertEquals("Incorrect total cost when adding unused and allocated values", 120.0, total, 0.001);		
	}
}
