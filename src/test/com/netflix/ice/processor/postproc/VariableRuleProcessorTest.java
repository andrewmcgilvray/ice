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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.ice.processor.ProcessorConfig;
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
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.processor.kubernetes.KubernetesReport;
import com.netflix.ice.processor.postproc.AllocationReport.Key;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTagKey;

public class VariableRuleProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	private static final String resourceDir = "src/test/resources/";

    static private ProductService ps;
	static private AccountService as;
	static private String a1 = "111111111111";
	static private String a2 = "222222222222";
	static private String a3 = "333333333333";
	static final String productCode = Product.Code.CloudFront.serviceCode;
	static final String ec2Instance = Product.Code.Ec2Instance.serviceCode;
	static final String ebs = Product.Code.Ebs.serviceCode;
	static final String cloudWatch = Product.Code.CloudWatch.serviceCode;
	static final String dataTransfer = Product.Code.DataTransfer.serviceCode;

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
			super(rule, outCauData, as, ps, rs, null, null);
			this.ar = ar;
		}

		@Override
		protected AllocationReport getAllocationReport(CostAndUsageData data) throws Exception {
			return ar;
		}
	}

    private void loadData(TagGroupSpec[] dataSpecs, CostAndUsageData data, int hour, int numTagKeys) throws Exception {
    	data.enableTagGroupCache(true);
        for (TagGroupSpec spec: dataSpecs) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	DataSerializer ds = data.get(tg.product);
        	if (ds == null) {
        		ds = new DataSerializer(numTagKeys);
        		data.put(tg.product,  ds);
        	}
        	ds.put(hour,  tg,  spec.value);
        }
    }

	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
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
				"  filter:\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
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
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 1000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 2000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 4000.0, 0),

        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 8000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterB", "compute"}, 16000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterB", "compute"}, 32000.0, 0),

        		new TagGroupSpec("Recurring", a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "compute"}, 10000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterC", "compute"}, 20000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterC", "compute"}, 40000.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
        loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), rs.getCustomTags(), rs);

		// First call with empty report to make sure it handles it
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		// Check that we have our original three EC2Instance records at hour 0
		Map<TagGroup, CostAndUsage> hourData = data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0);
		assertEquals("wrong number of output records", 3, hourData.keySet().size());
		// Make sure the original data is unchanged
        for (TagGroupSpec spec: dataSpecs) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value, costData.get(tg));
        }

		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,_Product,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterA,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertFalse("report parser flagged an error", ar.isParsingError());
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		vrp.process(data);
		hourData = data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0);
		assertEquals("wrong number of output records", 5, hourData.keySet().size());

        TagGroupSpec[] expected = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 50.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "twenty-five"}, 250.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "seventy"}, 700.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 2000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 4000.0, 0),

        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 8000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterB", "compute"}, 16000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterB", "compute"}, 32000.0, 0),

        		new TagGroupSpec("Recurring", a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "compute"}, 10000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterC", "compute"}, 20000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterC", "compute"}, 40000.0, 0),
        };

        for (TagGroupSpec spec: expected) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, CostAndUsage> dataMap = data.get(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value.cost, dataMap.get(tg).cost, 0.001);
        }

        // Process a report with duplicated entry
		String allocationYaml2 = "" +
				"name: k8s\n" +
				"start: 2019-11\n" +
				"end: 2022-11\n" +
				"in:\n" +
				"  filter:\n" +
				"    region: [eu-west-1]\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"";
		rule = new Rule(getConfig(allocationYaml2), as, ps, rs.getCustomTags());
		ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), rs.getCustomTags(), rs);
		reportData = "" +
				"StartDate,EndDate,Allocation,_Product,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterC,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterC,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterC,seventy\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterC,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertFalse("report parser flagged an error", ar.isParsingError());
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		vrp.process(data);
		hourData = data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0);
		assertEquals("wrong number of output records", 5, hourData.keySet().size());

        expected = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "compute"}, -9000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "twenty-five"}, 5000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "seventy"}, 14000.0, 0),
        };

        for (TagGroupSpec spec: expected) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, CostAndUsage> dataMap = data.get(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value.cost, dataMap.get(tg).cost, 0.001);
        }

        // Process a report with no input keys
		String allocationYaml3 = "" +
				"name: k8s\n" +
				"start: 2019-11\n" +
				"end: 2022-11\n" +
				"in:\n" +
				"  filter:\n" +
				"    region: [us-east-1]\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"";
		rule = new Rule(getConfig(allocationYaml3), as, ps, rs.getCustomTags());
		ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), rs.getCustomTags(), rs);
		reportData = "" +
				"StartDate,EndDate,Allocation,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertFalse("report parser flagged an error", ar.isParsingError());
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
        loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
        vrp.process(data);
		hourData = data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0);
		assertEquals("wrong number of output records", 7, hourData.keySet().size());

        expected = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 1000.0 * .05, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "twenty-five"}, 1000.0 * .25, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "seventy"}, 1000.0 * .7, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 2000.0 * .05, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "twenty-five"}, 2000.0 * .25, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "seventy"}, 2000.0 * .7, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 4000.0 * .05, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "twenty-five"}, 4000.0 * .25, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "seventy"}, 4000.0 * .7, 0),

        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 8000.0 * .05, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "twenty-five"}, 8000.0 * .25, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "seventy"}, 8000.0 * .7, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterB", "compute"}, 16000.0 * .05, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterB", "twenty-five"}, 16000.0 * .25, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterB", "seventy"}, 16000.0 * .7, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterB", "compute"}, 32000.0 * .05, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterB", "twenty-five"}, 32000.0 * .25, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterB", "seventy"}, 32000.0 * .7, 0),

        		new TagGroupSpec("Recurring", a1, "eu-west-1", ec2Instance, "RunInstances", "EUW2:r5.xlarge", new String[]{"clusterC", "compute"}, 10000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterC", "compute"}, 20000.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterC", "compute"}, 40000.0, 0),
        };

        for (TagGroupSpec spec: expected) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value.cost, costData.get(tg).cost, 0.0001);
        }
	}

	@Test
	public void testAllocationReportWithEmptyFields() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2","Key3","Key4"}, false);
		String allocationYaml = "" +
				"name: simple\n" +
				"start: 2019-11\n" +
				"end: 2022-11\n" +
				"in:\n" +
				"  filter:\n" +
				"    account: [" + a1 + "]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  out:\n" +
				"    Key1: Key1\n";

		TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
				new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"", "", "", ""}, 100.0, 0),
		};
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		loadData(dataSpecs, data, 1, rs.getUserTagKeys().size());
		loadData(dataSpecs, data, 2, rs.getUserTagKeys().size());
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), rs.getCustomTags(), rs);

		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,Key1\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,seventy\n" +
				"2020-08-01T01:00:00Z,2020-08-01T02:00:00Z,,twenty-five\n" +
				"2020-08-01T01:00:00Z,2020-08-01T02:00:00Z,,seventy\n" +
				"2020-08-01T02:00:00Z,2020-08-01T03:00:00Z,0.25,twenty-five\n" +
				"2020-08-01T02:00:00Z,2020-08-01T03:00:00Z,0.70,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertTrue("report parser should have flagged an error", ar.isParsingError());

		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		assertEquals("wrong number of output records", 3, data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());

		TagGroupSpec[] expected = new TagGroupSpec[]{
				new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"", "", "", ""}, 5.0, 0),
				new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"twenty-five", "", "", ""}, 25.0, 0),
				new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"seventy", "", "", ""}, 70.0, 0),
		};

		for (int i = 0; i < 3; i++) {
			if (i == 1) {
				TagGroup tg = expected[0].getTagGroup(as, ps);

				Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(i);
				assertEquals("wrong data for unallocated hour " + i, 100, costData.get(tg).cost, 0.001);
			}
			else {
				for (TagGroupSpec spec: expected) {
					TagGroup tg = spec.getTagGroup(as, ps);
					Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(i);
					assertEquals("wrong data for spec " + spec.toString() + " in hour " + i, spec.value.cost, costData.get(tg).cost, 0.001);
				}
			}
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
				"  filter:\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
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
				"    - maps:\n" +
				"        V25:\n" +
				"          key: Key2\n" +
				"          operator: isOneOf\n" +
				"          values: [25,'twenty-.*']\n" +
				"        V70:\n" +
				"          key: Key2\n" +
				"          operator: isOneOf\n" +
				"          values: [seventy]\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute", "", ""}, 1000.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
        loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), rs.getCustomTags(), rs);

		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,_Product,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,EC2Instance,clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,EC2Instance,clusterA,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertFalse("report parser flagged an error", ar.isParsingError());
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);
		assertEquals("wrong number of output records", 3, data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());

        TagGroupSpec[] expected = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute", "", ""}, 50.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "twenty-five", "V25", ""}, 250.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "seventy", "V70", ""}, 700.0, 0),
         };

        for (TagGroupSpec spec: expected) {
        	TagGroup tg = spec.getTagGroup(as, ps);
        	Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(0);
        	assertEquals("wrong data for spec " + spec.toString(), spec.value.cost, costData.get(tg).cost, 0.001);
        }
	}

	@Test
	public void testMonthlyReportByAccount() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" +
				"start: 2019-11\n" +
				"end: 2022-11\n" +
				"report:\n" +
				"  aggregate: [monthly]\n" +
				"in:\n" +
				"  filter:\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [account]\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 200.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 400.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, rs.getUserTagKeys(), null, as, ps);
        loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		List<ProcessorConfig.JsonFileType> jsonFiles = Lists.newArrayList();
		PostProcessor pp = new PostProcessor(null, null, "", as, ps, rs, null, jsonFiles,0);
		pp.debug = true;
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		CostAndUsageData outData = new CostAndUsageData(data, null, rs.getUserTagKeys());
		outData.enableTagGroupCache(true);
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, null, rs);
		vrp.process(data);

		// Make sure source data wasn't changed
		assertEquals("wrong number of output records", 1, data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());
    	TagGroup tg = dataSpecs[0].getTagGroup(as, ps);
    	Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(0);
    	assertEquals("wrong data for spec " + dataSpecs[0].toString(), dataSpecs[0].value.cost, costData.get(tg).cost, 0.001);
	}

	@Test
	public void testMonthlyReportWithAllocationReport() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" +
				"start: 2019-11\n" +
				"end: 2022-11\n" +
				"report:\n" +
				"  aggregate: [monthly]\n" +
				"in:\n" +
				"  filter:\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [costType,account]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"clusterA", "compute"}, 200.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"clusterA", "compute"}, 400.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, rs.getUserTagKeys(), null, as, ps);
        loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		List<String> userTagKeys = Lists.newArrayList(new String[]{"Key1","Key2"});
		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), userTagKeys, rs);

		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70,clusterA,seventy\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertFalse("report parser flagged an error", ar.isParsingError());

		CostAndUsageData outData = new CostAndUsageData(data, null, rs.getUserTagKeys());
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, ar, rs);
		vrp.process(data);

		// Make sure source data wasn't changed
		assertEquals("wrong number of input records", 1, data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());
    	TagGroup tg = dataSpecs[0].getTagGroup(as, ps);
    	Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(0);
    	assertEquals("wrong data for spec " + dataSpecs[0].toString(), dataSpecs[0].value.cost, costData.get(tg).cost, 0.001);

    	Account a = as.getAccountById(a1);
        TagGroup[] expectedTg = new TagGroup[]{
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "compute"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "twenty-five"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "seventy"})),
         };
        Double[] expectedValues = new Double[]{ 5.0 + 10.0 + 20.0, 25.0 + 50.0 + 100.0, 70.0 + 140.0 + 280.0 };

        assertEquals("wrong number of output records", expectedTg.length, outData.get(null).getData(0).size());
        for (int i = 0; i < expectedTg.length; i++) {
        	tg = expectedTg[i];
        	costData = outData.get(null).getData(0);
        	assertEquals("wrong data for spec " + tg, expectedValues[i], costData.get(tg).cost, 0.001);
        }
	}

	@Test
	public void testMonthlyReportWithAllocationReportAndEmptyInKeys() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" +
				"start: 2019-11\n" +
				"end: 2022-11\n" +
				"report:\n" +
				"  aggregate: [monthly]\n" +
				"in:\n" +
				"  filter:\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [costType,account]\n" +
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
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 1000.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"",         "compute"}, 10000.0, 0),
        		new TagGroupSpec("Recurring", a2, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterB", "compute"}, 100000.0, 0),
        		new TagGroupSpec("Recurring", a3, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"",         "compute"}, 1000000.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, rs.getUserTagKeys(), null, as, ps);
        loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		List<String> userTagKeys = Lists.newArrayList(new String[]{"Key1","Key2"});
		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), userTagKeys, rs);

		// Process with a report that has both empty and non-empty Key1 values.
		// Should apply the allocations for specific non-empty values and then use
		// the empty allocation for all remaining values (including empty values)
		String reportData = "" +
				"StartDate,EndDate,Allocation,Account ID,Key1,Key2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25," + a1 + ",clusterA,twenty-five\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.70," + a1 + ",,seventy\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,1.0," + a2 + ",clusterB,one-hundred\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,1.0,,,one-hundred-no-account\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertFalse("report parser flagged an error", ar.isParsingError());

		CostAndUsageData outData = new CostAndUsageData(data, null, rs.getUserTagKeys());
		outData.enableTagGroupCache(true);
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, ar, rs);
		vrp.process(data);

    	Account a = as.getAccountById(a1);
    	Account act2 = as.getAccountById(a2);
    	Account act3 = as.getAccountById(a3);
        TagGroup[] expectedTg = new TagGroup[]{
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "compute"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterA", "twenty-five"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterB", "compute"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterB", "seventy"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"",         "compute"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"",         "seventy"})),
        		TagGroup.getTagGroup(CostType.recurring, act2, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"clusterB", "one-hundred"})),
        		TagGroup.getTagGroup(CostType.recurring, act3, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"", "one-hundred-no-account"})),
         };
        Double[] expectedValues = new Double[]{ 75.0, 25.0, 300.0, 700.0, 3000.0, 7000.0, 100000.0, 1000000.0 };

        Map<TagGroup, CostAndUsage> outMap = outData.get(null).getData(0);
        assertEquals("wrong number of output records", expectedTg.length, outMap.size());
        for (int i = 0; i < expectedTg.length; i++) {
        	TagGroup tg = expectedTg[i];
        	assertTrue("Tag group not in output cost data: " + tg, outMap.containsKey(tg));
        	assertEquals("wrong data for spec " + tg, expectedValues[i], outMap.get(tg).cost, 0.001);
        }
	}

	// Test report that has one or more output dimensions not in the source custom tags list
	@Test
	public void testMonthlyReportWithAllocationReportAndExtraTags() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" +
				"start: 2019-11\n" +
				"end: 2022-11\n" +
				"report:\n" +
				"  aggregate: [monthly]\n" +
				"in:\n" +
				"  filter:\n" +
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [costType,account]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"  in:\n" +
				"    Key1: Key1\n" +
				"  out:\n" +
				"    Key2: Key2\n" +
				"    Extra1: Extra1\n" +
				"    Extra2: Extra2\n" +
				"";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute"}, 100.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC).getMillis(), null, rs.getUserTagKeys(), null, as, ps);
        loadData(dataSpecs, data, 0, rs.getUserTagKeys().size());
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

		List<String> reportUserTagKeys = Lists.newArrayList(new String[]{"Extra1", "Key1", "Key2", "Extra2"});
		AllocationReport ar = new AllocationReport(rule.config.getAllocation(), 0, rule.config.isReport(), reportUserTagKeys, rs);

		// Process with a report
		String reportData = "" +
				"StartDate,EndDate,Allocation,Key1,Key2,Extra1,Extra2\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,clusterA,twenty-five,extra1A,extra2A\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.30,clusterA,thirty,extra1B,extra2A\n" +
				"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.40,clusterA,forty,extra1B,extra2B\n" +
				"";
		ar.readCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), new StringReader(reportData));
		assertFalse("report parser flagged an error", ar.isParsingError());

		CostAndUsageData outData = new CostAndUsageData(data, null, UserTagKey.getUserTagKeys(reportUserTagKeys));
		outData.enableTagGroupCache(true);
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, outData, ar, rs);
		vrp.process(data);

		// Make sure source data wasn't changed
		assertEquals("wrong number of output records", 1, data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(0).keySet().size());
    	TagGroup tg = dataSpecs[0].getTagGroup(as, ps);
    	Map<TagGroup, CostAndUsage> costData = data.get(tg.product).getData(0);
    	assertEquals("wrong data for spec " + dataSpecs[0].toString(), dataSpecs[0].value, costData.get(tg));

    	Account a = as.getAccountById(a1);
        TagGroup[] expectedTg = new TagGroup[]{
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"", "clusterA", "compute", ""})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extra1A", "clusterA", "twenty-five", "extra2A"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extra1B", "clusterA", "thirty", "extra2A"})),
        		TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extra1B", "clusterA", "forty", "extra2B"})),
         };
        Double[] expectedValues = new Double[]{ 5.0, 25.0, 30.0, 40.0 };

    	costData = outData.get(null).getData(0);
        for (int i = 0; i < expectedTg.length; i++) {
        	tg = expectedTg[i];
        	CostAndUsage v = costData.get(tg);
        	assertEquals("wrong data for spec " + tg, expectedValues[i], v.cost, 0.001);
        }
	}

	class TestKubernetesReport extends KubernetesReport {

		public TestKubernetesReport(AllocationConfig config, DateTime start, File file, ResourceService resourceService) throws Exception {
			super(config, start, resourceService);

			readFile(file);
		}
		
		public TestKubernetesReport(AllocationConfig config, DateTime start, String[] rows, ResourceService resourceService) throws Exception {
			super(config, start, resourceService);  
			
			String input = String.join("\n", rows);
			readFile("test", new ByteArrayInputStream(input.getBytes()));
		}
	}

	@Test
	public void testGenerateAllocationReport() throws Exception {
		File file = new File(resourceDir, "kubernetes-2019-01.csv");
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Cluster","Role","K8sNamespace","Environment","K8sType","K8sResource","UserTag1","UserTag2"}, false);
		String allocationYaml = "" +
				"name: k8s\n" +
				"start: 2019-01\n" +
				"end: 2022-11\n" +
				"in:\n" +
				"  filter:\n" +
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
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2a", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{"dev-usw2a", "compute", "", "Dev", "", "", "", ""}, 40.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		final int testDataHour = 395;
        loadData(dataSpecs, data, testDataHour, rs.getUserTagKeys().size());

        RuleConfig rc = getConfig(allocationYaml);
		Rule rule = new Rule(rc, as, ps, rs.getCustomTags());
		DateTime start = new DateTime("2019-01", DateTimeZone.UTC);
		KubernetesReport kr = new TestKubernetesReport(rc.getAllocation(), start, file, rs);
		Set<String> unprocessedClusters = Sets.newHashSet(kr.getClusters());
		Set<String> unprocessedAtgs = Sets.newHashSet();

		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, null, rs);
		AllocationReport ar = vrp.generateAllocationReport(kr, data, unprocessedClusters, unprocessedAtgs);
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);

		assertEquals("wrong number of hours", testDataHour+1, ar.getNumHours());
		assertEquals("wrong number of keys", 1, ar.getKeySet(testDataHour).size());

		AllocationReport.Key key = ar.getKeySet(testDataHour).iterator().next();
		Map<AllocationReport.Key, Double> values = ar.getData(testDataHour, key);
		assertEquals("wrong number of allocation items", 11, values.size());

		for (AllocationReport.Key k: values.keySet()) {
			assertFalse("Empty namespace tag", ar.getTagValue(k, "K8sNamespace").isEmpty());
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
	public void testGenerateAllocationReportWithUsageType() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Cluster","Environment","K8sNamespace","K8sType","Role","UserTag1","UserTag2"}, false);
		String allocationYaml = "" +
				"name: k8s\n" +
				"start: 2019-01\n" +
				"end: 2022-11\n" +
				"in:\n" +
				"  filter:\n" +
				"    userTags:\n" +
				"      Cluster: [cluster]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"    region: us-east-1\n" +
				"    accountId: 123456789012\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"    _usageType: _UsageType\n" +
				"    Cluster: Cluster\n" +
				"  out:\n" +
				"    K8sNamespace: K8sNamespace\n" +
				"    UserTag1: UserTag1\n" +
				"    UserTag2: AltUserTag2\n" +
				"  kubernetes:\n" +
				"    clusterNameFormulae: [Cluster]\n" +
				"    type: Pod\n" +
				"    out:\n" +
				"      Namespace: K8sNamespace\n" +
				"";
		String[] k8sReport = {
				"Cluster,Type,Resource,Namespace,UsageType,StartDate,EndDate,RequestsCPUCores,UsedCPUCores,LimitsCPUCores,ClusterCPUCores,RequestsMemoryGiB,UsedMemoryGiB,LimitsMemoryGiB,ClusterMemoryGiB,NetworkInGiB,ClusterNetworkInGiB,NetworkOutGiB,ClusterNetworkOutGiB,PersistentVolumeClaimGiB,ClusterPersistentVolumeClaimGiB,UserTag1,AltUserTag2",
				"cluster,Pod,abc123xyz,namespace1,r5.4xlarge,2021-01-01T00:00:00Z,2021-01-01T01:00:00Z,10,0,0,100,20,0,0,200,0,0,0,0,1,20,tag1A,tag2A",
				"cluster,Pod,cde123xyz,namespace2,p3.2xlarge,2021-01-01T00:00:00Z,2021-01-01T01:00:00Z,50,0,0,100,30,0,0,500,0,0,0,0,2,30,tag1B,tag2B",
		};
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2b", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{"cluster", "", "", "", "", "", ""}, 40.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2a", ec2Instance, "RunInstances", "p3.2xlarge", new String[]{"cluster", "", "", "", "", "", ""}, 40.0, 0),
        		
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2b", ebs, "CreateVolume-Gp2", "EBS:VolumeUsage.gp2", new String[]{"cluster", "", "", "", "", "", ""}, 8.0, 0),
        		
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2b", cloudWatch, "MetricStorage:AWS/EC2", "CW:MetricMonitorUsage", new String[]{"cluster", "", "", "", "", "", ""}, 1.0, 0),
        		
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2b", dataTransfer, "InterZone-In", "DataTransfer-Regional-Bytes", new String[]{"cluster", "", "", "", "", "", ""}, 1.0, 0),
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2b", dataTransfer, "InterZone-Out", "DataTransfer-Regional-Bytes", new String[]{"cluster", "", "", "", "", "", ""}, 2.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		final int testDataHour = 0;
        loadData(dataSpecs, data, testDataHour, rs.getUserTagKeys().size());
		
        RuleConfig rc = getConfig(allocationYaml);
		Rule rule = new Rule(rc, as, ps, rs.getCustomTags());
		DateTime start = new DateTime("2021-01", DateTimeZone.UTC);
		KubernetesReport kr = new TestKubernetesReport(rc.getAllocation(), start, k8sReport, rs);
		
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, null, rs);
		
		Set<String> unprocessedClusters = Sets.newHashSet(kr.getClusters());
		Set<String> unprocessedAtgs = Sets.newHashSet();
		AllocationReport ar = vrp.generateAllocationReport(kr, data, unprocessedClusters, unprocessedAtgs);

		assertEquals("wrong number of hours", testDataHour+1, ar.getNumHours());
		// Check header
		List<String> arHeader = ar.getHeader();
		List<String> expectedHeader = Lists.newArrayList(new String[]{
				"StartDate", "EndDate", "Allocation", "Cluster", "_Product", "_UsageType", "K8sNamespace", "UserTag1", "AltUserTag2"
		});
		assertEquals("wrong header", expectedHeader, arHeader);
		
		// Verify keys
		Key ec2r5Key = new Key(Lists.newArrayList(new String[]{"cluster","EC2Instance","r5.4xlarge"}));
		Key ec2p3Key = new Key(Lists.newArrayList(new String[]{"cluster","EC2Instance","p3.2xlarge"}));
		Key ebsKey = new Key(Lists.newArrayList(new String[]{"cluster","EBS","EBS:VolumeUsage.gp2"}));
		Key cwKey = new Key(Lists.newArrayList(new String[]{"cluster","AmazonCloudWatch","CW:MetricMonitorUsage"}));
		Key dtKey = new Key(Lists.newArrayList(new String[]{"cluster","AWSDataTransfer","DataTransfer-Regional-Bytes"}));

		List<Key> expectedKeys = Lists.newArrayList(new Key[]{ec2r5Key, ec2p3Key, ebsKey, cwKey, dtKey});
		
		assertEquals("wrong number of keys", 5, ar.getKeySet(testDataHour).size());
		for (Key k: expectedKeys) {
			assertNotNull("key not found in allocation report: " + k, ar.getData(testDataHour, k));
		}
		assertEquals("have wrong number of unprocessed clusters", 0, unprocessedClusters.size());
		assertEquals("have unprocessed ATGs", 0, unprocessedAtgs.size());
		
		AllocationReport.HourData got = ar.getData().get(testDataHour);
		
		Map<Key, Double> splits = got.get(ec2r5Key);
		assertEquals("wrong number of allocations for r5 - should only have namespace1 and unused", 2, splits.size());
		assertEquals("wrong ec2 namespace1 allocation for r5", 0.1, splits.get(new Key(Lists.newArrayList(new String[]{"namespace1", "tag1A", "tag2A"}))), 0.001);
		assertEquals("wrong ec2 unused allocation for r5", 0.9, splits.get(new Key(Lists.newArrayList(new String[]{"unused", "", ""}))), 0.001);
		
		splits = got.get(ec2p3Key);
		assertEquals("wrong number of allocations for p3 - should only have namespace2 and unused", 2, splits.size());
		assertEquals("wrong ec2 namespace2 allocation for p3", 0.362, splits.get(new Key(Lists.newArrayList(new String[]{"namespace2", "tag1B", "tag2B"}))), 0.001);
		assertEquals("wrong ec2 unused allocation for p3", 0.638, splits.get(new Key(Lists.newArrayList(new String[]{"unused", "", ""}))), 0.001);
		
		splits = got.get(ebsKey);
		assertEquals("wrong number of allocations for ebs - should only have unused", 3, splits.size());
		assertEquals("wrong ebs namespace1 allocation", 0.05, splits.get(new Key(Lists.newArrayList(new String[]{"namespace1", "tag1A", "tag2A"}))), 0.001);
		assertEquals("wrong ebs namespace2 allocation", 0.067, splits.get(new Key(Lists.newArrayList(new String[]{"namespace2", "tag1B", "tag2B"}))), 0.001);
		assertEquals("wrong ebs unused allocation", 0.883, splits.get(new Key(Lists.newArrayList(new String[]{"unused", "", ""}))), 0.001);
		
		splits = got.get(cwKey);
		assertEquals("wrong number of allocations for cloudwatch - should only have unused", 3, splits.size());
		assertEquals("wrong cloudwatch namespace1 allocation", 0.1, splits.get(new Key(Lists.newArrayList(new String[]{"namespace1", "tag1A", "tag2A"}))), 0.001);
		assertEquals("wrong cloudwatch namespace2 allocation", 0.362, splits.get(new Key(Lists.newArrayList(new String[]{"namespace2", "tag1B", "tag2B"}))), 0.001);
		assertEquals("wrong cloudwatch unused allocation", 0.538, splits.get(new Key(Lists.newArrayList(new String[]{"unused", "", ""}))), 0.001);		
		splits = got.get(dtKey);
		
		assertEquals("wrong number of allocations for data transfer - should only have unused", 1, splits.size());
		assertEquals("wrong data transfer unused allocation", 1, splits.get(new Key(Lists.newArrayList(new String[]{"unused", "", ""}))), 0.001);		
	}

	@Test
	public void testProcessKubernetesReport() throws Exception {
		File file = new File(resourceDir, "kubernetes-2019-01.csv");
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
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2b", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{clusterTags[0], "compute", "", "Dev", "", "", "", ""}, 40.0, 3),
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2a", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{clusterTags[1], "compute", "", "Dev", "", "", "", ""}, 40.0, 3),
        		new TagGroupSpec("Recurring", a1, "us-west-2", "us-west-2a", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{clusterTags[2], "compute", "", "Dev", "", "", "", ""}, 40.0, 3),
        };
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		final int testDataHour = 395;
        loadData(dataSpecs, data, testDataHour, rs.getUserTagKeys().size());

        RuleConfig rc = getConfig(allocationYaml);
		Rule rule = new Rule(rc, as, ps, rs.getCustomTags());
		DateTime start = new DateTime("2019-01", DateTimeZone.UTC);
		KubernetesReport kr = new TestKubernetesReport(rc.getAllocation(), start, file, rs);
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
		Map<TagGroup, CostAndUsage> hourData = data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(testDataHour);

		for (int i = 0; i < clusterTags.length; i++) {
			String clusterTag = clusterTags[i];
			TagGroup tg = dataSpecs[i].getTagGroup(as, ps);

			String[] atags = new String[]{ clusterTags[i], "compute", "kube-system", "Dev", "", "", "", "" };
			ResourceGroup arg = ResourceGroup.getResourceGroup(atags);
			TagGroup atg = tg.withResourceGroup(arg);

			CostAndUsage allocatedCau = hourData.get(atg);
			assertNotNull("No allocated cost for kube-system namespace with cluster tag " + clusterTag, allocatedCau);
			assertEquals("Incorrect allocated cost with cluster tag " + clusterTag, expectedAllocatedCosts[i], allocatedCau.cost, 0.0001);
			String[] unusedTags = new String[]{ clusterTag, "compute", "unused", "Dev", "unused", "unused", "", "" };
			TagGroup unusedTg = TagGroup.getTagGroup(tg.costType, tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, ResourceGroup.getResourceGroup(unusedTags));
			CostAndUsage unusedCau = hourData.get(unusedTg);
			assertEquals("Incorrect unused cost with cluster tag " + clusterTag, expectedUnusedCosts[i], unusedCau.cost, 0.0001);

			int count = 0;
			for (TagGroup tg1: hourData.keySet()) {
				if (tg1.resourceGroup.getUserTags()[0].name.equals(clusterTags[i]))
					count++;
			}
			assertEquals("Incorrect number of cost entries for " + clusterTag, expectedAllocationCounts[i], count);
		}

		// Add up all the cost values to see if we get back to 120.0 (Three tagGroups with 40.0 each)
		CostAndUsage total = new CostAndUsage(0, 0);
		for (CostAndUsage v: hourData.values())
			total = total.add(v);
		assertEquals("Incorrect total cost when adding unused and allocated values", 120.0, total.cost, 0.001);
		assertEquals("Incorrect total usage when adding unused and allocated values", 9.0, total.usage, 0.001);
	}

	@Test
	public void testProcessKubernetesReportWithType() throws Exception {
		File file = new File(resourceDir + "private/", "kubernetes-2020-10.csv");
		if (!file.exists())
			return;

		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Application","CostCenter", "CreatedBy","Environment","K8sNamespace","K8sType","Market","Platform","Process","Product","Role"}, false);
		String allocationYaml = "" +
				"name: k8s\n" +
				"start: 2020-10\n" +
				"end: 2099-01\n" +
				"in:\n" +
				"  filter:\n" +
				"    account: [" + a1 + "]\n" +
				"    userTags:\n" +
				"      Role: [compute]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"    region: us-east-1\n" +
				"    accountId: 123456789012\n" +
				"  kubernetes:\n" +
				"    clusterNameFormulae: ['\"npd-blue-us-east-1\"']\n" +
				"    out:\n" +
				"      Namespace: K8sNamespace\n" +
				"      Type: K8sType\n" +
				"    type: Pod\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"  out:\n" +
				"    Application: Application\n" +
				"    CostCenter: CostCenter\n" +
				"    CreatedBy: CreatedBy\n" +
				"    Environment: Environment\n" +
				"    K8sNamespace: K8sNamespace\n" +
				"    K8sType: K8sType\n" +
				"    Market: Market\n" +
				"    Platform: Platform\n" +
				"    Process: Process\n" +
				"    Product: Product\n" +
				"";

		String clusterTag = "npd-blue-us-east-1";
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "us-east-1", "us-east-1b", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{"","","iamUser","","","","","","","", "compute"}, 40.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		final int testDataHour = 264; // 2020-10-12T00:00:00Z
        loadData(dataSpecs, data, testDataHour, rs.getUserTagKeys().size());

        RuleConfig rc = getConfig(allocationYaml);
		Rule rule = new Rule(rc, as, ps, rs.getCustomTags());
		DateTime start = new DateTime("2020-10", DateTimeZone.UTC);
		KubernetesReport kr = new TestKubernetesReport(rc.getAllocation(), start, file, rs);
		Set<String> unprocessedClusters = Sets.newHashSet(kr.getClusters());
		Set<String> unprocessedAtgs = Sets.newHashSet();
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, null, rs);
		AllocationReport ar = vrp.generateAllocationReport(kr, data, unprocessedClusters, unprocessedAtgs);
		ar.writeFile(start, new File(file.getParent(), "ar-" + file.getName()), false);
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);

		assertEquals("have wrong number of unprocessed clusters", 0, unprocessedClusters.size());
		assertEquals("have unprocessed ATGs", 0, unprocessedAtgs.size());

		double expectedAllocatedCost = 0.00005598;
		double expectedUnusedCost = 11.5679;
		Map<TagGroup, CostAndUsage> hourCostData = data.get(ps.getProduct(Product.Code.Ec2Instance)).getData(testDataHour);

		TagGroup tg = dataSpecs[0].getTagGroup(as, ps);

		String[] atags = new String[]{"","","iamUser","","default","Pod","","","","", "compute"};
		ResourceGroup arg = ResourceGroup.getResourceGroup(atags);
		TagGroup atg = tg.withResourceGroup(arg);

		CostAndUsage allocatedCau = hourCostData.get(atg);
		assertNotNull("No allocated cost for default namespace with cluster tag " + clusterTag, allocatedCau);
		assertEquals("Incorrect allocated cost with cluster tag " + clusterTag, expectedAllocatedCost, allocatedCau.cost, 0.0000001);
		String[] unusedTags = new String[]{"","","iamUser","","unused","unused","","","","", "compute"};
		TagGroup unusedTg = TagGroup.getTagGroup(tg.costType, tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, ResourceGroup.getResourceGroup(unusedTags));
		CostAndUsage unusedCau = hourCostData.get(unusedTg);
		assertEquals("Incorrect unused cost with cluster tag " + clusterTag, expectedUnusedCost, unusedCau.cost, 0.0001);

		// Add up all the cost values to see if we get back to 40.0
		double total = 0.0;
		for (CostAndUsage v: hourCostData.values())
			total += v.cost;
		assertEquals("Incorrect total cost when adding unused and allocated values", 40.0, total, 0.001);
	}

	@Test
	public void testProcessKubernetesReportTestStub() throws Exception {
		File file = new File(resourceDir + "private/", "kubernetes-2020-11.csv.gz");
		if (!file.exists())
			return;

		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Application","CostCenter", "CreatedBy","Environment","K8sNamespace","K8sType","Market","Platform","Process","Product","Role"}, false);
		String allocationYaml = "" +
				"name: k8s\n" +
				"start: 2020-10\n" +
				"end: 2099-01\n" +
				"in:\n" +
				"  filter:\n" +
				"    account: [" + a1 + "]\n" +
				"    userTags:\n" +
				"      Role: [compute]\n" +
				"allocation:\n" +
				"  s3Bucket:\n" +
				"    name: reports\n" +
				"    region: eu-west-1\n" +
				"    accountId: 123456789012\n" +
				"  kubernetes:\n" +
				"    clusterNameFormulae: ['\"npd-red-eu-west-1\"']\n" +
				"    out:\n" +
				"      Namespace: K8sNamespace\n" +
				"      Type: K8sType\n" +
				"    type: Pod\n" +
				"  in:\n" +
				"    _product: _Product\n" +
				"  out:\n" +
				"    Application: Application\n" +
				"    CostCenter: CostCenter\n" +
				"    CreatedBy: CreatedBy\n" +
				"    Environment: Environment\n" +
				"    K8sNamespace: K8sNamespace\n" +
				"    K8sType: K8sType\n" +
				"    Market: Market\n" +
				"    Platform: Platform\n" +
				"    Process: Process\n" +
				"    Product: Product\n" +
				"";

        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec("Recurring", a1, "eu-west-1", "eu-west-1a", ec2Instance, "RunInstances", "r5.4xlarge", new String[]{"","","iamUser","","","","","","","", "compute"}, 40.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", "eu-west-1a", ebs, "RunInstances", "r5.4xlarge", new String[]{"","","iamUser","","","","","","","", "compute"}, 40.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", "eu-west-1a", dataTransfer, "RunInstances", "r5.4xlarge", new String[]{"","","iamUser","","","","","","","", "compute"}, 40.0, 0),
        		new TagGroupSpec("Recurring", a1, "eu-west-1", "eu-west-1a", cloudWatch, "RunInstances", "r5.4xlarge", new String[]{"","","iamUser","","","","","","","", "compute"}, 40.0, 0),
        };
		CostAndUsageData data = new CostAndUsageData(null, 0, null, rs.getUserTagKeys(), null, as, ps);
		for (int i = 0; i < 720; i++)
			loadData(dataSpecs, data, i, rs.getUserTagKeys().size());

        RuleConfig rc = getConfig(allocationYaml);
		Rule rule = new Rule(rc, as, ps, rs.getCustomTags());
		DateTime start = new DateTime("2020-11", DateTimeZone.UTC);
		KubernetesReport kr = new TestKubernetesReport(rc.getAllocation(), start, file, rs);
		Set<String> unprocessedClusters = Sets.newHashSet(kr.getClusters());
		Set<String> unprocessedAtgs = Sets.newHashSet();
		VariableRuleProcessor vrp = new TestVariableRuleProcessor(rule, null, null, rs);
		AllocationReport ar = vrp.generateAllocationReport(kr, data, unprocessedClusters, unprocessedAtgs);
		ar.writeFile(start, new File(file.getParent(), "ar-" + file.getName()), false);
		vrp = new TestVariableRuleProcessor(rule, null, ar, rs);
		vrp.process(data);

		List<AllocationReport.HourData> arData = ar.getData();

		for (int hour = 0; hour < arData.size(); hour++) {
			AllocationReport.HourData allocations = arData.get(hour);
			for (Key key: allocations.keySet()) {
				Double total = 0.0;
				Map<Key, Double> outMap = allocations.get(key);
				for (Key outKey: outMap.keySet()) {
					total += outMap.get(outKey);
				}
				if (total > 1.000001) {
					logger.info("Over allocation for " + key + " at hour " + hour + ": " + total);
				}
			}
		}
	}
}
