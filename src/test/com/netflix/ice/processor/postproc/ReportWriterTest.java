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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;

public class ReportWriterTest {
	static final String ec2Instance = Product.Code.Ec2Instance.serviceCode;
	static private String a1 = "111111111111";

    static private ProductService ps;
	static private AccountService as;
	
	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
		ps.getProduct(Product.Code.CloudFront);
		ps.getProduct(Product.Code.CloudWatch);
	}

	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	@Test
	public void test() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String allocationYaml = "" +
				"name: report-test\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"report:\n" + 
				"  aggregate: [monthly]\n" + 
				"  types: [cost]\n" + 
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
		Rule rule = new Rule(getConfig(allocationYaml), as, ps, rs.getCustomTags());

    	Account a = as.getAccountById(a1);
        List<String> userTagKeys = Lists.newArrayList(new String[]{"Extra1", "Key1", "Key2", "Extra2"});
        DateTime start = new DateTime("2020-08-01", DateTimeZone.UTC);
    	
    	class Datum {
    		int num;
    		CostAndUsage value;
    		TagGroup tg;
    		
    		public Datum(int num, double cost, double usage, TagGroup tg) {
    			this.num = num;
    			this.value = new CostAndUsage(cost, usage);
    			this.tg = tg;
    		}
    	}
        Datum[] data = new Datum[]{
        		new Datum(0,  5.0, 0, TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"", "clusterA", "compute", ""}))),
        		new Datum(0, 25.0, 0, TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extra1A", "clusterA", "twenty-five", "extra2A"}))),
        		new Datum(0, 30.0, 0, TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extra1B", "clusterA", "thirty", "extra2A"}))),
        		new Datum(0, 40.0, 0, TagGroup.getTagGroup(CostType.recurring, a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"extra1B", "clusterA", "forty", "extra2B"}))),
        };

        DataSerializer ds = new DataSerializer(2);
        for (int i = 0; i < data.length; i++) {
        	Datum d = data[i];
        	ds.add(d.num, d.tg, d.value);	
        }
        
		String expectedHeader = "Date,Cost,CostType,Account ID,Account Name,Extra1,Key1,Key2,Extra2";
		String[] expectedRows = new String[]{
			"2020-08-01T00:00:00Z,5.0,Recurring,111111111111,111111111111,,clusterA,compute,",
			"2020-08-01T00:00:00Z,25.0,Recurring,111111111111,111111111111,extra1A,clusterA,twenty-five,extra2A",
			"2020-08-01T00:00:00Z,30.0,Recurring,111111111111,111111111111,extra1B,clusterA,thirty,extra2A",
			"2020-08-01T00:00:00Z,40.0,Recurring,111111111111,111111111111,extra1B,clusterA,forty,extra2B",
		};
		
        // Test monthly aggregation
		
		ReportWriter writer = new ReportWriter("", "report-test", rule.config.getReport(), null, start, rule.getGroupBy(), userTagKeys, ds, RuleConfig.Aggregation.monthly);		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Writer out = new OutputStreamWriter(os);
		writer.writeCsv(out);		
		String csv = os.toString();
		
		checkReport(csv, expectedHeader, expectedRows);
		
		// Test daily aggregation -- add some additional items from above at different times
		
		ds = new DataSerializer(2);
		for (int i = 0; i < data.length; i++) {
        	Datum d = data[i];
        	ds.add(i, d.tg, d.value);	
        }
		expectedRows = new String[]{
			"2020-08-01T00:00:00Z,5.0,Recurring,111111111111,111111111111,,clusterA,compute,",
			"2020-08-02T00:00:00Z,25.0,Recurring,111111111111,111111111111,extra1A,clusterA,twenty-five,extra2A",
			"2020-08-03T00:00:00Z,30.0,Recurring,111111111111,111111111111,extra1B,clusterA,thirty,extra2A",
			"2020-08-04T00:00:00Z,40.0,Recurring,111111111111,111111111111,extra1B,clusterA,forty,extra2B",
		};
		writer = new ReportWriter("", "report-test", rule.config.getReport(), null, start, rule.getGroupBy(), userTagKeys, ds, RuleConfig.Aggregation.daily);		
		os = new ByteArrayOutputStream();
		out = new OutputStreamWriter(os);
		writer.writeCsv(out);		
		csv = os.toString();
		checkReport(csv, expectedHeader, expectedRows);
		
		// Test hourly aggregation -- add some additional items from above at different times
		
		ds = new DataSerializer(2);
        for (int i = 0; i < data.length; i++) {
        	Datum d = data[i];
        	ds.add(i, d.tg, d.value);	
        }
		expectedRows = new String[]{
			"2020-08-01T00:00:00Z,5.0,Recurring,111111111111,111111111111,,clusterA,compute,",
			"2020-08-01T01:00:00Z,25.0,Recurring,111111111111,111111111111,extra1A,clusterA,twenty-five,extra2A",
			"2020-08-01T02:00:00Z,30.0,Recurring,111111111111,111111111111,extra1B,clusterA,thirty,extra2A",
			"2020-08-01T03:00:00Z,40.0,Recurring,111111111111,111111111111,extra1B,clusterA,forty,extra2B",
		};
		writer = new ReportWriter("", "report-test", rule.config.getReport(), null, start, rule.getGroupBy(), userTagKeys, ds, RuleConfig.Aggregation.hourly);		
		os = new ByteArrayOutputStream();
		out = new OutputStreamWriter(os);
		writer.writeCsv(out);		
		csv = os.toString();
		checkReport(csv, expectedHeader, expectedRows);		
	}

	private void checkReport(String csv, String expectedHeader, String[] eRows) {
		String[] rows = csv.split("\r\n");
		assertEquals("wrong number of lines", eRows.length + 1, rows.length);
		
		List<String> rowsSorted = Lists.newArrayList();
		for (int i = 1; i < rows.length; i++)
			rowsSorted.add(rows[i]);
		Collections.sort(rowsSorted);

		List<String> expectedRows = Lists.newArrayList(eRows);
		Collections.sort(expectedRows);
		
		int numCols = expectedHeader.split(",", -1).length;
				
		// Check header
		assertEquals("wrong header", expectedHeader, rows[0]);
		for (int i = 0; i < rowsSorted.size(); i++) {
			String row = rowsSorted.get(i);
			String expected = expectedRows.get(i);
			assertEquals("wrong number of columns on row" + Integer.toString(i + 1), numCols, row.split(",", -1).length);
			
			// Check for match on all but allocation number
			String[] cols = row.split(",", -1);
			String[] eCols = expected.split(",", -1);
			Double v = Double.parseDouble(cols[1]);
			cols[1] = "";
			Double e = Double.parseDouble(eCols[1]);
			eCols[1] = "";
			assertArrayEquals("wrong results for line " + Integer.toString(i), eCols, cols);
			assertEquals("wrong value for line " + Integer.toString(i), e, v, 0.0001);
		}
	}
}
