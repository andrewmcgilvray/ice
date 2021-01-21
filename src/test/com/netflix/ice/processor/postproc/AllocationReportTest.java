package com.netflix.ice.processor.postproc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.postproc.AllocationReport.Key;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;

public class AllocationReportTest {
	static final String ec2Instance = Product.Code.Ec2Instance.serviceCode;
	static private String a1 = "111111111111";

	@Test
	public void testKey() {
		// Test that we get the same hashcode for different objects with the same strings
		List<String> tags1 = Lists.newArrayList(new String[]{"foo", "bar"});
		List<String> tags2 = Lists.newArrayList(new String[]{"foo", "bar"});
		
		Key key1 = new Key(tags1);
		Key key2 = new Key(tags2);
		assertEquals("hashcodes don't match", key1.hashCode(), key2.hashCode());
		
		Map<Key, String> m = Maps.newHashMap();
		m.put(key1, "foo");
		m.put(key2, "bar");
		assertEquals("should only have one entry in map", 1, m.size());
	}
	
	private AllocationConfig getAllocationConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		AllocationConfig ac = new AllocationConfig();
		return mapper.readValue(yaml, ac.getClass());
	}
	
	@Test
	public void testConstructor() throws Exception {
		BasicProductService ps = new BasicProductService();
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"InTag1", "InTag2", "OutTag1", "OutTag2"}, false);
		
		// config with same key in both in and out, but different column names
		String arConfigYaml = "" +
		"s3Bucket:\n" +
		"  name: bucket-name\n" + 
		"  region: us-east-1\n" + 
		"  prefix: cluster-name\n" + 
		"  accountId: 123456789012\n" + 
		"in:\n" + 
		"  InTag1: inCol1\n" + 
		"out:\n" + 
		"  InTag1: outCol1\n" + 
		""; 
				
		AllocationReport ar = new AllocationReport(getAllocationConfig(arConfigYaml), 0, false, rs.getCustomTags(), rs);
		assertNotNull("report is null", ar);
	}
	
	@Test(expected = Exception.class)
	public void testConstructorThrows() throws Exception {
		BasicProductService ps = new BasicProductService();
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"InTag1", "InTag2", "OutTag1", "OutTag2"}, false);
			
		// config with same key in both in and out and same column names
		String arConfigYaml = "" +
		"s3Bucket:\n" +
		"  name: bucket-name\n" + 
		"  region: us-east-1\n" + 
		"  prefix: cluster-name\n" + 
		"  accountId: 123456789012\n" + 
		"type: cost\n" + 
		"in:\n" + 
		"  InTag1: inCol1\n" + 
		"out:\n" + 
		"  InTag1: inCol1\n" + 
		""; 
		
		// should throw
		new AllocationReport(getAllocationConfig(arConfigYaml), 0, false, rs.getCustomTags(), rs);
		fail("constructor didn't throw");
	}

	@Test
	public void testWriteCsv() throws Exception {
		BasicProductService ps = new BasicProductService();
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"InTag1", "InTag2", "OutTag1", "OutTag2"}, false);
		
		String arConfigYaml = "" +
		"s3Bucket:\n" +
		"  name: bucket-name\n" + 
		"  region: us-east-1\n" + 
		"  prefix: cluster-name\n" + 
		"  accountId: 123456789012\n" + 
		"in:\n" + 
		"  InTag1: inCol1\n" + 
		"  InTag2: inCol2\n" + 
		"out:\n" + 
		"  OutTag1: outCol1\n" + 
		"  OutTag2: outCol2\n"; 
				
		AllocationReport ar = new AllocationReport(getAllocationConfig(arConfigYaml), 0, false, rs.getCustomTags(), rs);
		ar.add(0, 1.0, Lists.newArrayList(new String[]{"inA", "inB"}), Lists.newArrayList(new String[]{"outA", "outB"}));
		
		StringWriter out = new StringWriter();
		ar.writeCsv(new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC), out);
		String result = out.toString();
		StringReader reader = new StringReader(result);
    	Iterable<CSVRecord> records = CSVFormat.DEFAULT
  		      .withFirstRecordAsHeader()
  		      .parse(reader);
		String[] header = records.iterator().next().getParser().getHeaderNames().toArray(new String[]{});
		String[] expectedHeader = new String[]{"StartDate", "EndDate", "Allocation", "inCol1", "inCol2", "outCol1", "outCol2"};
		assertArrayEquals("Incorrect header", expectedHeader, header);
	}

	@Test
	public void testReadCSV() throws Exception {
		
		BasicProductService ps = new BasicProductService();
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"InTag1", "InTag2", "OutTag1", "OutTag2"}, false);
		
		String arConfigYaml = "" +
		"s3Bucket:\n" +
		"  name: bucket-name\n" + 
		"  region: us-east-1\n" + 
		"  prefix: cluster-name\n" + 
		"  accountId: 123456789012\n" + 
		"in:\n" + 
		"  InTag1: inCol1\n" + 
		"  InTag2: inCol2\n" + 
		"out:\n" + 
		"  OutTag1: outCol1\n" + 
		"  OutTag2: outCol2\n"; 
				
		AllocationReport ar = new AllocationReport(getAllocationConfig(arConfigYaml), 0, false, rs.getCustomTags(), rs);
		
		// Throw in a record with a NaN for the allocation to make sure we ignore it
		String csv = "" +
		"StartDate,EndDate,Allocation,inCol1,inCol2,outCol1,outCol2\n" +
		"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.75,in1a,in1b,out1a,out1b\n" +
		"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,0.25,in1a,in1b,out2a,out2b\n" +
		"2020-08-01T00:00:00Z,2020-08-01T01:00:00Z,NaN,in1a,in1b,out2a,out2b\n" +
		"";
		
		DateTime start = new DateTime("2020-08-01T00:00:00Z", DateTimeZone.UTC);
		
		ar.readCsv(start, new StringReader(csv));
		
		assertEquals("wrong number of hours", 1, ar.getNumHours());
		assertEquals("wrong number of keys", 1, ar.getKeySet(0).size());
		assertEquals("wrong number of values", 2, ar.getData(0, ar.getKeySet(0).iterator().next()).size());
		
		// Validate Key
		Key key = ar.getKeySet(0).iterator().next();
		assertEquals("wrong number of inputs", 2, key.getTags().size());
		assertEquals("wrong value for first input key", "in1a", key.getTags().get(0));
		assertEquals("wrong value for second input key", "in1b", key.getTags().get(1));
		
		// Validate Values
		String[][] expected = new String[][]{
			new String[]{"out1a", "out1b"},
			new String[]{"out2a", "out2b"},
		};
		Map<Key, Double> values = ar.getData(0, key);
		for (int i = 0; i < expected.length; i++) {
			Key outKey = new Key(Lists.newArrayList(expected[i]));
			assertNotNull("expected key not found", values.get(outKey));
		}
	} 
	
	@Test
	public void testGetOutputTagGroup() throws Exception {
		BasicProductService ps = new BasicProductService();
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1", "Key2", "Key3", "Key4"}, false);
		BasicAccountService as = new BasicAccountService();
		
		// config with same key in both in and out, but different column names
		String arConfigYaml = "" +
				"s3Bucket:\n" +
				"  name: reports\n" +
				"in:\n" +
				"  _product: _Product\n" +
				"  Key1: Key1\n" +
				"out:\n" +
				"  Key2: Key2\n" +
				"tagMaps:\n" +
				"  Key2:\n" +
				"  - force: true\n" +
				"    maps:\n" +
				"      unused:\n" + // Mark unused if Key2 is empty
				"        key: Key4\n" +
				"        operator: isOneOf\n" +
				"        values: [A,D]\n" +
				"  Key4:\n" +
				"  - force: true\n" +
				"    maps:\n" +
				"      '':\n" +
				"        key: Key4\n" +
				"        operator: isOneOf\n" +
				"        values: [D]\n" +
				"  Key3:\n" +
				"  - maps:\n" +
				"      V25:\n" +
				"        key: Key2\n" +
				"        operator: isOneOf\n" +
				"        values: [25,'twenty-.*']\n" +
				"      V70:\n" +
				"        key: Key2\n" +
				"        operator: isOneOf\n" +
				"        values: [seventy]\n" +
				"";
				
		AllocationReport ar = new AllocationReport(getAllocationConfig(arConfigYaml), 0, false, rs.getCustomTags(), rs);
		
        TagGroupSpec[] data = new TagGroupSpec[]{
        		new TagGroupSpec(a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "compute", "", "A"}, 50.0, 0),
        		new TagGroupSpec(a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "", "", "B"}, 250.0, 0),
        		new TagGroupSpec(a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "", "", "C"}, 700.0, 0),
        		new TagGroupSpec(a1, "us-east-1", ec2Instance, "RunInstances", "m5.2xlarge", new String[]{"clusterA", "", "", "D"}, 0.0, 0),
         };
        Key[] outputKeys = {
        		new Key(Lists.newArrayList(new String[]{"compute"})),
        		new Key(Lists.newArrayList(new String[]{"twenty-five"})),
        		new Key(Lists.newArrayList(new String[]{"seventy"})),
        		new Key(Lists.newArrayList(new String[]{""})),
        };
        
        ResourceGroup[] expected = {
        		ResourceGroup.getResourceGroup(new String[]{"clusterA", "unused", "", "A"}),
        		ResourceGroup.getResourceGroup(new String[]{"clusterA", "twenty-five", "V25", "B"}),
        		ResourceGroup.getResourceGroup(new String[]{"clusterA", "seventy", "V70", "C"}),
        		ResourceGroup.getResourceGroup(new String[]{"clusterA", "unused", "", ""}),
        };
        
        for (int i = 0; i < data.length; i++) {
        	TagGroupSpec tgs = data[i];
            TagGroup got = ar.getOutputTagGroup(outputKeys[i], tgs.getTagGroup(as, ps));
            assertEquals("resource group doesn't match", expected[i], got.resourceGroup);
        }
	}
	

}
