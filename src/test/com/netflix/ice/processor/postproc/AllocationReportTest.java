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
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.processor.postproc.AllocationReport.Key;
import com.netflix.ice.processor.postproc.AllocationReport.Value;

public class AllocationReportTest {

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
				
		AllocationReport ar = new AllocationReport(getAllocationConfig(arConfigYaml), false, rs.getCustomTags());
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
		new AllocationReport(getAllocationConfig(arConfigYaml), false, rs.getCustomTags());
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
				
		AllocationReport ar = new AllocationReport(getAllocationConfig(arConfigYaml), false, rs.getCustomTags());
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
				
		AllocationReport ar = new AllocationReport(getAllocationConfig(arConfigYaml), false, rs.getCustomTags());
		
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
		assertEquals("wrong number of inputs", 2, key.getInputs().size());
		assertEquals("wrong value for first input key", "in1a", key.getInputs().get(0));
		assertEquals("wrong value for second input key", "in1b", key.getInputs().get(1));
		
		// Validate Values
		String[][] expected = new String[][]{
			new String[]{"out1a", "out1b"},
			new String[]{"out2a", "out2b"},
		};
		List<Value> values = ar.getData(0, key);
		for (int i = 0; i < values.size(); i++) {
			assertEquals("wrong number of outputs", 2, values.get(i).getOutputs().size());
			for (int j = 0; j < values.get(i).getOutputs().size(); j++)
				assertEquals("wrong value for output", expected[i][j], values.get(i).getOutputs().get(j));
		}
	} 
}
