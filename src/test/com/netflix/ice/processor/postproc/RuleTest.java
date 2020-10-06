package com.netflix.ice.processor.postproc;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;

public class RuleTest {
    static private ProductService ps;
	static private AccountService as;
	static private BasicResourceService rs;

	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
		rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
	}

	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	private String inputOperandTestYaml = "" +
			"name: inputOperandTest\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  accountAgg:\n" + 
			"    type: usage\n" +
			"    groupBy: [Region,Zone,Product,Operation,UsageType]\n" +
			"    usageType: ${group}-Usage\n" + 
			"  accountAggByList:\n" + 
			"    type: usage\n" +
			"    accounts: [1,2,3]\n" +
			"    usageType: ${group}-Usage\n" + 
			"  accountExAgg:\n" + 
			"    type: usage\n" +
			"    accounts: [1,2,3]\n" +
			"    usageType: ${group}-Usage\n" + 
			"in:\n" + 
			"  type: usage\n" + 
			"  product: Test\n" + 
			"  usageType: (..)-Usage\n" +
			"results:\n" +
			"  - out:\n" + 
			"      type: cost\n" + 
			"      product: Foo\n" + 
			"      usageType: Bar\n" + 
			"    value: '0'\n";
	
	@Test
	public void testOperandHasAggregation() throws Exception {
		Rule rule = new Rule(getConfig(inputOperandTestYaml), as, ps, rs.getCustomTags());
		assertFalse("in operand incorrectly indicates that it has aggregation", rule.getIn().hasAggregation());
		assertTrue("accountAgg operand incorrectly indicates it has no aggregation", rule.getOperand("accountAgg").hasAggregation());
		assertTrue("accountAggByList operand incorrectly indicates it has no aggregation", rule.getOperand("accountAggByList").hasAggregation());
		assertTrue("accountExAgg operand incorrectly indicates it has no aggregation", rule.getOperand("accountExAgg").hasAggregation());
	}
	

}
