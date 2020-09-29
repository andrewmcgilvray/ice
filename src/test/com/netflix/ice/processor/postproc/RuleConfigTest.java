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

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;

public class RuleConfigTest {

	@Test
	public void testFileRead() throws IOException {
		String yaml = "" +
		"name: ComputedCost\n" + 
		"start: 2019-11\n" + 
		"end: 2022-11\n" + 
		"operands:\n" + 
		"  data:\n" + 
		"    type: usage\n" + 
		"    usageType: ${group}-DataTransfer-Out-Bytes\n" + 
		"in:\n" + 
		"  type: usage\n" + 
		"  product: Product\n" + 
		"  usageType: (..)-Requests-[12].*\n" + 
		"results:\n" + 
		"  - out:\n" + 
		"      type: cost\n" + 
		"      product: ComputedCost\n" + 
		"      usageType: ${group}-Requests\n" + 
		"    value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'\n" + 
		"";
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		RuleConfig rc = new RuleConfig();
		rc = mapper.readValue(yaml, rc.getClass());
		
		assertEquals("Wrong rule name", "ComputedCost", rc.getName());
		assertEquals("Wrong number of operands", 1, rc.getOperands().size());
		assertEquals("Wrong in operand type", OperandType.usage, rc.getIn().getType());
		OperandConfig out = rc.getResults().get(0).getOut();
		assertEquals("Wrong product in result", "ComputedCost", out.getProduct());
		assertEquals("Wrong usageType in result", "${group}-Requests", out.getUsageType());
		assertEquals("Wrong out function", "(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000", rc.getResults().get(0).getValue());
	}

	@Test
	public void testAllocationRuleRead() throws JsonParseException, JsonMappingException, IOException {
		String yaml = "" +
		"name: kubernetes-breakout\n" + 
		"start: 2019-11\n" + 
		"end: 2022-11\n" + 
		"in:\n" + 
		"  type: cost\n" + 
		"  product: Product\n" + 
		"  userTags:\n" + 
		"    Role: compute\n" + 
	    "allocation: # Perform allocations provided through an allocation report (only one of allocation or results may be present)\n" +
	    "  s3Bucket:\n" +
	    "    name: k8s-report-bucket\n" +
	    "    region: us-east-1\n" +
	    "    prefix: hourly/kubernetes\n" +
	    "    accountId: 123456789012\n" +
	    "    accessRole: ice-role\n" +
	    "    externalId: 234567890123\n" +
	    "  kubernetes: # use the kubernetes precprocessor i.e. preprocess a Kubernetes report into an Allocation report.\n" +
	    "    clusterNameFormulae: [ 'Cluster.toLower()', 'Cluster.regex(\"k8s-(.*)\")', '\"literal-cluster\"' ]\n" +
	    "  in:\n" +
		"    Cluster: Cluster\n" +
		"    Product: Product\n" +
		"  out:\n" +
		"    Environment: Environment\n" +
		"    K8sNamespace: K8sNamespace\n" +
		"    K8sResource: K8sResource\n" +
		"    K8sType: K8sType\n" +
		"    userTag1: userTag1\n" +
		"    userTag2: userTag2\n" +
	    "  tagMaps:\n" +
	    "    Environment:\n" +
		"      maps:\n" + 
		"        Prod:\n" + 
		"          K8sNamespace: [ 're:.*prod.*', 're:.*production.*', 're:.*prd.*' ]\n" +
		"";
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		RuleConfig rc = new RuleConfig();
		rc = mapper.readValue(yaml, rc.getClass());

		assertEquals("Wrong rule name", "kubernetes-breakout", rc.getName());
		assertNull("Should have no operands", rc.getOperands());
		assertEquals("Wrong in operand type", OperandType.cost, rc.getIn().getType());
		assertEquals("Should be no results", null, rc.getResults());
		AllocationConfig ac = rc.getAllocation();
		assertNotNull("Should have an allocation object", ac);
		
		KubernetesConfig kc = ac.getKubernetes();
		assertNotNull("Should have a kubernetes config object", kc);
		assertEquals("Wrong cluster name for literal expression", "\"literal-cluster\"", kc.getClusterNameFormulae().get(2));
	}
	
	@Test
	public void testReportRuleRead() throws JsonParseException, JsonMappingException, IOException {
		String yaml = "" +
		"name: kubernetes-breakout\n" + 
		"start: 2019-11\n" + 
		"end: 2022-11\n" + 
		"reports: [monthly]\n" + 
		"in:\n" + 
		"  type: cost\n" + 
		"  product: Product\n" + 
		"  userTags:\n" + 
		"    Role: compute\n" + 
		"";
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		RuleConfig rc = new RuleConfig();
		rc = mapper.readValue(yaml, rc.getClass());
		
		assertTrue("isReport failing", rc.isReport());
		assertEquals("wrong number of reports", 1, rc.getReports().size());
		assertEquals("wrong report aggregation", RuleConfig.Aggregation.monthly, rc.getReports().get(0));
	}

}
