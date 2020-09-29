/*
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
package com.netflix.ice.processor.config;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class BillingDataConfigTest {

	@Test
	public void testDeserializeFromYaml() throws JsonParseException, JsonMappingException, IOException {
		// Test the primary nodes.
		String yaml = "" +
		"accounts:\n" + 
		"  - id: 123456789012\n" + 
		"    name: act1\n" + 
		"    parents: [root, ou]\n" + 
		"    tags:\n" + 
		"      TagName: tag-value\n" + 
		"    riProducts: [ec2, rds]\n" + 
		"    role: ice\n" + 
		"    externalId:\n" + 
		"    \n" + 
		"tags:\n" + 
		"  - name: Environment\n" + 
		"    aliases: [env]\n" + 
		"    values:\n" + 
		"      Prod: [production, prd]\n" + 
		"      \n" + 
		"postprocrules:\n" + 
		"  - name: ComputedCost\n" + 
		"    start: 2019-11\n" + 
		"    end: 2022-11\n" + 
		"    operands:\n" + 
		"      data:\n" + 
		"        type: usage\n" + 
		"        usageType: ${group}-DataTransfer-Out-Bytes\n" + 
		"    in:\n" + 
		"      type: usage\n" + 
		"      product: Product\n" + 
		"      usageType: (..)-Requests-[12].*\n" + 
		"    results:\n" + 
		"      - out:\n" + 
		"          type: cost\n" + 
		"          product: ComputedCost\n" + 
		"          usageType: ${group}-Requests\n" + 
		"        value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'\n" + 
		"      - out:\n" + 
		"          type: usage\n" + 
		"          product: ComputedCost\n" + 
		"          usageType: ${group}-Requests\n" + 
		"        value: '${in} - (${data} * 4 * 8 / 2)'\n" +
		"  - name: kubernetes-breakout\n" + 
		"    start: 2019-11\n" + 
		"    end: 2022-11\n" + 
		"    in:\n" + 
		"      type: cost\n" + 
		"      product: Product\n" + 
		"      userTags:\n" + 
		"        Role: compute\n" + 
	    "    allocation: # Perform allocations provided through an allocation report (only one of allocation or results may be present)\n" +
	    "      s3Bucket:\n" +
	    "        name: k8s-report-bucket\n" +
	    "        region: us-east-1\n" +
	    "        prefix: hourly/kubernetes\n" +
	    "        accountId: 123456789012\n" +
	    "        accessRole: ice-role\n" +
	    "        externalId: 234567890123\n" +
	    "      kubernetes: # preprocess a Kubernetes report into an Allocation report.\n" +
	    "        clusterNameFormulae: [ 'Cluster.toLower()', 'Cluster.regex(\"k8s-(.*)\")' ]\n" +
		"        out:\n" +
		"          Namespace: K8sNamespace\n" +
		"          Resource: K8sResource\n" +
		"          Type: K8sType\n" +
	    "      in:\n" +
		"        Cluster: Cluster\n" +
		"        _Product: _Product\n" +
		"      out:\n" +
		"        Environment: Environment\n" +
		"        K8sNamespace: K8sNamespace\n" +
		"        K8sResource: K8sResource\n" +
		"        K8sType: K8sType\n" +
		"        userTag1: userTag1\n" +
		"        userTag2: userTag2\n" +
	    "      tagMaps:\n" +
	    "        Environment:\n" +
	    "          maps:\n" +
		"            Prod:\n" + 
		"              Namespace: [ \".*prod.*\", \".*production.*\", \".*prd.*\" ]\n" + 
		"";
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		BillingDataConfig bdc = new BillingDataConfig();
		bdc = mapper.readValue(yaml, bdc.getClass());
		
		assertEquals("Wrong number of accounts", 1, bdc.getAccounts().size());
		assertEquals("Wrong number of tags", 1, bdc.getTags().size());
		assertEquals("Wrong number of postprocrules", 2, bdc.getPostprocrules().size());
	}

}
