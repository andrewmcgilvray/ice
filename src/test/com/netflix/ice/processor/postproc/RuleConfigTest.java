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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
		"  - result:\n" + 
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
		OperandConfig out = rc.getResults().get(0).getResult();
		assertEquals("Wrong product in result", "ComputedCost", out.getProduct());
		assertEquals("Wrong usageType in result", "${group}-Requests", out.getUsageType());
		assertEquals("Wrong out function", "(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000", rc.getResults().get(0).getValue());
	}

}
