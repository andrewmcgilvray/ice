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
package com.netflix.ice.processor.config;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.TagConfig;
import com.netflix.ice.common.TagMappingTerm;

public class TagConfigTest {

	@Test
	public void testConstructor() {
		String name = "Env";
		List<TagConfig.KeyAlias> aliases = Lists.newArrayList(new TagConfig.KeyAlias("Environment", null, null));
		List<String> displayAliases = Lists.newArrayList(new String[]{"Alias"});
		Map<String, List<String>> values = Maps.newHashMap();
		List<String> prodAliases = Lists.newArrayList(new String[]{"production"});
		values.put("Prod", prodAliases);
		
		
		TagConfig tc = new TagConfig(name, aliases, displayAliases, values);
		assertEquals("wrong tag name", "Env", tc.name);
		assertEquals("wrong number of aliases", 1, tc.aliases.size());
		assertEquals("wrong alias", "Environment", tc.aliases.get(0).name);
		assertEquals("wrong number of displayAliases", 1, tc.displayAliases.size());
		assertEquals("wrong displayAlias", "Alias", tc.displayAliases.get(0));
		assertEquals("wrong number of values", 1, tc.values.size());
		assertTrue("wrong value name", tc.values.containsKey("Prod"));
		assertEquals("wrong number of value aliases", 1, tc.values.get("Prod").size());
		assertEquals("wrong value alias", "production", tc.values.get("Prod").get(0));
	}

	@Test
	public void testDeserializeFromYaml() throws JsonParseException, JsonMappingException, IOException {
		String yaml = "" +
		"name: Env\n" +
		"aliases:\n" +
		"  - name: Environment\n" +
		"values:\n" +
		"  Prod: [production]\n" +
		"mapped:\n" +
		"- name: foobar\n" +
		"  owners: [foo.bar@company.com,a.b@co.com]\n" +
		"  maps:\n" +
		"    QA:\n" +
		"      key: Application\n" +
		"      values: [test-web-server,'']\n" +
		"      operator: isOneOf\n" +
		"  include: [123456789012]\n";

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		TagConfig tc = new TagConfig();
		tc = mapper.readValue(yaml, tc.getClass());		
		
		assertEquals("wrong tag name", "Env", tc.name);
		assertEquals("wrong number of aliases", 1, tc.aliases.size());
		assertEquals("wrong alias", "Environment", tc.aliases.get(0).name);
		assertEquals("wrong number of values", 1, tc.values.size());
		assertTrue("wrong value name", tc.values.containsKey("Prod"));
		assertEquals("wrong number of value aliases", 1, tc.values.get("Prod").size());
		assertEquals("wrong value alias", "production", tc.values.get("Prod").get(0));
		assertEquals("wrong number of computed values", 1, tc.mapped.size());
		
		Map<String, TagMappingTerm> maps = tc.mapped.get(0).maps;
		assertTrue("wrong computed value name", maps.containsKey("QA"));
		TagMappingTerm term = maps.get("QA");
		assertNotNull("no term", term);
		assertTrue("wrong term key", term.getKey().equals("Application"));
		assertEquals("wrong number of term values", 2, term.getValues().size());
		assertEquals("wrong term value", "test-web-server", term.getValues().get(0));
		assertEquals("wrong term value", "", term.getValues().get(1));
		
		List<String> include = tc.mapped.get(0).include;
		assertTrue("No include map", include != null);
		assertEquals("Incorrect include accounts list size", 1, include.size());
		assertEquals("Incorrect include account", "123456789012", include.get(0));
	}
}
