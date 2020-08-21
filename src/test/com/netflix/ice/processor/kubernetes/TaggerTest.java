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
package com.netflix.ice.processor.kubernetes;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.config.KubernetesNamespaceMapping;

public class TaggerTest {
	private ResourceService rs;
	private ProductService ps;
	private String[] customTags = new String[]{"Tag1", "Tag2", "Tag3"};
	
	class TestKubernetesReport extends KubernetesReport {
		// Force report columns to be "Cluster", "Namespace", "Tag3"
		public TestKubernetesReport(DateTime month, KubernetesConfig config) {
			super(null, config, month, rs);
		}
		
		@Override
		public String getString(String[] item, KubernetesColumn col) {
			if (col != KubernetesColumn.Namespace)
				throw new IllegalArgumentException("expected Namespace, got " + col);
			return item[1];
		}
		
		@Override
		public String getUserTag(String[] item, String col) {
			if (!col.equals("Tag3"))
				throw new IllegalArgumentException("expected Tag3, got " + col);
			return item[2];
		}
	}
	
	@Before
	public void init() {
		ps = new BasicProductService();
		rs = new BasicResourceService(ps, customTags, false);
	}


	@Test
	public void testTagger() throws IOException {
		KubernetesConfig config = new KubernetesConfig();
		config.setTags(Lists.newArrayList(customTags));
		config.setComputeTag("Tag1");
		TestKubernetesReport tkr = new TestKubernetesReport(new DateTime("2019-01", DateTimeZone.UTC), config);
		
		List<String> tagsToCopy = Lists.newArrayList("Tag3");
		List<KubernetesNamespaceMapping> rules = Lists.newArrayList();
		rules.add(new KubernetesNamespaceMapping("Tag2", "Foo", Lists.newArrayList("bar")));
		rules.add(new KubernetesNamespaceMapping("Tag1", "Bar", Lists.newArrayList(".*bar.*")));
		Tagger t = new Tagger(tagsToCopy, rules);
		List<String> tagKeys = t.getTagKeys();
		
		// 0 - Cluster  Tag1
		// 1 - Namespace  Tag2
		// 2 - Tag3
		String[] item = new String[]{ "dev-usw2a", "bar", "foobar" };
		List<String> userTags = t.getTagValues(tkr, item);
		assertEquals("Incorrect tagged value", "Bar", userTags.get(tagKeys.indexOf("Tag1")));
		assertEquals("Incorrect tagged value", "Foo", userTags.get(tagKeys.indexOf("Tag2")));
		assertEquals("Tag3 not copied", "foobar", userTags.get(tagKeys.indexOf("Tag3")));
		
		item = new String[]{ "dev-usw2a", "inAbar", "" };
		userTags = t.getTagValues(tkr, item);
		assertEquals("Incorrect tagged value", "Bar", userTags.get(tagKeys.indexOf("Tag1")));
		assertEquals("Wrong tag changed", "", userTags.get(tagKeys.indexOf("Tag2")));
		assertEquals("Tag3 was not copied correctly", "", userTags.get(tagKeys.indexOf("Tag3")));
		
		item = new String[]{ "dev-usw2a", "inAbar", "useMe" };
		userTags = t.getTagValues(tkr, item);
		assertEquals("Tag3 was incorrectly copied", "useMe", userTags.get(tagKeys.indexOf("Tag3")));
	}
	
	@Test
	public void test() {
		String foo = "foo";
		StringBuilder sb = new StringBuilder();
		sb.append("foo");
		assertEquals("strings don't match", foo, sb.toString());
		assertFalse("string objects match", foo == sb.toString());
		List<String> list = Lists.newArrayList();
		list.add(foo);
		assertTrue("not in list", list.contains(sb.toString()));
	}
}
