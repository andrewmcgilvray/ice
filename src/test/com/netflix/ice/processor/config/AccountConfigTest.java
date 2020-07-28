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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.junit.Test;

import com.amazonaws.services.organizations.model.Account;
import com.amazonaws.services.organizations.model.AccountJoinedMethod;
import com.amazonaws.services.organizations.model.Tag;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AccountConfigTest {

	@Test
	public void testContructor() {
		List<String> customTags = Lists.newArrayList("Tag1");
		
		Map<String, String> accountTags = Maps.newHashMap();
		accountTags.put("Tag1", "tag1value");
		accountTags.put("IceName", "act1");
		accountTags.put("IceRole", "ice");
		accountTags.put("IceRiProducts", "ec2+rds");
		accountTags.put("IceExternalId", "12345");
		
		List<com.amazonaws.services.organizations.model.Tag> tags = Lists.newArrayList();
		for (Entry<String, String> e: accountTags.entrySet()) {
			Tag tag = new Tag();
			tag.setKey(e.getKey());
			tag.setValue(e.getValue());
			tags.add(tag);
		}
		
		List<String> parents = Lists.newArrayList();
		parents.add("org1");
		Account a = new Account()
			.withId("123456789012")
			.withName("account1")
			.withStatus("ACTIVE")
			.withJoinedMethod(AccountJoinedMethod.CREATED)
			.withJoinedTimestamp(new Date(new DateTime("2020-01-05").getMillis()));
		
		AccountConfig account = new AccountConfig(a, parents, tags, customTags);
		
		assertEquals("Wrong account id", "123456789012", account.id);
		assertEquals("Wrong account name", "act1", account.name);
		assertEquals("Wrong account awsName", "account1", account.awsName);
		assertEquals("Wrong number of account tags", 1, account.tags.size());
		assertTrue("Map doesn't have tag", account.tags.containsKey("Tag1"));
		String tag = account.tags.get("Tag1");
		assertEquals("Wrong tag value", "tag1value", tag);
		assertEquals("Wrong number of RI Products", 2, account.riProducts.size());
		assertEquals("Wrong RI Product", "ec2", account.riProducts.get(0));
		assertEquals("Wrong role", "ice", account.role);
		assertEquals("Wrong externalId", "12345", account.externalId);
		
		String s = account.toString();
		assertEquals("Wrong string form", "id: 123456789012, name: act1, awsName: account1, parents: org1, status: ACTIVE, joinedMethod: CREATED, joinedDate: 2020-01-05, riProducts: [ec2, rds], role: ice, externalId: 12345, tags: {Tag1: tag1value}", s);
	}

}
