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
package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.tag.Account;

public class BasicAccountServiceTest {

	@Test
	public void testAccountConfigMapConstructor() {
		Map<String, AccountConfig> configs = Maps.newHashMap();
		
		configs.put("123456789012", new AccountConfig("123456789012", "account1", "account 1", Lists.newArrayList("Org"), "ACTIVE", Lists.newArrayList("ec2"), "role", "12345"));
		configs.put("234567890123", new AccountConfig("234567890123", "account2", "account 2", null, null, null, null, null));
		BasicAccountService bas = new BasicAccountService(configs);
		
		assertEquals("Wrong number of accounts", 2, bas.getAccounts().size());
		assertEquals("Wrong name for account1 by ID", "account1", bas.getAccountById("123456789012").getIceName());
		assertEquals("Wrong id for account1 by name", "123456789012", bas.getAccountByName("account1").getId());
		assertEquals("Wrong number of accounts with reserved instances", 1, bas.getReservationAccounts().size());
		assertEquals("Wrong number of reserved instance products", 1, bas.getReservationAccounts().values().iterator().next().size());
		assertEquals("Wrong number of account parents", 1, bas.getAccountById("123456789012").getParents().size());
		assertEquals("Wrong root name for account parent", "Org", bas.getAccountById("123456789012").getParents().get(0));
	}
	
	@Test
	public void testAccountListConstructor() {
		Account a = new Account("123456789012", "account1", null);
		BasicAccountService bas = new BasicAccountService(Lists.newArrayList(a));
		
		assertEquals("Wrong name for account1 by ID", "account1", bas.getAccountById("123456789012").getIceName());
		assertEquals("Wrong id for account1 by name", "123456789012", bas.getAccountByName("account1").getId());
	}
	
	@Test
	public void testUpdateAccounts() {
		List<Account> accounts = Lists.newArrayList();
		String id = "123456789012";
		accounts.add(new Account(id, "OldName", "OldAwsName", "OldEmail", null, "ACTIVE", null, null, null));
		
		BasicAccountService bas = new BasicAccountService(accounts);
		
		assertEquals("Wrong number of accounts before update", 1, bas.getAccounts().size());
		
		Account origAccount = bas.getAccountById(id);
		assertNotNull("Missing account before update fetch by ID", origAccount);
		
		assertNotNull("Missing account before update fetch by Name", bas.getAccountByName("OldName"));
		assertEquals("Wrong account name before update", "OldName", bas.getAccountById(id).getIceName());
		assertEquals("Wrong account id before update", id, bas.getAccountById(id).getId());
		assertEquals("Wrong email before update", "OldEmail", bas.getAccountById(id).getEmail());
		assertEquals("Wrong status", "ACTIVE", bas.getAccountById(id).getStatus());
		
		accounts = Lists.newArrayList();	
		List<String> parents = Lists.newArrayList("OrgRoot");
		Map<String, String> tags = Maps.newHashMap();
		tags.put("Environment", "Production");
		Account newAccount = new Account(id, "NewName", "NewAwsName", "NewEmail", parents, "SUSPENDED", null, null, tags);
		accounts.add(newAccount);
		
		bas.updateAccounts(accounts);
		assertEquals("Wrong number of accounts after update", 1, bas.getAccounts().size());
		assertEquals("Account object not the same", origAccount, bas.getAccountById(id));
		assertNotNull("Missing account after update fetch by ID", bas.getAccountById(id));

		// Check that the old name is no longer in the accounts map
		assertFalse("Account still available by old name", bas.hasAccountByIceName("OldName"));
		
		// Make sure lookup by new name doesn't create a new account
		bas.getAccountByName("NewName");
		assertEquals("Lookup by new name created additional account", 1, bas.getAccounts().size());
		
		assertEquals("Wrong account name after update", "NewName", bas.getAccountById(id).getIceName());
		assertEquals("Wrong account id after update", id, bas.getAccountById(id).getId());
		assertEquals("Wrong account email after update", "NewEmail", bas.getAccountById(id).getEmail());
		assertEquals("Wrong parent", parents, bas.getAccountById(id).getParents());
		assertEquals("Wrong status", "SUSPENDED", bas.getAccountById(id).getStatus());
		assertEquals("Wrong tags", tags, bas.getAccountById(id).getTags());
		
		// Remove separate ICE name
		newAccount = new Account(id, "NewAwsName", "NewAwsName", "NewEmail", parents, "SUSPENDED", null, null, tags);
		accounts = Lists.newArrayList();	
		accounts.add(newAccount);
		bas.updateAccounts(accounts);
		// Check that the old name is no longer in the accounts map
		assertFalse("Account still available by old name", bas.hasAccountByIceName("NewName"));
		assertEquals("Wrong number of accounts after update", 1, bas.getAccounts().size());
		assertEquals("Account object not the same", origAccount, bas.getAccountById(id));
		
	}
	
	@Test
	public void testGetAccountById() {
		Map<String, AccountConfig> configs = Maps.newHashMap();
		BasicAccountService bas = new BasicAccountService(configs);
		
		assertEquals("Wrong parent for unlinked account", BasicAccountService.unlinkedAccountParents, bas.getAccountById("123456789012", "").getParents());
	}

	@Test
	public void testValues() {
		com.amazonaws.services.organizations.model.Account account = new com.amazonaws.services.organizations.model.Account()
			.withId("123456789012")
			.withName("account")
			.withStatus("ACTIVE");
		
		// Three tags: Tag1 with single value, Tag2 with initial value and updated with new effective date, Tag3 clearing tag.
		// Tag3 is out of time order to make sure we're not assuming time ordered effective dates.
		List<com.amazonaws.services.organizations.model.Tag> tags = Lists.newArrayList();
		tags.add(new com.amazonaws.services.organizations.model.Tag().withKey("Tag1").withValue("foobar"));
		tags.add(new com.amazonaws.services.organizations.model.Tag().withKey("Tag2").withValue("foo/2020-04=bar"));
		tags.add(new com.amazonaws.services.organizations.model.Tag().withKey("Tag3").withValue("2020-06=/foo/2020-04=bar"));
		
		List<String> customTags = Lists.newArrayList(new String[]{"Tag1", "Tag2", "Tag3"});
		Map<String, AccountConfig> configs = Maps.newHashMap();
		
		configs.put("123456789012", new AccountConfig(account, Lists.newArrayList("Org"), tags, customTags));
		BasicAccountService bas = new BasicAccountService(configs);
		
		// Get the headers
		Account a = bas.getAccountById("123456789012");
		int tagStartIndex = Account.headerWithoutTags().length;
		
		// Check for returning full tag values
		List<String> values = a.values(customTags, false);
		assertEquals("wrong tag1 full value", "foobar", values.get(tagStartIndex));
		assertEquals("wrong tag2 full value", "foo/2020-04=bar", values.get(tagStartIndex+1));
		assertEquals("wrong tag3 full value", "2020-06=/foo/2020-04=bar", values.get(tagStartIndex+2));
		
		// Check for returning only effective tag values
		values = bas.getAccountById("123456789012").values(customTags, true);
		assertEquals("wrong tag1 effective value", "foobar", values.get(tagStartIndex));
		assertEquals("wrong tag2 effective value", "bar", values.get(tagStartIndex+1));
		assertEquals("wrong tag3 effective value", "", values.get(tagStartIndex+2));
	}
}
