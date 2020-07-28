/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ice.basic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.tag.Account;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BasicAccountService implements AccountService {

    Logger logger = LoggerFactory.getLogger(getClass());
    
    public static final String unlinkedAccountOrgUnitName = "Unlinked";
    public static final List<String> unlinkedAccountParents = Lists.newArrayList(unlinkedAccountOrgUnitName);

    private Map<String, Account> accountsById = Maps.newConcurrentMap();
    private Map<String, Account> accountsByIceName = Maps.newConcurrentMap();
    private Map<Account, Set<String>> reservationAccounts = Maps.newHashMap();
    private Map<Account, String> reservationAccessRoles = Maps.newHashMap();
    private Map<Account, String> reservationAccessExternalIds = Maps.newHashMap();
    
    // Constructor used by the processor
    public BasicAccountService(Map<String, AccountConfig> configs) {
    	for (AccountConfig a: configs.values()) {
    		String iceName = StringUtils.isEmpty(a.name) ? a.awsName : a.name;
			Account account = new Account(a.id, iceName, a.awsName, a.email, a.parents, a.status, a.accessGroups, a.tags);
			accountsByIceName.put(iceName, account);
			accountsById.put(a.id, account);
			if (a.riProducts != null && a.riProducts.size() > 0) {
				reservationAccounts.put(account, Sets.newHashSet(a.riProducts));
			}
			if (!StringUtils.isEmpty(a.role)) {
				reservationAccessRoles.put(account,  a.role);
			}
			if (!StringUtils.isEmpty(a.externalId)) {
				reservationAccessExternalIds.put(account, a.externalId);
			}
    	}
    }

    // Constructor used by the reader - initialized from the work bucket data config
    // Also used by unit test code.
    public BasicAccountService(List<Account> accounts) {
    	for (Account a: accounts) {
    		a.initDefaultTags();
    		accountsById.put(a.getId(), a);
    		accountsByIceName.put(a.getIceName(), a);
    	}
    	// Reservation maps are not used by the reader.
    }
    
    // Used by test code
    public BasicAccountService() {}
    
    // testing methods
    protected boolean hasAccountByIceName(String name) {
    	return accountsByIceName.containsKey(name);
    }
    protected boolean hasAccountById(String name) {
    	return accountsById.containsKey(name);
    }
    
    // Accounts for the reader are refreshed from the work bucket data configuration after each processor run
    public void updateAccounts(List<Account> accounts) {
     	// Run through the account list and update our maps
    	for (Account a: accounts) {
    		Account existing = accountsById.get(a.getId());
    		if (existing == null) {
    			// Add the new account
    			a.initDefaultTags();
    			accountsById.put(a.getId(), a);
    			accountsByIceName.put(a.getIceName(), a);
    		}
    		else {
    			// Remove the account by old name
    			accountsByIceName.remove(existing.getIceName());
    			// Update account organization info
    			existing.update(a);
    			accountsByIceName.put(a.getIceName(), a);
    		}
    	}
    }
    
    public Account getAccountById(String accountId) {
    	Account account = accountsById.get(accountId);
    	if (account == null) {
    		logger.error("getAccountById() unregistered account: " + accountId);
    		account = getAccountById(accountId, "");
    	}
    	return account;
    }

    public Account getAccountById(String accountId, String root) {
        Account account = accountsById.get(accountId);
        if (account == null) {
        	// We get here when the billing data has an account that is no longer active in any of the payer accounts
        	String[] parents = StringUtils.isEmpty(root) ? new String[]{ unlinkedAccountOrgUnitName } : new String[]{ root, unlinkedAccountOrgUnitName };
            account = new Account(accountId, accountId, Lists.newArrayList(parents));
            accountsByIceName.put(account.getIceName(), account);
            accountsById.put(account.getId(), account);
            logger.info("getAccountById() created account " + accountId + "=\"" + account.getIceName() + "\".");
        }
        return account;
    }
    
    public Account getAccountByName(String accountName) {
        Account account = accountsByIceName.get(accountName);
        // for accounts that were not mapped to names in ice.properties (ice.account.xxx), this check will make sure that
        // data/tags are updated properly once the mapping is established in ice.properties
        if (account == null) {
            account = accountsById.get(accountName);
        }
        if (account == null) {
            account = new Account(accountName, accountName, unlinkedAccountParents);
            accountsByIceName.put(account.getIceName(), account);
            accountsById.put(account.getId(), account);
            logger.info("getAccountByName() created account " + accountName + ".");
        }
        return account;
    }

    public List<Account> getAccounts() {
        List<Account> result = Lists.newArrayList();
        // Accounts can have the same name, so be sure to get the list using the ID map
        for (Account a: accountsById.values())
            result.add(a);
        return result;
    }

    public List<Account> getAccounts(List<String> accountNames) {
        List<Account> result = Lists.newArrayList();
        for (String name: accountNames)
            result.add(accountsByIceName.get(name));
        return result;
    }

    public Map<Account, Set<String>> getReservationAccounts() {
        return reservationAccounts;
    }

    public Map<Account, String> getReservationAccessRoles() {
        return reservationAccessRoles;
    }


    public Map<Account, String> getReservationAccessExternalIds() {
        return reservationAccessExternalIds;
    }
    
    /**
     * Helper function for report generation
     */
	public String getAccountsReport() throws IOException {
		// Get the tagKeys
		Set<String> tagKeys = Sets.newHashSet();
		for (Account a: accountsById.values()) {
			tagKeys.addAll(a.getTags().keySet());			
		}
		Set<String> sortedTagKeys = Sets.newTreeSet(tagKeys);
		
		// Build the header
		List<String> names = Lists.newArrayList(Account.headerWithoutTags());
		for (String key: sortedTagKeys)
			names.add(key);

		StringWriter writer = new StringWriter(1024);
		CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader((String[]) names.toArray(new String[names.size()])));
		
		// generate report in account ID order
		Set<String> ids = Sets.newTreeSet(accountsById.keySet());		
		for (String id: ids) {			
			printer.printRecord(accountsById.get(id).values(sortedTagKeys, true));
		}
		printer.close(true);
		return writer.toString();
	}

}
