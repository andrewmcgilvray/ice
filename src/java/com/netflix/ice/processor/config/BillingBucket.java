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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;

public class BillingBucket extends S3BucketConfig {
    private String rootName;
    private String configAccountsBasename;
    private String configTagsBasename;
    private String configPostProcBasename;
    
    public BillingBucket() {
    	this.rootName = "";
    	this.configAccountsBasename = "";
    	this.configTagsBasename = "";
    	this.configPostProcBasename = "";
    }
    
    @Override
    public BillingBucket withName(String name) {
    	return (BillingBucket) super.withName(name);
    }
        
    @Override
    public BillingBucket withRegion(String region) {
    	return (BillingBucket) super.withRegion(region);
    }
        
    @Override
    public BillingBucket withPrefix(String prefix) {
    	return (BillingBucket) super.withPrefix(prefix);
    }
        
    @Override
    public BillingBucket withAccountId(String accountId) {
    	return (BillingBucket) super.withAccountId(accountId);
    }
        
    @Override
    public BillingBucket withAccessRole(String accessRole) {
    	return (BillingBucket) super.withAccessRole(accessRole);
    }
        
    @Override
    public BillingBucket withExternalId(String externalId) {
    	return (BillingBucket) super.withExternalId(externalId);
    }
        
    public BillingBucket withRootName(String rootName) {
    	this.rootName = rootName;
    	return this;
    }
    
    public BillingBucket withConfigAccountsBasename(String configBasename) {
    	this.configAccountsBasename = configBasename;
    	return this;
    }
    
    public BillingBucket withConfigTagsBasename(String configBasename) {
    	this.configTagsBasename = configBasename;
    	return this;
    }
    
    public BillingBucket withConfigPostProcBasename(String configBasename) {
    	this.configPostProcBasename = configBasename;
    	return this;
    }
    
    /**
     * Constructor for deserializing from JSON or YAML
     * 
     * @param in String to parse.
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public BillingBucket(String in) throws JsonParseException, JsonMappingException, IOException {
    	BillingBucket bb = null;
    	
		if (in.trim().startsWith("{")) {
			Gson gson = new Gson();
			bb = gson.fromJson(in, getClass());
		}
		else {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			bb = mapper.readValue(in, getClass());			
		}
    	setName(bb.getName());
    	setRegion(bb.getRegion());
    	setPrefix(bb.getPrefix());
    	setAccountId(bb.getAccountId());
    	setExternalId(bb.getExternalId());
    	this.rootName = bb.rootName;
    	this.configAccountsBasename = bb.configAccountsBasename;
    	this.configTagsBasename = bb.configTagsBasename;
    	this.configPostProcBasename = bb.configPostProcBasename;
    }
    
    public String getRootName() {
    	return rootName;
    }
    
    public String getConfigTagsBasename() {
    	return configTagsBasename;
    }
    public String getConfigAccountsBasename() {
    	return configAccountsBasename;
    }
    public String getConfigPostProcBasename() {
    	return configPostProcBasename;
    }
}