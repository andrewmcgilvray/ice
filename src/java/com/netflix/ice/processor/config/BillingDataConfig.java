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
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;
import com.netflix.ice.common.TagConfig;
import com.netflix.ice.processor.postproc.RuleConfig;

/*
 *  BillingDataConfig loads and holds AWS account name and default tagging configuration data
 *  that is specified in S3 along side the billing data reports.
 *
 *  Example BillingDataConfig JSON:
 *

{
	"accounts": [
		{
			"id": "123456789012",
			"name": "act1",
			"parents": [ "root", "ou" ],
			"tags": {
				"TagName": "tag-value"
			},
			"riProducts": [ "ec2", "rds" ],
			"role": "ice",
			"externalId": ""
		}
	],
	"tags":[
		{
			"name": "Environment",
			"aliases": [ "env" ],
			"values": {
				"Prod": [ "production", "prd" ]
			}
		}
	]
}
  
 *
 *  Example BillingDataConfig YAML:
 *  

accounts:
  -  id: 123456789012
	 name: act1
	 parents: [root, ou]
	 tags:
	   TagName: tag-value
	 riProducts: [ec2, rds]
	 role: ice
	 externalId:
	 
tags:
  - name: Environment
    aliases: [env]
    values:
      Prod: [production, prd]
      
postprocrules:
  - name: ComputedCost
    product: Product
    start: 2019-11
    end: 2022-11
    operands: 
      data:
        type: usage
		usageType: ${group}-DataTransfer-Out-Bytes
    in:
      type: usage
      product: Product
      usageType: (..)-Requests-[12].*
    results: # Directly compute results using operands (only one of allocation or results may be present)
      - result:
          type: cost
          product: ComputedCost
          usageType: ${group}-Requests
          value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'
      - result:
          type: usage
          product: ComputedCost
          usageType: ${group}-Requests
          value: '${in} - (${data} * 4 * 8 / 2)'
     allocation: # Perform allocations provided through an allocation report (only one of allocation or results may be present)
       s3Bucket:
         name: <S3 bucket name>
         region: <S3 bucket region>
         prefix: <S3 bucket prefix>
         accountId: <account that owns the bucket>
         accessRole: <role to use when accessing bucket if cross-account>
         externalId: <external ID>
       kubernetes: # Preprocess a Kubernetes report into an Allocation report.
         clusterNameFormulae: [Cluster]
         out:
           Namespace: <tag key to assign namespace>
       type: cost
       in:
		 <Map of input tags to column names>
	   out:
	     <Map of output tags to column names>
 *  
 */
public class BillingDataConfig {
	public List<AccountConfig> accounts;
    public List<TagConfig> tags;
    public List<RuleConfig> postprocrules;

    public BillingDataConfig() {    	
    }
    
	public BillingDataConfig(List<AccountConfig> accounts, List<TagConfig> tags, List<RuleConfig> ruleConfigs) {
		this.accounts = accounts;
		this.tags = tags;
		this.postprocrules = ruleConfigs;
	}

	public BillingDataConfig(String in) throws JsonParseException, JsonMappingException, IOException {
		BillingDataConfig config = null;
		
		if (in.trim().startsWith("{")) {
			Gson gson = new Gson();
			config = gson.fromJson(in, getClass());
		}
		else {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			config = mapper.readValue(in, getClass());			
		}
		this.accounts = config.accounts;
		this.tags = config.tags;
		this.postprocrules = config.postprocrules;
	}
	
	public String toJSON() {
		Gson gson = new Gson();
    	return gson.toJson(this);
	}
	
	public List<AccountConfig> getAccounts() {
		return accounts;
	}

	public void setAccounts(List<AccountConfig> accounts) {
		this.accounts = accounts;
	}

	public List<TagConfig> getTags() {
		return tags;
	}

	public void setTags(List<TagConfig> tags) {
		this.tags = tags;
	}

	public List<RuleConfig> getPostprocrules() {
		return postprocrules;
	}

	public void setPostprocrules(List<RuleConfig> postprocrules) {
		this.postprocrules = postprocrules;
	}
	
}
