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

public class ResultConfig {
	private RuleConfig.DataType type;	
	private TagGroupConfig out;
	private String value;
	private boolean single;
	
	public RuleConfig.DataType getType() {
		return type;
	}

	public void setType(RuleConfig.DataType type) {
		this.type = type;
	}

	public TagGroupConfig getOut() {
		return out;
	}

	public void setOut(TagGroupConfig out) {
		this.out = out;
	}

	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	public boolean isSingle() {
		return single;
	}
	
	public void setSingle(boolean single) {
		this.single = single;
	}
}
