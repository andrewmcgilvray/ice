<%--

    Copyright 2013 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

--%>

<%@ page import="com.netflix.ice.reader.ReaderConfig" %>

<%@ page contentType="text/html;charset=UTF-8" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Aws Usage Detail</title>
</head>
<body>
<div class="" style="margin: auto; {{getBodyWidth('width: 1652px;')}} padding: 20px 30px"  ng-controller="detailCtrl">
  <table ng-show="!graphOnly()">
    <tr>
      <td class="metaStart">Start</td>
      <td>Show</td>
      <td class="metaAccounts">
      	<input type="checkbox" ng-model="dimensions[ACCOUNT_INDEX]" ng-change="accountsEnabled()"> Account</input>
      	<select ng-model="organizationalUnit" ng-show="dimensions[ACCOUNT_INDEX]" ng-options="org for org in organizationalUnits" ng-change="orgUnitChanged()">
      		<option value="">All</option>
      	</select>
      </td>
      <td class="metaRegions"><input type="checkbox" ng-model="dimensions[REGION_INDEX]" ng-change="regionsEnabled()"> Region</input></td>
      <td class="metaProducts"><input type="checkbox" ng-model="dimensions[PRODUCT_INDEX]" ng-change="productsEnabled()"> Product</input></td>
      <td class="metaOperations"><input type="checkbox" ng-model="dimensions[OPERATION_INDEX]" ng-change="operationsEnabled()"> Operation</input></td>
      <td class="metaUsageTypes"><input type="checkbox" ng-model="dimensions[USAGETYPE_INDEX]" ng-change="usageTypesEnabled()"> UsageType</input></td>
    </tr>
    <tr>
      <td>
        <input class="required" type="text" name="start" id="start" size="14"/>
        <div style="padding-top: 10px">End</div>
        <br><input class="required" type="text" name="end" id="end" size="14"/>
        <div  ng-show="usage_cost=='cost'" style="padding-top: 10px">CostType</div>
        <div ng-show="usage_cost=='cost'" stype="padding-top: 10px">
          <table class="costTypeTable">
            <tr>
        	  <td class="left"><input type="checkbox" ng-model="recurring" ng-change="includeChanged()"> Recurring Fees</input></td>
		      <td class="right"><input type="checkbox" ng-model="credit" ng-change="includeChanged()"> Credits</input></td>    
            </tr>
		      <td class="left"><input type="checkbox" ng-model="amortized" ng-change="includeChanged()"> Amortization</input></td>        
		      <td class="right"><input type="checkbox" ng-model="tax" ng-change="includeChanged()"> Taxes</input></td>        
            <tr>
            </tr>
          </table>
        </div>
      </td>
      <td nowrap="">
        <input type="radio" ng-model="usage_cost" value="cost" id="radio_cost"> <label for="radio_cost" style="cursor: pointer">Cost</label>&nbsp;&nbsp;
        <input type="radio" ng-model="usage_cost" value="usage" id="radio_usage"> <label for="radio_usage" style="cursor: pointer">Usage</label>
        <select ng-show="usage_cost=='usage'" ng-model="usageUnit">
          <option>Instances</option>
          <option>ECUs</option>
          <option>vCPUs</option>
          <option>Normalized</option>
        </select>
        <div style="padding-top: 10px">Group by
          <select ng-model="groupBy" ng-options="a.name for a in groupBys"></select>
          <input type="checkbox" ng-model="consolidateGroups" style=""> Consolidate</input>
        </div>
        <div style="padding-top: 5px">Aggregate
          <select ng-model="consolidate">
          	<g:if test="${ReaderConfig.getInstance().hourlyData}">
            	<option>hourly</option>
            </g:if>
            <option>daily</option>
            <option>weekly</option>
            <option>monthly</option>
          </select>
        </div>
        <div style="padding-top: 5px">Plot type
          <select ng-model="plotType">
            <option>area</option>
            <option>column</option>
          </select>
        </div>
        <div style="padding-top: 5px">RI/SP Sharing
          <select ng-model="reservationSharing" ng-change="reservationSharingChanged()">
            <option>borrowed</option>
            <option>lent</option>
          </select>
        </div>
        <div style="padding-top: 5px" ng-show="throughput_metricname">
          <input type="checkbox" ng-model="showsps" id="showsps">
          <label for="showsps">Show {{throughput_metricname}}</label>
        </div>
        <div style="padding-top: 5px" ng-show="throughput_metricname">
          <input type="checkbox" ng-model="factorsps" id="factorsps">
          <label for="factorsps">Factor {{throughput_metricname}}</label>
        </div>
      </td>
      <td>
      	<div ng-show="dimensions[ACCOUNT_INDEX]">
	        <select ng-model="selected_accounts" ng-options="a.name for a in accounts | filter:filterAccount(filter_accounts)" ng-change="accountsChanged()" multiple="multiple" class="metaAccounts metaSelect"></select>
	        <br><input ng-model="filter_accounts" type="text" class="metaFilter" placeholder="filter">
	        <button ng-click="selected_accounts = accounts; accountsChanged()" class="allNoneButton">+</button>
	        <button ng-click="selected_accounts = []; accountsChanged()" class="allNoneButton">-</button>
      	</div>
      </td>
      <td>
      	<div ng-show="dimensions[REGION_INDEX]">
	        <select ng-model="selected_regions" ng-options="a.name for a in regions | filter:filter_regions" ng-change="regionsChanged()" multiple="multiple" class="metaRegions metaSelect"></select>
	        <br><input ng-model="filter_regions" type="text" class="metaFilter" placeholder="filter">
	        <button ng-click="selected_regions = regions; regionsChanged()" class="allNoneButton">+</button>
	        <button ng-click="selected_regions = []; regionsChanged()" class="allNoneButton">-</button>
      	</div>
      </td>
      <td>
      	<div ng-show="dimensions[PRODUCT_INDEX]">
	        <select ng-model="selected_products" ng-options="a.name for a in products | filter:filter_products" ng-change="productsChanged()" multiple="multiple" class="metaProducts metaSelect"></select>
	        <br><input ng-model="filter_products" type="text" class="metaFilter" placeholder="filter">
	        <button ng-click="selected_products = products; productsChanged()" class="allNoneButton">+</button>
	        <button ng-click="selected_products = []; productsChanged()" class="allNoneButton">-</button>
      	</div>
      </td>
      <td>
      	<div ng-show="dimensions[OPERATION_INDEX]">
	        <select ng-model="selected_operations" ng-options="a.name for a in operations | filter:filter_operations" ng-change="operationsChanged()" multiple="multiple" class="metaOperations metaSelect"></select>
	        <br><input ng-model="filter_operations" type="text" class="metaFilter" placeholder="filter">
	        <button ng-click="selected_operations = operations; operationsChanged()" class="allNoneButton">+</button>
	        <button ng-click="selected_operations = []; operationsChanged()" class="allNoneButton">-</button>
      	</div>
      </td>
      <td>
      	<div ng-show="dimensions[USAGETYPE_INDEX]">
	        <select ng-model="selected_usageTypes" ng-options="a.name for a in usageTypes | filter:filter_usageTypes" multiple="multiple" class="metaUsageTypes metaSelect"></select>
	        <br><input ng-model="filter_usageTypes" type="text" class="metaFilter" placeholder="filter">
	        <button ng-click="selected_usageTypes = usageTypes" class="allNoneButton">+</button>
	        <button ng-click="selected_usageTypes = []" class="allNoneButton">-</button>
      	</div>
      </td>
    </tr>
  </table>
  <table ng-show="showUserTags" class="userTags">
    <tr ng-show="userTagValues.length > 0">
      <td>
   	    Tags:
        <div ng-show="isGroupByTag()" style="padding-top: 10px">Group by
          <select ng-model="groupByTag" ng-options="a.name for a in groupByTags"></select>
        </div>      
      </td>
      <td ng-repeat="tagValue in getUserTagValues(0, 6)">
        <input type="checkbox" ng-model="enabledUserTags[$index]" ng-change="userTagsChanged($index)"> {{userTags[$index].name}}</input>
      	<div ng-show="enabledUserTags[$index]">
          <select ng-model="selected_userTagValues[$index]" ng-options="a.name for a in userTagValues[$index] | filter:filter_userTagValues[$index]" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[$index]" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_userTagValues[$index] = userTagValues[$index]" class="allNoneButton">+</button>
          <button ng-click="selected_userTagValues[$index] = []" class="allNoneButton">-</button>
		</div>      
      </td>
    </tr>
    <tr ng-show="userTagValues.length > 6">
      <td></td>
      <td ng-repeat="tagValue in getUserTagValues(6, 6)">
        <input type="checkbox" ng-model="enabledUserTags[6+$index]" ng-change="userTagsChanged(6+$index)"> {{userTags[6+$index].name}}</input>
      	<div ng-show="enabledUserTags[6+$index]">
          <select ng-model="selected_userTagValues[6+$index]" ng-options="a.name for a in userTagValues[6+$index] | filter:filter_userTagValues[6+$index]" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[6+$index]" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_userTagValues[6+$index] = userTagValues[6+$index]" class="allNoneButton">+</button>
          <button ng-click="selected_userTagValues[6+$index] = []" class="allNoneButton">-</button>
		</div>      
      </td>
    </tr>
    <tr ng-show="userTagValues.length > 12">
      <td></td>
      <td ng-repeat="tagValue in getUserTagValues(12, 6)">
        <input type="checkbox" ng-model="enabledUserTags[12+$index]" ng-change="userTagsChanged(12+$index)"> {{userTags[12+$index].name}}</input>
      	<div ng-show="enabledUserTags[12+$index]">
          <select ng-model="selected_userTagValues[12+$index]" ng-options="a.name for a in userTagValues[12+$index] | filter:filter_userTagValues[12+$index]" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[12+$index]" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_userTagValues[12+$index] = userTagValues[12+$index]" class="allNoneButton">+</button>
          <button ng-click="selected_userTagValues[12+$index] = []" class="allNoneButton">-</button>
		</div>      
      </td>
    </tr>
  </table>

  <div class="buttons" ng-show="!graphOnly()">
    <img src="${resource(dir: '/')}images/spinner.gif" ng-show="loading">
    <a href="javascript:void(0)" class="monitor" style="background-image: url(${resource(dir: '/')}images/tango/16/apps/utilities-system-monitor.png)"
       ng-click="updateUrl(); getData()" ng-show="!loading"
       ng-disabled="selected_accounts.length == 0 || selected_regions.length == 0 || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Submit</a>
    <a href="javascript:void(0)" style="background-image: url(${resource(dir: '/')}images/tango/16/actions/document-save.png)"
       ng-click="download()" ng-show="!loading"
       ng-disabled="selected_accounts.length == 0 || selected_regions.length == 0 || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Download</a>
   	<span ng-show="errorMessage">&nbsp;&nbsp;<img src="${resource(dir: '/')}images/error.png" style="position: relative; top: 5px"/>&nbsp;&nbsp;{{errorMessage}}</span>
  </div>

  <table style="width: 100%; margin-top: 20px">
    <tr>
      <td ng-show="!graphOnly()">

        <div class="list">
          <div>
            <a href="javascript:void(0)" class="legendControls" ng-click="showall()">SHOW ALL</a>
            <a href="javascript:void(0)" class="legendControls" ng-click="hideall()">HIDE ALL</a>
            <input ng-model="filter_legend" type="text" class="metaFilter" placeHolder="filter" style="float: right; margin-right: 0">
          </div>
          <table style="width: 100%;">
            <thead>
            <tr>
              <th ng-click="order(legends, 'name', false)"><div class="legendIcon" style="{{legend.iconStyle}}"></div>{{legendName}}</th>
              <th ng-click="order(legends, 'total', true)">Total</th>
              <th ng-click="order(legends, 'max', true)">Max</th>
              <th ng-click="order(legends, 'average', true)">Average</th>
            </tr>
            </thead>
            <tbody>
            <tr ng-repeat="legend in legends | filter:filter_legend" style="{{legend.style}}; cursor: pointer;" ng-click="clickitem(legend)" class="{{getTrClass($index)}}">
              <td style="word-wrap: break-word">
                <div class="legendIcon" style="{{legend.iconStyle}}"></div>
                {{legend.name}}
              </td>
              <td><span ng-show="legend_usage_cost == 'cost'">{{currencySign}} </span>{{legend.stats.total | number:2}}</td>
              <td><span ng-show="legend_usage_cost == 'cost'">{{currencySign}} </span>{{legend.stats.max | number:2}}</td>
              <td><span ng-show="legend_usage_cost == 'cost'">{{currencySign}} </span>{{legend.stats.average | number:2}}</td>
            </tr>
            </tbody>
          </table>
        </div>
      </td>
      <td style="width: 65%">
        <div id="highchart_container" style="width: 100%; height: 600px;">
        </div>
      </td>
    </tr>
  </table>

</div>
</body>
</html>