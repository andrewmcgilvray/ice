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
  <title>Aws Savings Plans</title>
</head>
<body>
  <div class="" style="margin: auto; width: 1600px; padding: 20px 30px" ng-controller="savingsPlansCtrl">
    <div ng-show="!graphOnly()" class="meta">
      <div class="metaStart">
        <div class="metaTime">
          <div>Start</div>
          <div><input class="required" type="text" name="start" id="start" size="15" style="width: 102px" /></div>
          <div>End</div>
          <div><input class="required" type="text" name="end" id="end" size="15" style="width: 102px" /></div>
          <div class="aggregate">
            Aggregate&nbsp;&nbsp;
            <select ng-model="consolidate">
              <option>hourly</option>
              <option>daily</option>
              <option>weekly</option>
              <option>monthly</option>
            </select>
          </div>
        </div>
        <div ng-show="usage_cost=='cost'" class="metaCostTypeHeader">CostType</div>
        <div ng-show="usage_cost=='cost'" class="metaCostType">
          <div><input type="checkbox" ng-model="recurring" ng-change="costTypesChanged()"> Recurring</div>
          <div><input type="checkbox" ng-model="credit" ng-change="costTypesChanged()"> Credit</div>
          <div><input type="checkbox" ng-model="amortization" ng-change="costTypesChanged()"> Amortization</div>
          <div><input type="checkbox" ng-model="savings" ng-change="costTypesChanged()"> Savings</div>
        </div>
      </div>
      <div class="metaShow" nowrap="">
        <div>Show</div>
        <div>
          <input type="radio" ng-model="usage_cost" value="cost" id="radio_cost" ng-change="usageCostChanged()">
          <label for="radio_cost" style="cursor: pointer">Cost</label>&nbsp;&nbsp;
          <input type="radio" ng-model="usage_cost" value="usage" id="radio_usage" ng-change="usageCostChanged()">
          <label for="radio_usage" style="cursor: pointer">Usage</label>
        </div>
        <div ng-show="usage_cost!='cost'">Usage Units
          <select ng-show="usage_cost=='usage'" ng-model="usageUnit">
            <option>Instances</option>
            <option>ECUs</option>
            <option>vCPUs</option>
            <option>Normalized</option>
          </select>
        </div>
        <div>Group by
          <select ng-model="groupBy" ng-options="a.name for a in groupBys"></select>
          <div ng-show="groupBy.name=='Operation' || groupBy.name=='UsageType'">
            <input type="checkbox" ng-model="consolidateGroups"> Consolidate
          </div>
        </div>
        <div>RI/SP Sharing
          <select ng-model="reservationSharing" ng-change="reservationSharingChanged()">
            <option>borrowed</option>
            <option>lent</option>
          </select>
        </div>
      </div>
      <div class="metaAccounts">
        <input type="checkbox" ng-model="dimensions[ACCOUNT_INDEX]" ng-change="accountsEnabled()"> Account
        <div ng-show="dimensions[ACCOUNT_INDEX]" class="metaTag">
          <div class="metaSelect">
            <select ng-model="organizationalUnit" ng-show="dimensions[ACCOUNT_INDEX]" 
                    ng-options="org for org in organizationalUnits" ng-change="orgUnitChanged()" class="orgSelect">
              <option value="">All</option>
            </select>
            <br>
            <select ng-model="selected_accounts" ng-options="a.name for a in accounts | filter:filterAccount(filter_accounts)"
                     ng-change="accountsChanged()" multiple="multiple" class="accountSelect"></select>
          </div>
          <input ng-model="filter_accounts" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_accounts = accounts; accountsChanged()" class="allButton">+</button>
          <button ng-click="selected_accounts = []; accountsChanged()" class="noneButton">-</button>
        </div>
      </div>
      <div class="metaRegions">
        <input type="checkbox" ng-model="dimensions[REGION_INDEX]" ng-change="regionsEnabled()"> Region
        <div ng-show="dimensions[REGION_INDEX]" class="metaTag">
          <select ng-model="selected_regions" ng-options="a.name for a in regions | filter:filter_regions"
                  ng-change="regionsChanged()" multiple="multiple" class="metaSelect"></select>
          <input ng-model="filter_regions" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_regions = regions; regionsChanged()" class="allButton">+</button>
          <button ng-click="selected_regions = []; regionsChanged()" class="noneButton">-</button>
        </div>
      </div>
      <div class="metaProducts">
        <input type="checkbox" ng-model="dimensions[PRODUCT_INDEX]" ng-change="productsEnabled()"> Product
        <div ng-show="dimensions[PRODUCT_INDEX]" class="metaTag">
          <select ng-model="selected_products" ng-options="a.name for a in products | filter:filter_products"
                  ng-change="productsChanged()" multiple="multiple" class="metaSelect"></select>
          <input ng-model="filter_products" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_products = products; productsChanged()" class="allButton">+</button>
          <button ng-click="selected_products = []; productsChanged()" class="noneButton">-</button>
        </div>
      </div>
      <div class="metaOperations">
        <input type="checkbox" ng-model="dimensions[OPERATION_INDEX]" ng-change="operationsEnabled()"> Operation
        <div ng-show="dimensions[OPERATION_INDEX]" class="metaTag">
          <select ng-model="selected_operations" ng-options="a.name for a in operations | filter:filter_operations"
                  ng-change="operationsChanged()" multiple="multiple" class="metaSelect"></select>
          <input ng-model="filter_operations" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_operations = operations; operationsChanged()" class="allButton">+</button>
          <button ng-click="selected_operations = []; operationsChanged()" class="noneButton">-</button>
        </div>
      </div>
      <div class="metaUsageTypes">
        <input type="checkbox" ng-model="dimensions[USAGETYPE_INDEX]" ng-change="usageTypesEnabled()"> UsageType
        <div ng-show="dimensions[USAGETYPE_INDEX]" class="metaTag">
          <select ng-model="selected_usageTypes" ng-options="a.name for a in usageTypes | filter:filter_usageTypes"
                  multiple="multiple" class="metaSelect"></select>
          <input ng-model="filter_usageTypes" type="text" class="metaFilter" placeholder="filter">
          <button ng-click="selected_usageTypes = usageTypes" class="allButton">+</button>
          <button ng-click="selected_usageTypes = []" class="noneButton">-</button>
        </div>
      </div>
    </div>
    <div ng-show="!graphOnly()" class="metaUserTags">
      <div class="metaUserTagsHeader">
        <div><input type="checkbox" ng-model="showUserTags" ng-change="userTagsEnabled()"> Tags</div>
        <div ng-show="isGroupByTag()">Group by</div>
        <div ng-show="isGroupByTag()"><select ng-model="groupByTag" ng-options="a.name for a in groupByTags"></select></div>
      </div>
      <div ng-show="showUserTags" class="metaUserTagsGrid">
        <g:set var="numKeys" value="${ReaderConfig.getInstance().userTagKeys.size()}"/>
        <g:set var="index" value="${0}"/>
        <g:while test="${index < numKeys}">
          <div>
            <input type="checkbox" ng-model="enabledUserTags[${index}]" ng-change="userTagsChanged(${index})">
                    {{getUserTagDisplayName(${index})}}
            <div ng-show="enabledUserTags[${index}]" class="metaTag">
              <select ng-model="selected_userTagValues[${index}]"
                      ng-options="a.name for a in userTagValues[${index}] | filter:filter_userTagValues[${index}]"
                      multiple="multiple" class="metaSelect">
              </select>
              <br>
              <input ng-model="filter_userTagValues[${index}]" type="text" class="metaFilter" placeholder="filter">
              <button ng-click="selected_userTagValues[${index}] = userTagValues[${index}]" class="allButton">+</button>
              <button ng-click="selected_userTagValues[${index}] = []" class="noneButton">-</button>
            </div>
          </div>
          <g:set var="index" value="${index+1}"/>
        </g:while>
      </div>
    </div>

    <div class="buttons" ng-show="!graphOnly()">
      <img src="${resource(dir: '/')}images/spinner.gif" ng-show="loading">
      <a href="javascript:void(0)" class="monitor"
        style="background-image: url(${resource(dir: '/')}images/tango/16/apps/utilities-system-monitor.png)"
        ng-click="updateUrl(); getData()" ng-show="!loading"
        ng-disabled="selected_accounts.length == 0 || selected_regions.length == 0 || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Submit</a>
      <a href="javascript:void(0)"
        style="background-image: url(${resource(dir: '/')}images/tango/16/actions/document-save.png)"
        ng-click="download()" ng-show="!loading"
        ng-disabled="selected_accounts.length == 0 || selected_regions.length == 0 || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Download</a>
      <span ng-show="errorMessage">&nbsp;&nbsp;
        <img src="${resource(dir: '/')}images/error.png" style="position: relative; top: 5px" />
        &nbsp;&nbsp;{{errorMessage}}
      </span>
    </div>

    <table style="width: 100%; margin-top: 20px">
      <tr>
        <td ng-show="!graphOnly()">
          <div class="list">
            <div>
              <a href="javascript:void(0)" class="legendControls" ng-click="showall()">SHOW ALL</a>
              <a href="javascript:void(0)" class="legendControls" ng-click="hideall()">HIDE ALL</a>
              <span style="padding-left: 100px">
                <input type="radio" ng-model="plotType" value="area" id="radio_area" ng-change="plotTypeChanged()">
                <label for="radio_cost" style="cursor: pointer">Area</label>&nbsp;&nbsp;
                <input type="radio" ng-model="plotType" value="column" id="radio_column" ng-change="plotTypeChanged()">
                <label for="radio_column" style="cursor: pointer">Column</label>
              </span>
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
                  <td style="word-wrap: break-word"><div class="legendIcon" style="{{legend.iconStyle}}"></div> {{legend.name}}</td>
                  <td><span ng-show="legend_usage_cost == 'cost'">{{currencySign}}</span>{{legend.stats.total | number:2}}</td>
                  <td><span ng-show="legend_usage_cost == 'cost'">{{currencySign}}</span>{{legend.stats.max | number:2}}</td>
                  <td><span ng-show="legend_usage_cost == 'cost'">{{currencySign}}</span>{{legend.stats.average | number:2}}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </td>
        <td style="width: 65%">
          <div id="highchart_container" style="width: 100%; height: 600px;"></div>
        </td>
      </tr>
    </table>

  </div>
</body>
</html>