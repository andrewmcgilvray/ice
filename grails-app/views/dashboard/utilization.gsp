<%--

    Copyright 2013 Netflix, Inc.
    Copyright 2016 TiVo, Inc.

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

<%@ page contentType="text/html;charset=UTF-8" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Aws Instance Utilization</title>
</head>
<body>
<div class="" style="margin: auto; width: 1600px; padding: 20px 30px" ng-controller="utilizationCtrl">
    <div ng-show="!graphOnly()" class="metaUtilization">
      <div class="metaStart">
        <div class="metaTime">
          <div>Start</div>
          <div><input class="required" type="text" name="start" id="start" size="15" style="width: 102px" /></div>
          <div>End</div>
          <div><input class="required" type="text" name="end" id="end" size="15" style="width: 102px" /></div>
        </div>
      </div>
      <div class="metaShow" nowrap="">
        <div>Show</div>
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
        <td>
          <div>
            <a href="javascript:void(0)" class="legendControls" ng-click="showall()">SHOW ALL</a>
            <a href="javascript:void(0)" class="legendControls" ng-click="hideall()">HIDE ALL</a>
            <input ng-model="filter_legend" type="text" class="metaFilter" placeHolder="filter" style="float: right; margin-right: 0">
          </div>
          <div class="list">
            <table style="width: 100%;">
              <thead>
              <tr>
                <th ng-click="order(legends, 'name', false)">{{legendName}}</th>
                <th ng-click="order(legends, 'elasticity', true)">% Elasticity</th>
                <th ng-click="order(legends, 'avgDailyMax', true)">AvgDailyMax</th>
                <th ng-click="order(legends, 'avgDailyMin', true)">AvgDailyMin</th>
              </tr>
              </thead>
              <tbody>
              <tr ng-repeat="legend in legends | filter:filter_legend" style="{{legend.style}}; cursor: pointer;" ng-click="clickitem(legend)" class="{{getTrClass($index)}}">
                <td style="word-wrap: break-word"><div class="legendIcon" style="{{legend.iconStyle}}"></div>{{legend.name}}</td>
                <td>{{legend.stats.elasticity | number:legendPrecision}}</td>
                <td>{{legend.stats.avgDailyMax | number:legendPrecision}}</td>
                <td>{{legend.stats.avgDailyMin | number:legendPrecision}}</td>
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