<%@ page contentType="text/html;charset=UTF-8" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Aws Tag Coverage</title>
</head>
<body>
<div class="" style="margin: auto; width: 1600px; padding: 20px 30px" ng-controller="tagCoverageCtrl">

  <table>
    <tr>
      <td>Start</td>
      <td>Show</td>
      <td>TagKey</td>
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
        <input class="required" type="text" name="start" id="start" size="15" style="width: 102px"/>
        <div style="padding-top: 10px">End</div>
        <br><input class="required" type="text" name="end" id="end" size="15" style="width: 102px"/>
      </td>
      <td nowrap="">
        <div style="padding-top: 10px">Group by
          <select ng-model="groupBy" ng-options="a.name for a in groupBys"></select>
        </div>
        <div style="padding-top: 5px">Aggregate
          <select ng-model="consolidate">
            <option>daily</option>
            <option>weekly</option>
            <option>monthly</option>
          </select>
        </div>
      </td>
      <td ng-show="isGroupByTagKey()">
        <select ng-model="selected_tagKeys" ng-options="a.name for a in tagKeys | filter:filter_tagKeys" multiple="multiple" class="metaTags metaSelect"></select>
        <br><input ng-model="filter_tagKeys" type="text" class="metaFilter" placeholder="filter">
        <button ng-click="selected_tagKeys = tagKeys" class="allNoneButton">+</button>
        <button ng-click="selected_tagKeys = []" class="allNoneButton">-</button>
      </td>
      <td ng-show="!isGroupByTagKey()">
      	<div  style="padding-top: 10px">
        	<select ng-model="selected_tagKey" ng-options="a.name for a in tagKeys"></select>
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
    <g:set var="numRows" value="${(ReaderConfig.getInstance().userTagKeys.size() + 5) / 6}"/>
    <g:set var="rowIndex" value="${0}"/>
    <g:while test="${rowIndex < numRows}">
      <g:set var="rowStartIndex" value="${rowIndex * 6}"/>
      <tr ng-show="userTagValues.length > 0">
        <td>
          <g:if test="${rowIndex == 0}">
   	        Tags:
            <div ng-show="isGroupByTag()" style="padding-top: 10px">Group by
              <select ng-model="groupByTag" ng-options="a.name for a in groupByTags"></select>
            </div>      
          </g:if>
        </td>
        <td ng-repeat="tagValue in getUserTagValues(${rowStartIndex}, 6)">
          <input type="checkbox" ng-model="enabledUserTags[${rowStartIndex}+$index]" ng-change="userTagsChanged(${rowStartIndex}+$index)"> {{getUserTagDisplayName(${rowStartIndex}+$index)}}</input>
      	  <div ng-show="enabledUserTags[${rowStartIndex}+$index]">
            <select ng-model="selected_userTagValues[${rowStartIndex}+$index]" ng-options="a.name for a in userTagValues[${rowStartIndex}+$index] | filter:filter_userTagValues[${rowStartIndex}+$index]" multiple="multiple" class="metaUserTags metaSelect"></select>
            <br><input ng-model="filter_userTagValues[${rowStartIndex}+$index]" type="text" class="metaFilter" placeholder="filter">
            <button ng-click="selected_userTagValues[${rowStartIndex}+$index] = userTagValues[${rowStartIndex}+$index]" class="allNoneButton">+</button>
            <button ng-click="selected_userTagValues[${rowStartIndex}+$index] = []" class="allNoneButton">-</button>
		  </div>      
        </td>
      </tr>
      <g:set var="rowIndex" value="${rowIndex+1}"/>
    </g:while>
  </table>

  <div class="buttons">
    <img src="${resource(dir: '/')}images/spinner.gif" ng-show="loading">
    <a href="javascript:void(0)" class="monitor" style="background-image: url(${resource(dir: '/')}images/tango/16/apps/utilities-system-monitor.png)"
       ng-click="updateUrl(); getData()" ng-show="!loading"
       ng-disabled="selected_tagKeys.length == 0 || selected_accounts.length == 0 || selected_regions.length == 0 && !showZones || selected_zones.length == 0 && showZones || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Submit</a>
    <a href="javascript:void(0)" style="background-image: url(${resource(dir: '/')}images/tango/16/actions/document-save.png)"
       ng-click="download()" ng-show="!loading"
       ng-disabled="selected_tagKeys.length == 0 || selected_accounts.length == 0 || selected_regions.length == 0 && !showZones || selected_zones.length == 0 && showZones || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Download</a>
   	<span ng-show="errorMessage">&nbsp;&nbsp;<img src="${resource(dir: '/')}images/error.png" style="position: relative; top: 5px"/>&nbsp;&nbsp;{{errorMessage}}</span>
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
              <th ng-click="order(legends, 'max', true)">Max</th>
              <th ng-click="order(legends, 'avgerage', true)">Avg</th>
              <th ng-click="order(legends, 'min', true)">Min</th>
            </tr>
            </thead>
            <tbody>
            <tr ng-repeat="legend in legends | filter:filter_legend" style="{{legend.style}}; cursor: pointer;" ng-click="clickitem(legend)" class="{{getTrClass($index)}}">
              <td style="word-wrap: break-word"><div class="legendIcon" style="{{legend.iconStyle}}"></div>{{legend.name}}</td>
              <td>{{legend.stats.max | number:legendPrecision}}%</td>
              <td>{{legend.stats.average | number:legendPrecision}}%</td>
              <td>{{legend.stats.min | number:legendPrecision}}%</td>
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