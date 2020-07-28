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

<%@ page contentType="text/html;charset=UTF-8" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Accounts</title>
</head>
<body>
<div class="" style="margin: auto; width: 1600px; padding: 20px 30px" ng-controller="accountsCtrl">
  <h1>Accounts  	
	    <span class="resourcesButtons">
	      <input ng-model="filter_accounts" type="text" class="resourcesFilter" placeholder="Filter"/>&nbsp;
	      <a href="javascript:void(0)" style="background-image: url(${resource(dir: '/')}images/tango/16/actions/document-save.png)"
	       ng-click="download()"
	       ng-disabled="accounts.length == 0">Download</a>
	      <img src="${resource(dir: '/')}images/gear.png" ng-click="settings()"/>   
	    </span>
  </h1>
  <div ng-show="showSettings" class="modal">
    <table class="settingsModal">
      <tr><td><h1>Show/Hide Columns</h1></td></tr>
      <tr ng-repeat="col in header">
        <td><input type="checkbox" ng-model="showColumn[$index]"> {{col}}</input></td>
      </tr>
      <tr><td><button type="submit" class="processorStatusButton" ng-click="close()">Close</button></td></tr>
    </table>
  </div>
  <div class="list">
	  <table style="width: 100%;">
	    <thead>
	      <tr>
	        <th ng-repeat="col in header" ng-show="showColumn[$index]" ng-click="order($index)">{{col}}</th>
	      </tr>
	    </thead>
	    <tbody>
	      <tr ng-repeat="row in accounts | filter:filter_accounts" class="{{getTrClass($index)}}">
	        <td ng-repeat="col in row" ng-show="showColumn[$index]">{{col}}</td>
	      </tr>
	    </tbody>
	  </table>
  </div>
</div>
</body>
</html>