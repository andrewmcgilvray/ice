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

var ice = angular.module('ice', ["ui"], function ($locationProvider) {
  $locationProvider.html5Mode(true);
});

ice.value('ui.config', {
  select2: {
  }
});

ice.factory('highchart', function () {
  var metricname = throughput_metricname;
  var metricunitname = throughput_metricunitname;
  var factoredCostCurrencySign = throughput_factoredCostCurrencySign;

  var hc_chart, consolidate = "hour", currencySign = global_currencySign, legends, showsps = false, factorsps = false;
  var hc_options = {
    chart: {
      renderTo: 'highchart_container',
      zoomType: 'x',
      spacingRight: 5,
      plotBorderWidth: 1
    },
    title: {
      style: { fontSize: '15px' }
    },
    xAxis: {
      type: 'datetime'
    },
    yAxis: {
      text: 'Cost Per Hour'
    },
    legend: {
      enabled: true
    },
    rangeSelector: {
      inputEnabled: false,
      enabled: false
    },
    series: [],
    credits: {
      enabled: false
    },
    plotOptions: {
      area: { lineWidth: 1, stacking: 'normal' },
      column: { lineWidth: 1, stacking: 'normal' },
      series: {
        states: {
          hover: {
            lineWidth: 2
          }
        },
        events: {
          //          mouseOver: function(event) {
          //            var i;
          //            for (i = 0; i < $scope.data.legend.length; i++) {
          //              $scope.data.legend[i].fontWeight = "font-weight: normal;";
          //            }
          //            $scope.data.legend[parseInt(this.name)].fontWeight = "font-weight: bold;";
          //            $scope.$apply();
          //          }
        }
      }
    },
    tooltip: {
      shared: true,
      formatter: function () {
        var s = '<span style="font-size: x-small;">' + Highcharts.dateFormat('%A, %b %e, %l%P, %Y', this.x) + '</span>';

        var total = 0;
        for (var i = 0; i < this.points.length - (showsps ? 1 : 0); i++) {
          total += this.points[i].y;
        }

        var precision = currencySign === "" ? 0 : (currencySign === "¢" ? 4 : 2);
        for (var i = 0; i < this.points.length - (showsps ? 1 : 0); i++) {
          var point = this.points[i];
          if (i == 0) {
            s += '<br/><span>aggregated : ' + currencySign + Highcharts.numberFormat(total, precision, '.') + ' / ' + (factorsps ? metricunitname : consolidate);
          }
          var perc = showsps ? point.y * 100 / total : point.percentage;
          s += '<br/><span style="color: ' + point.series.color + '">' + point.series.name + '</span> : ' + currencySign + Highcharts.numberFormat(point.y, precision, '.') + ' / ' + (factorsps ? metricunitname : consolidate) + ' (' + Highcharts.numberFormat(perc, 1) + '%)';
          if (i > 25 && point) {
            // total up the rest and label as 'other'
            var otherTotal = 0;
            for (var j = i + 1; j < this.points.length - (showsps ? 1 : 0); j++) {
              otherTotal += this.points[j].y;
            }
            s += '<br/><span>other : ' + currencySign + Highcharts.numberFormat(otherTotal, precision, '.') + ' / ' + (factorsps ? metricunitname : consolidate) + ' (' + Highcharts.numberFormat(perc, 1) + '%)';
            break;
          }
        }

        return s;
      }
    }
  };

  var setupHcData = function (result, plotType, showsps, zeroDataValid) {

    Highcharts.setOptions({
      global: {
        useUTC: true
      }
    });

    hc_options.series = [];
    var i, j;
    for (i in result.data) {
      var data = result.data[i].data;
      var hasData = false;
      if (zeroDataValid) {
        // Don't filter out series with all zeros
        hasData = true;
      }
      else {
        for (j in data) {
          data[j] = parseFloat(data[j].toFixed(2));
          if (data[j] !== 0)
            hasData = true;
        }
      }

      if (hasData) {
        if (!result.interval && result.time) {
          for (j in data) {
            data[j] = [result.time[j], data[j]];
          }
        }
        var name = result.data[i].name;
        var serie = {
          name: name,
          data: data,
          pointStart: result.start,
          pointInterval: result.interval,
          //step: true,
          type: plotType
        };
        if (name.startsWith("Credit"))
          serie['stack'] = 'credit';

        hc_options.series.push(serie);
      }
    };

    if (showsps && result.sps && result.sps.length > 0) {
      var serie = {
        name: metricname,
        data: result.sps,
        pointStart: result.start,
        pointInterval: result.interval,
        //step: true,
        type: plotType,
        yAxis: 1
      };
      hc_options.series.push(serie);
    }
  }

  var setupYAxis = function (isCost, usageUnit, showsps, factorsps, elasticity, tagCoverage) {
    var unitStr = usageUnit === '' ? '' : ' (' + usageUnit + ')';
    var unitType = "";
    if (elasticity)
      unitType = '% Elasticity';
    else if (tagCoverage)
      unitType = '% Tag Coverage';
    else
      unitType = isCost ? 'Cost' : 'Usage' + unitStr;
    var yAxis = { title: { text: unitType + ' per ' + (factorsps ? metricunitname : consolidate) }, lineWidth: 2 };
    if (isCost)
      yAxis.labels = {
        formatter: function () {
          return currencySign + this.value;
        }
      }
    hc_options.yAxis = [yAxis];

    if (showsps) {
      hc_options.yAxis.push({ title: { text: metricname }, height: 100, min: 0, lineWidth: 2, offset: 0 });
      hc_options.yAxis[0].top = 150;
      hc_options.yAxis[0].height = 350;
    }
  }

  return {
    dateFormat: function (time) {
      //y-MM-dd hha
      //return Highcharts.dateFormat('%A, %b %e, %l%P, %Y', this.x);
      return Highcharts.dateFormat('%Y-%m-%d %I%p', time);
    },

    monthFormat: function (time) {
      return Highcharts.dateFormat('%B', time);
    },

    dayFormat: function (time) {
      return Highcharts.dateFormat('%Y-%m-%d', time);
    },

    drawGraph: function (result, $scope, legendEnabled, elasticity, tagCoverage) {
      consolidate = $scope.consolidate === 'daily' ? 'day' : $scope.consolidate.substr(0, $scope.consolidate.length - 2);
      currencySign = $scope.usage_cost === 'cost' ? ($scope.factorsps ? factoredCostCurrencySign : global_currencySign) : "";
      hc_options.legend.enabled = legendEnabled;

      setupHcData(result, $scope.plotType, $scope.showsps, elasticity || tagCoverage);
      setupYAxis($scope.usage_cost === 'cost', $scope.usageUnit, $scope.showsps, $scope.factorsps, elasticity, tagCoverage);
      showsps = $scope.showsps;
      factorsps = $scope.factorsps;

      hc_chart = new Highcharts.StockChart(hc_options, function (chart) {
        if ($scope && $scope.legends) {
          var legend = { name: "aggregated" };
          if ($scope.stats && $scope.stats.aggregated)
            legend.stats = $scope.stats.aggregated;
          $scope.legends.push(legend);
        }
        var i = 0;
        for (i = 0; i < chart.series.length - ($scope.showsps ? 2 : 1); i++) {
          if ($scope && $scope.legends) {
            var legend = {
              name: chart.series[i].name,
              style: "color: " + chart.series[i].color,
              iconStyle: "background-color: " + chart.series[i].color,
              color: chart.series[i].color
            }
            if ($scope.stats && $scope.stats[chart.series[i].name])
              legend.stats = $scope.stats[chart.series[i].name];
            $scope.legends.push(legend);
          }
        }

        if ($scope) {
          legends = $scope.legends;
        }

        var xextemes = chart.xAxis[0].getExtremes();
        Highcharts.addEvent(chart.container, 'dblclick', function (e) {
          chart.xAxis[0].setExtremes(xextemes.min, xextemes.max);
        });
      });
    },

    clickitem: function (legend) {
      if (legend.name === "aggregated")
        return;

      var series;
      for (var index = 0; index < hc_chart.series.length; index++) {
        if (hc_chart.series[index].name === legend.name) {
          series = hc_chart.series[index];
          series.setVisible(!series.visible);
          break;
        }
      }
      legend.style = series.visible ? "color: " + series.color : "color: rgb(192, 192, 192)";
      legend.iconStyle = series.visible ? "background-color: " + series.color : "color: rgb(192, 192, 192)";
    },

    showall: function () {
      for (var index = 0; index < hc_chart.series.length; index++) {
        hc_chart.series[index].setVisible(true, false);
      }
      hc_chart.redraw();
      for (var i in legends) {
        legends[i].style = "color: " + legends[i].color;
        legends[i].iconStyle = "background-color: " + legends[i].color;
      }
    },

    hideall: function () {
      for (var index = 0; index < hc_chart.series.length - 1; index++) {
        if (hc_chart.series[index].yAxis === 0 || hc_chart.series[index].yAxis.options.index === 0)
          hc_chart.series[index].setVisible(false, false);
      }
      hc_chart.redraw();
      for (var i in legends) {
        legends[i].style = "color: rgb(192, 192, 192)";
        legends[i].iconStyle = "background-color: rgb(192, 192, 192)";
      }
    },

    order: function (legends) {
      var newIndex = 0;
      for (var i = 0; i < legends.length; i++) {
        if (legends[i].name === 'aggregated')
          continue;
        for (var index = 0; index < hc_chart.series.length - 1; index++) {
          if (legends[i].name === hc_chart.series[index].name) {
            hc_chart.series[index].update({ index: newIndex }, false);
            newIndex++;
            break;
          }
        }
      }
      hc_chart.redraw();
    }
  }
});

ice.factory('usage_db', function ($window, $http, $filter) {

  var graphonly = false;

  var retrieveNamesIfNotAll = function (array, selected, preselected, filter, organizationalUnit) {
    if (!selected && !preselected)
      return;

    var result = [];
    if (selected) {
      for (var i = 0; i < selected.length; i++) {
        if (filterAccountByOrg(selected[i], organizationalUnit) && filterItem(selected[i].name, filter))
          result.push(selected[i].name);
      }
    }
    else {
      for (var i = 0; i < preselected.length; i++) {
        if (filterItem(preselected[i], filter))
          result.push(preselected[i]);
      }
    }
    return result.join(",");
  }

  var filterItem = function (name, filter) {
    return !filter
          || name.toLowerCase().indexOf(filter.toLowerCase()) >= 0
          || (filter[0] === "!" && name.toLowerCase().indexOf(filter.slice(1).toLowerCase()) < 0);
  }

  var filterAccountByOrg = function(account, organizationalUnit) {
    // Check organizationalUnit -- used for Account filtering
    return !organizationalUnit || account.path.startsWith(organizationalUnit);
  }

  var getSelected = function (from, selected) {
    var result = [];
    for (var i = 0; i < from.length; i++) {
      if (selected.indexOf(from[i].name) >= 0)
        result.push(from[i]);
    }
    return result;
  }

  var updateSelected = function (from, selected) {
    var result = [];
    var selectedArr = [];
    for (var i = 0; i < selected.length; i++)
      selectedArr.push(selected[i].name);
    for (var i = 0; i < from.length; i++) {
      if (selectedArr.indexOf(from[i].name) >= 0)
        result.push(from[i]);
    }

    return result;
  }

  var getOrgs = function (accounts) {
    let set = new Set();
    for (var i = 0; i < accounts.length; i++) {
      var parents = accounts[i].parents;
      if (!parents)
        parents = [];
      var path = [];
      for (var j = 0; j < parents.length; j++) {
        path.push(parents[j]);
        set.add(path.join("/"));
      }
      accounts[i].path = parents.join("/");
    }
    var result = [];
    set.forEach((org) => {
      result.push(org);
    })
    result.sort();
    return result;
  }

  var timeParams = "";

  return {
    graphOnly: function () {
      return graphonly;
    },
    addParams: function (params, name, array, selected, preselected, filter, organizationalUnit) {
      var selected = retrieveNamesIfNotAll(array, selected, preselected, filter, organizationalUnit);
      if (selected)
        params[name] = selected;
    },

    filterSelected: function (selected, filter, organizationalUnit) {
      var result = [];
      for (var i = 0; i < selected.length; i++) {
        if (filterAccountByOrg(selected[i], organizationalUnit) && filterItem(selected[i].name, filter))
          result.push(selected[i]);
      }
      return result;
    },

    filterAccount: function ($scope, filter_accounts) {
      return function(account) {
        return filterAccountByOrg(account, $scope.organizationalUnit) && filterItem(account.name, filter_accounts);
      }
    },
  
    addDimensionParams: function($scope, params) {
      var hasDimension = false;
      for (var i = 0; i < $scope.dimensions.length; i++) {
        if ($scope.dimensions[i]) {
          hasDimension = true;
          break;
        }
      }
      if (!hasDimension)
        return;

      if ($scope.dimensions[$scope.ACCOUNT_INDEX]) {
        if ($scope.organizationalUnit)
          params.orgUnit = $scope.organizationalUnit;
        var accounts = this.filterSelected($scope.selected_accounts, $scope.filter_accounts, $scope.organizationalUnit);
        if (accounts.length > 0)
          params.account = { selected: accounts, filter: $scope.filter_accounts };
      }
      if ($scope.showZones) {
        if ($scope.dimensions[$scope.ZONE_INDEX]) {
          var zones = this.filterSelected($scope.selected_zones, $scope.filter_zones);
          if (zones.length > 0)
            params.zone = { selected: zones, filter: $scope.filter_zones };
        }
      }
      else {
        if ($scope.dimensions[$scope.REGION_INDEX]) {
          var regions = this.filterSelected($scope.selected_regions, $scope.filter_regions);
          if (regions.length > 0)
            params.region = { selected: regions, filter: $scope.filter_regions };
        }
      }
      if ($scope.dimensions[$scope.PRODUCT_INDEX]) {
        var products = this.filterSelected($scope.selected_products, $scope.filter_products);
        if (products.length > 0)
          params.product = { selected: products, filter: $scope.filter_products };
      }
      if ($scope.dimensions[$scope.OPERATION_INDEX]) {
        var operations = this.filterSelected($scope.selected_operations, $scope.filter_operations);
        if (operations.length > 0)
          params.operation = { selected: operations, filter: $scope.filter_operations };
      }
      if ($scope.dimensions[$scope.USAGETYPE_INDEX]) {
        var usageTypes = this.filterSelected($scope.selected_usageTypes, $scope.filter_usageTypes);
        if (usageTypes.length > 0)
          params.usageType = { selected: usageTypes, filter: $scope.filter_usageTypes };
      }

      params.dimensions = $scope.dimensions.join(",");
    },

    addUserTagParams: function ($scope, params) {
      var hasEnabledTags = false;
      for (var i = 0; i < $scope.enabledUserTags.length; i++) {
        if ($scope.enabledUserTags[i]) {
          hasEnabledTags = true;
          break;
        }
      }
      if (!hasEnabledTags)
        return;
      
      params.enabledUserTags = $scope.enabledUserTags.join(",");
      for (var i = 0; i < $scope.userTags.length; i++) {
        if ($scope.enabledUserTags[i]) {
          params["tag-" + $scope.userTags[i].name] = { selected: $scope.selected_userTagValues[i] };
        }
        if ($scope.filter_userTagValues[i])
          params["filter-tag-" + $scope.userTags[i].name] = $scope.filter_userTagValues[i];
      }      
    },

    updateUrl: function ($location, data) {
      var result = "";
      var time = "";
      for (var key in data) {
        if (result)
          result += "&";
        result += key + "=";

        if (typeof data[key] == "string") {
          result += data[key];

          if (key === "start" || key === "end") {
            if (time)
              time += "&";
            time += key + "=" + data[key];
          }
        }
        else if (data[key] != undefined) {
          var selected = data[key].selected;
          for (var i in selected) {
            if (i != 0)
              result += ",";
            result += selected[i].name;
          }
          var filter = data[key].filter;
          if (filter)
            result += "&filter-" + key + "s=" + data[key].filter;
        }
      }

      $location.hash(result);

      if (time) {
        timeParams = time;
      }
    },

    getTimeParams: function () {
      return timeParams;
    },

    getParams: function (hash, $scope) {
      $scope.tagParams = {};
      $scope.tagFilterParams = {};
      if (hash) {
        var params = hash.split("&");
        var i, j, time = "";
        for (i = 0; i < params.length; i++) {

          if (params[i].indexOf("spans=") === 0) {
            $scope.spans = parseInt(params[i].substr(6));
          }
          else if (params[i].indexOf("graphOnly=true") === 0) {
            graphonly = true;
          }
          else if (params[i].indexOf("showUserTags=") === 0) {
            $scope.showUserTags = "true" === params[i].substr(13);
          }
          else if (params[i].indexOf("resources=") === 0) {
            $scope.resources = params[i].substr(10);
          }
          else if (params[i].indexOf("showZones=") === 0) {
            $scope.showZones = "true" === params[i].substr(10);
          }
          else if (params[i].indexOf("showsps=") === 0) {
            $scope.showsps = "true" === params[i].substr(8);
          }
          else if (params[i].indexOf("factorsps=") === 0) {
            $scope.factorsps = "true" === params[i].substr(10);
          }
          else if (params[i].indexOf("plotType=") === 0) {
            $scope.plotType = params[i].substr(9);
          }
          else if (params[i].indexOf("reservationSharing=") === 0) {
            $scope.reservationSharing = params[i].substr(19);
          }
          else if (params[i].indexOf("consolidate=") === 0) {
            $scope.consolidate = params[i].substr(12);
          }
          else if (params[i].indexOf("usage_cost=") === 0) {
            $scope.usage_cost = params[i].substr(11);
          }
          else if (params[i].indexOf("usageUnit=") === 0) {
            $scope.usageUnit = params[i].substr(10);
          }
          else if (params[i].indexOf("start=") === 0) {
            $scope.start = params[i].substr(6);
            if (time)
              time += "&";
            time += "start=" + $scope.start;
          }
          else if (params[i].indexOf("end=") === 0) {
            $scope.end = params[i].substr(4);
            if (time)
              time += "&";
            time += "end=" + $scope.end;
          }
          else if (params[i].indexOf("groupBy=") === 0) {
            var groupBy = params[i].substr(8);
            for (var j in $scope.groupBys) {
              if ($scope.groupBys[j].name === groupBy) {
                $scope.groupBy = $scope.groupBys[j];
                break;
              }
            }
          }
          else if (params[i].indexOf("groupByTag=") === 0) {
            $scope.initialGroupByTag = params[i].substr(11);
          }
          else if (params[i].indexOf("orgUnit=") === 0) {
            $scope.organizationalUnit = params[i].substr(8);
          }
          else if (params[i].indexOf("account=") === 0) {
            $scope.selected__accounts = params[i].substr(8).split(",");
          }
          else if (params[i].indexOf("region=") === 0) {
            $scope.selected__regions = params[i].substr(7).split(",");
          }
          else if (params[i].indexOf("zone=") === 0) {
            $scope.selected__zones = params[i].substr(5).split(",");
          }
          else if (params[i].indexOf("product=") === 0) {
            $scope.selected__products = params[i].substr(8).split(",");
          }
          else if (params[i].indexOf("operation=") === 0) {
            $scope.selected__operations = params[i].substr(10).split(",");
          }
          else if (params[i].indexOf("usageType=") === 0) {
            $scope.selected__usageTypes = params[i].substr(10).split(",");
          }
          else if (params[i].indexOf("consolidateGroups=") === 0) {
            $scope.consolidateGroups = "true" === params[i].substr(18);
          }
          else if (params[i].indexOf("tagKey=") === 0) {
            $scope.selected__tagKeys = params[i].substr(7).split(",");
            $scope.selected__tagKey = $scope.selected__tagKeys.length > 0 ? $scope.selected__tagKeys[0] : null;
          }
          else if (params[i].indexOf("userTags=") === 0) {
            var tags = params[i].substr(9).split(",");
            $scope.userTags = [];
            for (var k = 0; k < tags.length; k++) {
              $scope.userTags[k] = { name: tags[k] };
            }
          }
          else if (params[i].indexOf("dimensions=") === 0) {
            var dims = params[i].substr(11).split(",");
            $scope.dimensions = Array($scope.NUM_DIMENSIONS);
            for (j = 0; j < $scope.NUM_DIMENSIONS; j++)
              $scope.dimensions[j] = "true" === dims[j];
          }
          else if (params[i].indexOf("enabledUserTags=") === 0) {
            var enabled = params[i].substr(16).split(",");
            $scope.enabledUserTags = Array(enabled.length);
            for (j = 0; j < enabled.length; j++)
              $scope.enabledUserTags[j] = "true" === enabled[j];
          }
          else if (params[i].indexOf("filter-accounts=") === 0) {
            $scope.filter_accounts = params[i].substr(16);
          }
          else if (params[i].indexOf("filter-regions=") === 0) {
            $scope.filter_regions = params[i].substr(15);
          }
          else if (params[i].indexOf("filter-zones=") === 0) {
            $scope.filter_zones = params[i].substr(13);
          }
          else if (params[i].indexOf("filter-products=") === 0) {
            $scope.filter_products = params[i].substr(16);
          }
          else if (params[i].indexOf("filter-operations=") === 0) {
            $scope.filter_operations = params[i].substr(18);
          }
          else if (params[i].indexOf("filter-usageTypes=") === 0) {
            $scope.filter_usageTypes = params[i].substr(18);
          }
          else if (params[i].indexOf("tag-") === 0) {
            var parts = params[i].substr(4).split("=");
            $scope.tagParams[parts[0]] = parts[1];
          }
          else if (params[i].indexOf("filter-tag-") === 0) {
            var parts = params[i].substr(11).split("=");
            $scope.tagFilterParams[parts[0]] = parts[1];
          }
        }
      }
      if (time) {
        timeParams = time;
      }
    },

    processParams: function ($scope) {
      if ($scope.showUserTags) {
        $scope.filter_userTagValues = Array($scope.userTags.length);
        $scope.selected__userTagValues = Array($scope.userTags.length);
        for (var key in $scope.tagParams) {
          for (j = 0; j < $scope.userTags.length; j++) {
            if ($scope.userTags[j].name === key) {
              $scope.selected__userTagValues[j] = $scope.tagParams[key].split(",");
            }
          }
        }
        for (var key in $scope.tagFilterParams) {
          if ($scope.userTags[j].name === key) {
            $scope.filter_userTagValues[j] = $scope.tagFilterParams[key];
          }
        }
      }
      if (!$scope.showUserTags) {
        for (var j in $scope.groupBys) {
          if ($scope.groupBys[j].name === "Tag") {
            $scope.groupBys.splice(j, 1);
            break;
          }
        }
      }
      var toRemove = $scope.showZones ? "Region" : "Zone";
      for (var j in $scope.groupBys) {
        if ($scope.groupBys[j].name === toRemove) {
          $scope.groupBys.splice(j, 1);
          break;
        }
      }
    },

    updateOrganizationalUnit: function ($scope) {
      // select all the accounts in the org
      $scope.selected_accounts = this.filterSelected($scope.accounts, null, $scope.organizationalUnit);
    },

    getAccounts: function ($scope, fn, params) {
      if (!$scope.dimensions[$scope.ACCOUNT_INDEX]) {
        if (fn)
          fn({});
        return;
      }
      if (!params) {
        params = {};
      }
      $http({
        method: "GET",
        url: "getAccounts",
        params: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.accounts = result.data;
          $scope.organizationalUnits = getOrgs($scope.accounts);
          if ($scope.selected__accounts && !$scope.selected_accounts)
            $scope.selected_accounts = getSelected($scope.accounts, $scope.selected__accounts);
          else
            this.updateOrganizationalUnit($scope);
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getRegions: function ($scope, fn, params) {
      if (!$scope.dimensions[$scope.REGION_INDEX]) {
        if (fn)
          fn({});
        return;
      }
      if (!params) {
        params = {};
      }
      if ($scope.dimensions[$scope.ACCOUNT_INDEX])
        this.addParams(params, "account", $scope.accounts, $scope.selected_accounts);

      $http({
        method: "POST",
        url: "getRegions",
        data: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.regions = result.data;
          if ($scope.selected__regions && !$scope.selected_regions)
            $scope.selected_regions = getSelected($scope.regions, $scope.selected__regions);
          else if (!$scope.selected_regions) {
            $scope.selected_regions = $scope.regions;
          }
          else if ($scope.selected_regions.length > 0) {
            $scope.selected_regions = updateSelected($scope.regions, $scope.selected_regions);
          }
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getZones: function ($scope, fn, params) {
      if (!$scope.dimensions[$scope.ZONE_INDEX]) {
        if (fn)
          fn({});
        return;
      }
      if (!params) {
        params = {};
      }
      if ($scope.dimensions[$scope.ZONE_INDEX])
        this.addParams(params, "account", $scope.accounts, $scope.selected_accounts);

      $http({
        method: "POST",
        url: "getZones",
        data: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.zones = result.data;
          if ($scope.selected__zones && !$scope.selected_zones)
            $scope.selected_zones = getSelected($scope.zones, $scope.selected__zones);
          else if (!$scope.selected_zones)
            $scope.selected_zones = $scope.zones;
          else if ($scope.selected_zones.length > 0)
            $scope.selected_zones = updateSelected($scope.zones, $scope.selected_zones);
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getProducts: function ($scope, fn, params) {
      if (!$scope.dimensions[$scope.PRODUCT_INDEX]) {
        if (fn)
          fn({});
        return;
      }
      if (!params) {
        params = {};
      }
      if ($scope.dimensions[$scope.ACCOUNT_INDEX])
        this.addParams(params, "account", $scope.accounts, $scope.selected_accounts);
      if ($scope.dimensions[$scope.REGION_INDEX])
        this.addParams(params, "region", $scope.regions, $scope.selected_regions);

      if ($scope.resources) {
        params.resources = true;
      }

      $http({
        method: "POST",
        url: "getProducts",
        data: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.products = result.data;
          if ($scope.selected__products && !$scope.selected_products)
            $scope.selected_products = getSelected($scope.products, $scope.selected__products);
          else if (!$scope.selected_products)
            $scope.selected_products = $scope.products;
          else if ($scope.selected_products.length > 0)
            $scope.selected_products = updateSelected($scope.products, $scope.selected_products);
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getUserTags: function ($scope, fn, params) {
      if (!params)
        params = {}

      $http({
        method: "GET",
        url: "tags",
        params: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.userTags = result.data;

          if ($scope.showUserTags) {
            $scope.groupByTags = $scope.userTags;
            if ($scope.userTags.length > 0) {
              $scope.groupByTag = $scope.userTags[0];
              for (var j in $scope.groupByTags) {
                if ($scope.groupByTags[j].name === $scope.initialGroupByTag) {
                  $scope.groupByTag = $scope.groupByTags[j];
                  break;
                }
              }
              if ($scope.enabledUserTags.length != $scope.userTags.length) {
                $scope.enabledUserTags = Array($scope.userTags.length);
                for (var i = 0; i < $scope.userTags.length; i++)
                  $scope.enabledUserTags[i] = false;
              }
            }
          }

          $scope.tagKeys = result.data;
          if ($scope.selected__tagKeys && !$scope.selected_tagKeys)
            $scope.selected_tagKeys = getSelected($scope.tagKeys, $scope.selected__tagKeys);
          else
            $scope.selected_tagKeys = $scope.tagKeys;
          if ($scope.selected__tagKey && !$scope.selected_tagKey)
            $scope.selected_tagKey = getSelected($scope.tagKeys, $scope.selected__tagKey)[0];
          else {
            if ($scope.tagKeys.length > 0)
              $scope.selected_tagKey = $scope.tagKeys[0];
          }

          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getUserTagValues: function ($scope, index, fn, params) {
      if (!$scope.enabledUserTags[index]) {
        $scope.userTagValues[index] = {};
        if (fn)
          fn({});
        return;
      }

      if (!params) {
        params = {};
      }
      params.index = index;
      if ($scope.dimensions[$scope.ACCOUNT_INDEX])
        this.addParams(params, "account", $scope.accounts, $scope.selected_accounts);
      if ($scope.dimensions[$scope.REGION_INDEX])
        this.addParams(params, "region", $scope.regions, $scope.selected_regions);
      if ($scope.dimensions[$scope.PRODUCT_INDEX])
        this.addParams(params, "product", $scope.regions, $scope.selected_products);
      
      $http({
        method: "POST",
        url: "userTagValues",
        data: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.userTagValues[index] = result.data;
          if ($scope.selected__userTagValues[index] && !$scope.selected_userTagValues[index])
            $scope.selected_userTagValues[index] = getSelected($scope.userTagValues[index], $scope.selected__userTagValues[index]);
          else if (!$scope.selected_userTagValues[index])
            $scope.selected_userTagValues[index] = $scope.userTagValues[index];
          else if ($scope.selected_userTagValues[index].length > 0)
            $scope.selected_userTagValues[index] = updateSelected($scope.userTagValues[index], $scope.selected_userTagValues[index]);
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getOperations: function ($scope, fn, params) {
      if (!$scope.dimensions[$scope.OPERATION_INDEX]) {
        if (fn)
          fn({});
        return;
      }
      if (!params) {
        params = {};
      }
      params["usage_cost"] = $scope.usage_cost;
      params["showLent"] = $scope.reservationSharing === "lent";
      if (!$scope.recurring || !$scope.amortized || !$scope.credit || !$scope.tax || !$scope.savings) {
        categories = [];
        if (!$scope.recurring)
          categories.push("recurring");
        if (!$scope.amortized)
          categories.push("amortized");
        if (!$scope.credit)
          categories.push("credit");
        if (!$scope.tax)
          categories.push("tax");
        if (!$scope.savings)
          categories.push("savings");

        params["exclude"] = categories.join(',');
      }
      if ($scope.dimensions[$scope.ACCOUNT_INDEX])
        this.addParams(params, "account", $scope.accounts, $scope.selected_accounts);
      if ($scope.dimensions[$scope.REGION_INDEX])
        this.addParams(params, "region", $scope.regions, $scope.selected_regions);
      if ($scope.dimensions[$scope.PRODUCT_INDEX])
        this.addParams(params, "product", $scope.regions, $scope.selected_products);
      
      $http({
        method: "POST",
        url: "getOperations",
        data: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.operations = result.data;
          if ($scope.selected__operations && !$scope.selected_operations)
            $scope.selected_operations = getSelected($scope.operations, $scope.selected__operations);
          else if (!$scope.selected_operations)
            $scope.selected_operations = $scope.operations;
          else if ($scope.selected_operations.length > 0)
            $scope.selected_operations = updateSelected($scope.operations, $scope.selected_operations);
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getUsageTypes: function ($scope, fn, params) {
      if (!$scope.dimensions[$scope.USAGETYPE_INDEX]) {
        if (fn)
          fn({});
        return;
      }
      if (!params) {
        params = {};
      }
      if ($scope.dimensions[$scope.ACCOUNT_INDEX])
        this.addParams(params, "account", $scope.accounts, $scope.selected_accounts);
      if ($scope.dimensions[$scope.REGION_INDEX])
        this.addParams(params, "region", $scope.regions, $scope.selected_regions);
      if ($scope.dimensions[$scope.PRODUCT_INDEX])
        this.addParams(params, "product", $scope.regions, $scope.selected_products);
      if ($scope.dimensions[$scope.OPERATION_INDEX])
        this.addParams(params, "operation", $scope.operations, $scope.selected_operations);
      if ($scope.resources)
        params.resources = true;
      
      $http({
        method: "POST",
        url: "getUsageTypes",
        data: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.usageTypes = result.data;
          if ($scope.selected__usageTypes && !$scope.selected_usageTypes)
            $scope.selected_usageTypes = getSelected($scope.usageTypes, $scope.selected__usageTypes);
          else if (!$scope.selected_usageTypes)
            $scope.selected_usageTypes = $scope.usageTypes;
          else if ($scope.selected_usageTypes.length > 0)
            $scope.selected_usageTypes = updateSelected($scope.usageTypes, $scope.selected_usageTypes);
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getReservationOps: function ($scope, fn, params) {
      if (!params) {
        params = {};
      }
      params["showLent"] = $scope.reservationSharing === "lent";
      $http({
        method: "GET",
        url: "getReservationOps",
        params: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.reservationOps = result.data;
        }
        if (fn)
          fn(result.data);
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });    
    },

    getSavingsPlanOps: function ($scope, fn, params) {
      if (!params) {
        params = {};
      }
      params["showLent"] = $scope.reservationSharing === "lent";
      $http({
        method: "GET",
        url: "getSavingsPlanOps",
        params: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.savingsPlanOps = result.data;
        }
        if (fn)
          fn(result.data);
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    },

    getUtilizationOps: function ($scope, fn, params) {
      if (!params) {
        params = {};
      }
      $http({
        method: "GET",
        url: "getUtilizationOps",
        params: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.utilizationOps = result.data;
        }
        if (fn)
          fn(result.data);
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });    
    },

    getData: function ($scope, fn, params, download, errfn) {
      if (!params)
        params = {};
      params = jQuery.extend({
        isCost: $scope.usage_cost === "cost",
        usageUnit: $scope.usageUnit,
        aggregate: "stats",
        groupBy: $scope.groupBy.name,
        consolidate: $scope.consolidate,
        start: $scope.start,
        end: $scope.end,
        breakdown: false,
        showsps: $scope.showsps ? true : false,
        factorsps: $scope.factorsps ? true : false,
        consolidateGroups: $scope.consolidateGroups ? true : false,
        tagCoverage: $scope.tagCoverage ? true : false,
        showLent: $scope.reservationSharing === "lent",
      }, params);
      
      if (!$scope.recurring || !$scope.amortized || !$scope.credit || !$scope.tax || !$scope.savings) {
        categories = [];
        if (!$scope.recurring)
          categories.push("recurring");
        if (!$scope.amortized)
          categories.push("amortized");
        if (!$scope.credit)
          categories.push("credit");
        if (!$scope.tax)
          categories.push("tax");
        if (!$scope.savings)
          categories.push("savings");

        params["exclude"] = categories.join(',');
      }

      if ($scope.dimensions[$scope.ACCOUNT_INDEX])
        this.addParams(params, "account", $scope.accounts, $scope.selected_accounts, $scope.selected__accounts, $scope.filter_accounts, $scope.organizationalUnit);
      if ($scope.showZones) {
        if ($scope.dimensions[$scope.ZONE_INDEX])
          this.addParams(params, "zone", $scope.zones, $scope.selected_zones, $scope.selected__zones, $scope.filter_zones);
      }
      else {
        if ($scope.dimensions[$scope.REGION_INDEX])
          this.addParams(params, "region", $scope.regions, $scope.selected_regions, $scope.selected__regions, $scope.filter_regions);
      }
      if ($scope.dimensions[$scope.PRODUCT_INDEX])
        this.addParams(params, "product", $scope.products, $scope.selected_products, $scope.selected__products, $scope.filter_products);
      if ($scope.dimensions[$scope.OPERATION_INDEX])
        this.addParams(params, "operation", $scope.operations, $scope.selected_operations, $scope.selected__operations, $scope.filter_operations);
      if ($scope.dimensions[$scope.USAGETYPE_INDEX])
        this.addParams(params, "usageType", $scope.usageTypes, $scope.selected_usageTypes, $scope.selected__usageTypes, $scope.filter_usageTypes);

      if ($scope.tagCoverage) {
        if ($scope.isGroupByTagKey())
          this.addParams(params, "tagKey", $scope.tagKeys, $scope.selected_tagKeys, $scope.selected__tagKeys, $scope.filter_tagKeys);
        else
          params.tagKey = $scope.selected_tagKey ? $scope.selected_tagKey.name : $scope.tagKeys.length > 0 ? $scope.tagKeys[0] : null;
      }
      if ($scope.showUserTags) {
        params.showUserTags = true;
        if ($scope.isGroupByTag())
          params.groupByTag = $scope.groupByTag.name;
        for (var i = 0; i < $scope.userTags.length; i++) {
          if ($scope.enabledUserTags[i]) {
            this.addParams(params, "tag-" + $scope.userTags[i].name, $scope.userTagValues[i], $scope.selected_userTagValues[i], $scope.selected__userTagValues[i], $scope.filter_userTagValues[i]);
          }
        }
      }

      if (!download) {
        $http({
          method: "POST",
          url: "getData",
          data: params
        }).success(function (result) {
          if (result.status === 200 && result.data && fn) {
            fn(result);
          }
        }).error(function (result, status) {
          if (status === 401)
            $window.location.reload();
          else if (errfn)
            errfn(result, status);
          });
      }
      else {
        jQuery("#download_form").empty();

        for (var key in params) {
          jQuery("<input type='text' />")
            .attr("id", key)
            .attr("name", key)
            .attr("value", params[key])
            .appendTo(jQuery("#download_form"));
        }

        jQuery("#download_form").submit();
      }
    },

    reverse: function (date) {
      var copy = [].concat(date);
      return copy.reverse();
    }
  };
});

function mainCtrl($scope, $location, $timeout, usage_db, highchart) {
  $scope.currencySign = global_currencySign;

  $scope.ACCOUNT_INDEX = 0;
  $scope.REGION_INDEX = 1;
  $scope.ZONE_INDEX = 2;
  $scope.PRODUCT_INDEX = 3;
  $scope.OPERATION_INDEX = 4;
  $scope.USAGETYPE_INDEX = 5;
  $scope.NUM_DIMENSIONS = 6;


  $scope.init = function ($scope) {
    $scope.dimensions = [false, false, false, false, false, false];
    $scope.showsps = false;
    $scope.factorsps = false;
    $scope.consolidateGroups = false;
    $scope.plotType = 'area';
    $scope.reservationSharing = 'borrowed';
    $scope.consolidate = "daily";
    $scope.legends = [];
    $scope.showZones = false;
    $scope.end = new Date();
    $scope.start = new Date();
    $scope.usage_cost = "cost";
    $scope.groupByTag = {}
    $scope.initialGroupByTag = '';
    $scope.showUserTags = false;
    $scope.predefinedQuery = null;
    $scope.recurring = true;
    $scope.amortized = true;
    $scope.credit = true;
    $scope.tax = true;
    $scope.savings = true;
  }

  $scope.initUserTagVars = function ($scope) {
    $scope.enabledUserTags = [];
    $scope.userTags = [];
    $scope.userTagValues = [];
    $scope.selected_userTagValues = [];
    $scope.selected__userTagValues = [];
    $scope.filter_userTagValues = [];
  }

  $scope.addCommonParams = function ($scope, params) {
    params.start = $scope.start;
    params.end = $scope.end;
    if ($scope.usage_cost)
      params.usage_cost = $scope.usage_cost;
    if ($scope.usageUnit)
      params.usageUnit = $scope.usageUnit;
    if ($scope.plotType)
      params.plotType = $scope.plotType;
    if ($scope.reservationSharing)
      params.reservationSharing = $scope.reservationSharing;
    if ($scope.showsps)
      params.showsps = "" + $scope.showsps;
    if ($scope.factorsps)
      params.factorsps = "" + $scope.factorsps;
    if ($scope.showUserTags)
      params.showUserTags = "" + $scope.showUserTags;
    if ($scope.groupBy.name)
      params.groupBy = $scope.groupBy.name;
    if ($scope.consolidateGroups)
      params.consolidateGroups = "" + $scope.consolidateGroups;
    if ($scope.groupByTag && $scope.groupByTag.name)
      params.groupByTag = $scope.groupByTag.name;
    if ($scope.consolidate)
      params.consolidate = $scope.consolidate;
    if ($scope.showZones)
      params.showZones = "" + $scope.showZones;
  }

  window.onhashchange = function () {
    window.location.reload();
  }

  var pageLoaded = false;
  $scope.$watch(function () { return $location.path(); }, function (locationPath) {
    if (pageLoaded)
      $timeout(function () { location.reload() });
    else
      pageLoaded = true;
  });

  $scope.throughput_metricname = throughput_metricname;

  $scope.getTimeParams = function () {
    return usage_db.getTimeParams();
  }

  $scope.reload = function () {
    $timeout(function () { location.reload() });
  }

  $scope.dateFormat = function (time) {
    return highchart.dateFormat(time);
  }

  $scope.monthFormat = function (time) {
    return highchart.monthFormat(time);
  }

  $scope.dayFormat = function (time) {
    return highchart.dayFormat(time);
  }

  $scope.getConsolidateName = function (consolidate) {
    if (consolidate === 'weekly')
      return "week";
    else if (consolidate == 'monthly')
      return "month";
    else
      return "";
  }

  $scope.clickitem = function (legend) {
    highchart.clickitem(legend);
  }

  $scope.showall = function () {
    highchart.showall();
  }

  $scope.hideall = function () {
    highchart.hideall();
  }

  $scope.getTrClass = function (index) {
    return index % 2 == 0 ? "even" : "odd";
  }

  $scope.order = function (data, name, stats) {

    if ($scope.predicate != name) {
      $scope.reservse = name === 'name';
      $scope.predicate = name;
    }
    else {
      $scope.reservse = !$scope.reservse;
    }

    var compare = function (a, b) {
      if (a['name'] === 'aggregated')
        return -1;
      if (b['name'] === 'aggregated')
        return 1;

      if (!stats) {
        if (a[name] < b[name])
          return !$scope.reservse ? 1 : -1;
        if (a[name] > b[name])
          return !$scope.reservse ? -1 : 1;
        return 0;
      }
      else {
        if (a.stats[name] < b.stats[name])
          return !$scope.reservse ? 1 : -1;
        if (a.stats[name] > b.stats[name])
          return !$scope.reservse ? -1 : 1;
        return 0;
      }
    }
    data.sort(compare);
    highchart.order(data);
  }

  $scope.graphOnly = function () {
    return usage_db.graphOnly();
  }

  $scope.getBodyWidth = function (defaultWidth) {
    return usage_db.graphOnly() ? "" : defaultWidth;
  }

  $scope.updateAccounts = function ($scope) {
    usage_db.getAccounts($scope, function (data) {
      if ($scope.showZones)
        $scope.updateZones($scope);
      else
        $scope.updateRegions($scope);
    });
  }

  $scope.updateZones = function ($scope) {
    usage_db.getZones($scope, function (data) {
      $scope.updateProducts($scope);
    });
  }

  $scope.updateRegions = function ($scope) {
    usage_db.getRegions($scope, function (data) {
      $scope.updateProducts($scope);
    });
  }

  $scope.updateProducts = function ($scope) {
    var query = $scope.predefinedQuery ? jQuery.extend({}, $scope.predefinedQuery) : null;

    usage_db.getProducts($scope, function (data) {
      if ($scope.showUserTags)
        $scope.updateUserTagValues($scope, 0, true);
      else
        $scope.updateOperations($scope);
    }, query);
  }

  $scope.updateOperations = function ($scope) {
    var query = $scope.predefinedQuery ? jQuery.extend({}, $scope.predefinedQuery) : null;

    usage_db.getOperations($scope, function (data) {
      $scope.updateUsageTypes($scope);
    }, query);
  }

  $scope.updateUsageTypes = function ($scope) {
    var query = $scope.predefinedQuery ? jQuery.extend({}, $scope.predefinedQuery) : null;

    usage_db.getUsageTypes($scope, null, query);
  }

  $scope.updateUserTagValues = function ($scope, index, all) {
    usage_db.getUserTagValues($scope, index, function (data) {
      if (all) {
        index++;
        if (index < $scope.userTags.length)
          $scope.updateUserTagValues($scope, index, all);
        else
          $scope.updateOperations($scope);
      }
    });
  }
}

function reservationCtrl($scope, $location, $http, usage_db, highchart) {

  $scope.init($scope);
  $scope.consolidate = "hourly";
  $scope.usageUnit = "Instances";
  $scope.groupBys = [
    { name: "CostType" },
    { name: "OrgUnit" },
    { name: "Account" },
    { name: "Region" },
    { name: "Zone" },
    { name: "Product" },
    { name: "Operation" },
    { name: "UsageType" }
  ];
  $scope.groupBy = $scope.groupBys[6];
  var startMonth = $scope.end.getUTCMonth() - 1;
  var startYear = $scope.end.getUTCFullYear();
  if (startMonth < 0) {
    startMonth += 12;
    startYear -= 1;
  }
  $scope.start.setUTCFullYear(startYear);
  $scope.start.setUTCMonth(startMonth);
  $scope.start.setUTCDate(1);
  $scope.start.setUTCHours(0);

  $scope.end = highchart.dateFormat($scope.end); //$filter('date')($scope.end, "y-MM-dd hha");
  $scope.start = highchart.dateFormat($scope.start); //$filter('date')($scope.start, "y-MM-dd hha");

  $scope.updateUrl = function () {
    $scope.end = jQuery('#end').datetimepicker().val();
    $scope.start = jQuery('#start').datetimepicker().val();
    var params = {};
    $scope.addCommonParams($scope, params);
    usage_db.addDimensionParams($scope, params);
    usage_db.updateUrl($location, params);
  }

  $scope.download = function () {
    var query = { operation: $scope.reservationOps.join(","), forReservation: true };
    if ($scope.showZones)
      query.showZones = true;
    usage_db.getData($scope, null, query, true);
  }

  $scope.getData = function () {
    $scope.loading = true;
    $scope.errorMessage = null;
    var query = { operation: $scope.reservationOps.join(","), forReservation: true };
    if ($scope.showZones)
      query.showZones = true;
    usage_db.getData($scope, function (result) {
      var hourlydata = [];
      for (var key in result.data) {
        hourlydata.push({ name: key, data: result.data[key] });
      }
      result.data = hourlydata;
      $scope.legends = [];
      $scope.stats = result.stats;
      highchart.drawGraph(result, $scope);
      $scope.loading = false;

      $scope.legendPrecision = $scope.usage_cost == "cost" ? 2 : 0;
      $scope.legendName = $scope.groupBy.name;
      $scope.legend_usage_cost = $scope.usage_cost;
    }, query, false, function (result, status) {
      $scope.errorMessage = "Error: " + status;
      $scope.loading = false;
    });
  }

  $scope.accountsEnabled = function () {
    $scope.updateAccounts($scope);
  }
  
  $scope.regionsEnabled = function () {
    $scope.updateRegions($scope);
  }
  
  $scope.zonesEnabled = function () {
    $scope.updateZones($scope);
  }
  
  $scope.productsEnabled = function () {
    $scope.updateProducts($scope);
  }
  
  $scope.operationsEnabled = function () {
    $scope.updateOperations($scope);
  }
  
  $scope.usageTypesEnabled = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.filterAccount = function (filter_accounts) {
    return usage_db.filterAccount($scope, filter_accounts);
  }

  $scope.includeChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.reservationSharingChanged = function () {
    usage_db.getReservationOps($scope, function () {
      $scope.predefinedQuery = { operation: $scope.reservationOps.join(","), forReservation: true };
      $scope.updateOperations($scope);
    });
  }

  $scope.orgUnitChanged = function () {
    usage_db.updateOrganizationalUnit($scope);
    $scope.accountsChanged();
  }

  $scope.accountsChanged = function () {
    if ($scope.showZones)
      $scope.updateZones($scope);
    else
      $scope.updateRegions($scope);
  }

  $scope.regionsChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.zonesChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.productsChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.operationsChanged = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.usageCostChanged = function () {
    updateOperations($scope);
  }

  var fn = function () {
    $scope.predefinedQuery = { operation: $scope.reservationOps.join(","), forReservation: true };

    $scope.updateAccounts($scope);
    $scope.getData();

    jQuery("#start, #end").datetimepicker({
      showTime: false,
      showMinute: false,
      ampm: true,
      timeFormat: 'hhTT',
      dateFormat: 'yy-mm-dd'
    });
    jQuery('#end').datetimepicker().val($scope.end);
    jQuery('#start').datetimepicker().val($scope.start);
  }

  usage_db.getParams($location.hash(), $scope);
  usage_db.processParams($scope);
  usage_db.getReservationOps($scope, fn);
}

function savingsPlansCtrl($scope, $location, $http, usage_db, highchart) {

  $scope.init($scope);
  $scope.consolidate = "hourly";
  $scope.usageUnit = "Instances";
  $scope.groupBys = [
    { name: "CostType" },
    { name: "OrgUnit" },
    { name: "Account" },
    { name: "Region" },
    { name: "Zone" },
    { name: "Product" },
    { name: "Operation" },
    { name: "UsageType" }
  ];
  $scope.groupBy = $scope.groupBys[6];
  var startMonth = $scope.end.getUTCMonth() - 1;
  var startYear = $scope.end.getUTCFullYear();
  if (startMonth < 0) {
    startMonth += 12;
    startYear -= 1;
  }
  $scope.start.setUTCFullYear(startYear);
  $scope.start.setUTCMonth(startMonth);
  $scope.start.setUTCDate(1);
  $scope.start.setUTCHours(0);

  $scope.end = highchart.dateFormat($scope.end); //$filter('date')($scope.end, "y-MM-dd hha");
  $scope.start = highchart.dateFormat($scope.start); //$filter('date')($scope.start, "y-MM-dd hha");

  $scope.updateUrl = function () {
    $scope.end = jQuery('#end').datetimepicker().val();
    $scope.start = jQuery('#start').datetimepicker().val();
    var params = {};
    $scope.addCommonParams($scope, params);
    usage_db.addDimensionParams($scope, params);
    usage_db.updateUrl($location, params);
  }

  $scope.download = function () {
    var ops = $scope.savingsPlanOps.concat($scope.reservationOps);
    var query = { operation: ops.join(","), forSavingsPlans: true };
    if ($scope.showZones)
      query.showZones = true;
    usage_db.getData($scope, null, query, true);
  }

  $scope.getData = function () {
    $scope.loading = true;
    $scope.errorMessage = null;
    var ops = $scope.savingsPlanOps.concat($scope.reservationOps);
    var query = { operation: ops.join(","), forSavingsPlans: true };
    if ($scope.showZones)
      query.showZones = true;
    usage_db.getData($scope, function (result) {
      var hourlydata = [];
      for (var key in result.data) {
        hourlydata.push({ name: key, data: result.data[key] });
      }
      result.data = hourlydata;
      $scope.legends = [];
      $scope.stats = result.stats;
      highchart.drawGraph(result, $scope);
      $scope.loading = false;

      $scope.legendPrecision = $scope.usage_cost == "cost" ? 2 : 0;
      $scope.legendName = $scope.groupBy.name;
      $scope.legend_usage_cost = $scope.usage_cost;
    }, query, false, function (result, status) {
      $scope.errorMessage = "Error: " + status;
      $scope.loading = false;
    });
  }

  $scope.accountsEnabled = function () {
    $scope.updateAccounts($scope);
  }
  
  $scope.regionsEnabled = function () {
    $scope.updateRegions($scope);
  }
  
  $scope.zonesEnabled = function () {
    $scope.updateZones($scope);
  }
  
  $scope.productsEnabled = function () {
    $scope.updateProducts($scope);
  }
  
  $scope.operationsEnabled = function () {
    $scope.updateOperations($scope);
  }
  
  $scope.usageTypesEnabled = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.filterAccount = function (filter_accounts) {
    return usage_db.filterAccount($scope, filter_accounts);
  }

  $scope.orgUnitChanged = function () {
    usage_db.updateOrganizationalUnit($scope);
    $scope.accountsChanged();
  }

  $scope.includeChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.reservationSharingChanged = function () {
    usage_db.getSavingsPlanOps($scope, function() {
      usage_db.getReservationOps($scope, function () {
        var ops = $scope.savingsPlanOps.concat($scope.reservationOps);
        $scope.predefinedQuery = { operation: ops.join(","), forSavingsPlans: true };
        $scope.updateOperations($scope);
      });
    });
  }

  $scope.accountsChanged = function () {
    if ($scope.showZones)
      $scope.updateZones($scope);
    else
      $scope.updateRegions($scope);
  }

  $scope.regionsChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.zonesChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.productsChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.operationsChanged = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.usageCostChanged = function () {
    updateOperations($scope);
  }

  var fn = function () {
    var ops = $scope.savingsPlanOps.concat($scope.reservationOps);
    $scope.predefinedQuery = { operation: ops.join(","), forSavingsPlans: true };

    $scope.updateAccounts($scope);
    $scope.getData();

    jQuery("#start, #end").datetimepicker({
      showTime: false,
      showMinute: false,
      ampm: true,
      timeFormat: 'hhTT',
      dateFormat: 'yy-mm-dd'
    });
    jQuery('#end').datetimepicker().val($scope.end);
    jQuery('#start').datetimepicker().val($scope.start);
  }

  $scope.updateProducts = function ($scope) {
    var query = { operation: $scope.savingsPlanOps.join(","), forSavingsPlans: true };

    usage_db.getProducts($scope, function (data) {
      if ($scope.showUserTags)
        $scope.updateUserTagValues($scope, 0, true);
      else
        $scope.updateOperations($scope);
    }, query);
  }

  usage_db.getParams($location.hash(), $scope);
  usage_db.processParams($scope);
  usage_db.getSavingsPlanOps($scope, function() {
    usage_db.getReservationOps($scope, fn);
  });
}

function tagCoverageCtrl($scope, $location, $http, usage_db, highchart) {
  $scope.init($scope);
  $scope.initUserTagVars($scope);
  $scope.resources = true; // limit products, operations, and usageTypes to those that have tagged resources
  $scope.plotType = "line";
  $scope.usage_cost = "";
  $scope.usageUnit = "";
  $scope.tagCoverage = true;
  $scope.groupBys = [
    { name: "None" },
    { name: "TagKey" },
    { name: "CostType" },
    { name: "OrgUnit" },
    { name: "Account" },
    { name: "Region" },
    { name: "Product" },
    { name: "Operation" },
    { name: "UsageType" },
    { name: "Tag" }
  ],
  $scope.groupBy = $scope.groupBys[1];
  $scope.consolidate = "daily";
  var startMonth = $scope.end.getUTCMonth() - 1;
  var startYear = $scope.end.getUTCFullYear();
  if (startMonth < 0) {
    startMonth += 12;
    startYear -= 1;
  }
  $scope.start.setUTCFullYear(startYear);
  $scope.start.setUTCMonth(startMonth);
  $scope.start.setUTCDate(1);
  $scope.start.setUTCHours(0);

  $scope.end = highchart.dateFormat($scope.end); //$filter('date')($scope.end, "y-MM-dd hha");
  $scope.start = highchart.dateFormat($scope.start); //$filter('date')($scope.start, "y-MM-dd hha");

  $scope.isGroupByTagKey = function () {
    return $scope.groupBy.name === 'TagKey';
  }
  
  $scope.isGroupByTag = function () {
    return $scope.groupBy.name === 'Tag';
  }
  
  $scope.updateUrl = function () {
    $scope.end = jQuery('#end').datetimepicker().val();
    $scope.start = jQuery('#start').datetimepicker().val();
    var params = {
      tagCoverage: "" + $scope.tagCoverage,
    };
    $scope.addCommonParams($scope, params);
    usage_db.addDimensionParams($scope, params);
    if ($scope.showUserTags) {
      usage_db.addUserTagParams($scope, params);
    }

    if ($scope.isGroupByTagKey())
      params.tagKey = { selected: $scope.selected_tagKeys };
    else
      params.tagKey = $scope.selected_tagKey.name;

    usage_db.updateUrl($location, params);
  }

  $scope.download = function () {
    usage_db.getData($scope, null, null, true);
  }

  $scope.getData = function () {
    $scope.loading = true;
    $scope.errorMessage = null;
    usage_db.getData($scope, function (result) {
      var hourlydata = [];
      for (var key in result.data) {
        hourlydata.push({ name: key, data: result.data[key] });
      }
      result.data = hourlydata;
      $scope.legends = [];
      $scope.stats = result.stats;
      highchart.drawGraph(result, $scope, false, false, true);
      $scope.loading = false;

      $scope.legendName = $scope.groupBy.name;
      $scope.legend_usage_cost = $scope.usage_cost;
    }, null, false, function (result, status) {
      $scope.errorMessage = "Error: " + status;
      $scope.loading = false;
    });
  }

  $scope.getUserTagValues = function (index, count) {
    var vals = [];
    for (var i = index; i < index + count && i < $scope.userTagValues.length; i++)
      vals.push($scope.userTagValues[i]);
    return vals;
  }

  $scope.accountsEnabled = function () {
    $scope.updateAccounts($scope);
  }
  
  $scope.regionsEnabled = function () {
    $scope.updateRegions($scope);
  }
  
  $scope.productsEnabled = function () {
    $scope.updateProducts($scope);
  }
  
  $scope.operationsEnabled = function () {
    $scope.updateOperations($scope);
  }
  
  $scope.usageTypesEnabled = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.filterAccount = function (filter_accounts) {
    return usage_db.filterAccount($scope, filter_accounts);
  }

  $scope.orgUnitChanged = function () {
    usage_db.updateOrganizationalUnit($scope);
    $scope.accountsChanged();
  }

  $scope.accountsChanged = function () {
    $scope.updateRegions($scope);
  }

  $scope.regionsChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.productsChanged = function () {
    if ($scope.showUserTags)
      $scope.updateUserTagValues($scope, 0, true);
    else
      $scope.updateOperations($scope);
  }

  $scope.operationsChanged = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.userTagsChanged = function (index) {
    $scope.updateUserTagValues($scope, index, false);
  }


  usage_db.getParams($location.hash(), $scope);
  usage_db.processParams($scope);

  $scope.getUserTags = function () {
    usage_db.getUserTags($scope, function (data) {      
      $scope.getData();
    });
  }

  var fn = function () {
    usage_db.getUserTags($scope, function (data) {
      $scope.updateAccounts($scope);
      $scope.getData();
    });

    jQuery("#start, #end").datetimepicker({
      showTime: false,
      showMinute: false,
      ampm: true,
      timeFormat: 'hhTT',
      dateFormat: 'yy-mm-dd'
    });
    jQuery('#end').datetimepicker().val($scope.end);
    jQuery('#start').datetimepicker().val($scope.start);
  }

  fn();
}

function utilizationCtrl($scope, $location, $http, usage_db, highchart) {

  $scope.init($scope);
  $scope.usage_cost = "usage";
  $scope.usageUnit = "ECUs";
  $scope.groupBys = [
    { name: "OrgUnit" },
    { name: "Account" },
    { name: "Region" },
    { name: "Product" },
    { name: "Operation" },
    { name: "UsageType" }
  ];
  $scope.groupBy = $scope.groupBys[0];
  $scope.consolidate = "daily";
  $scope.plotType = 'line';
  var startMonth = $scope.end.getUTCMonth() - 1;
  var startYear = $scope.end.getUTCFullYear();
  if (startMonth < 0) {
    startMonth += 12;
    startYear -= 1;
  }
  $scope.start.setUTCFullYear(startYear);
  $scope.start.setUTCMonth(startMonth);
  $scope.start.setUTCDate(1);
  $scope.start.setUTCHours(0);

  $scope.end.setUTCHours(0);
  $scope.start.setUTCHours(0);
  $scope.end = highchart.dateFormat($scope.end); //$filter('date')($scope.end, "y-MM-dd hha");
  $scope.start = highchart.dateFormat($scope.start); //$filter('date')($scope.start, "y-MM-dd hha");

  $scope.updateUrl = function () {
    $scope.end = jQuery('#end').datetimepicker().val();
    $scope.start = jQuery('#start').datetimepicker().val();
    var params = {};
    $scope.addCommonParams($scope, params);
    usage_db.addDimensionParams($scope, params);
    usage_db.updateUrl($location, params);
  }

  $scope.download = function () {
    var query = { operation: utilizationOps.join(","), forReservation: true, elasticity: true };
    usage_db.getData($scope, null, query, true);
  }

  $scope.getData = function () {
    $scope.loading = true;
    $scope.errorMessage = null;
    var query = { operation: $scope.utilizationOps.join(","), forReservation: true, elasticity: true };

    usage_db.getData($scope, function (result) {
      var dailyData = [];
      for (var key in result.data) {
        dailyData.push({ name: key, data: result.data[key] });
      }
      result.data = dailyData;
      $scope.legends = [];
      $scope.stats = result.stats;
      highchart.drawGraph(result, $scope, false, true, false);
      $scope.loading = false;

      $scope.legendPrecision = 0;
      $scope.legendName = $scope.groupBy.name;
      $scope.legend_usage_cost = $scope.usage_cost;
    }, query, false, function (result, status) {
      $scope.errorMessage = "Error: " + status;
      $scope.loading = false;
    });
  }

  $scope.accountsEnabled = function () {
    $scope.updateAccounts($scope);
  }
  
  $scope.regionsEnabled = function () {
    $scope.updateRegions($scope);
  }
  
  $scope.productsEnabled = function () {
    $scope.updateProducts($scope);
  }
  
  $scope.operationsEnabled = function () {
    $scope.updateOperations($scope);
  }
  
  $scope.usageTypesEnabled = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.filterAccount = function (filter_accounts) {
    return usage_db.filterAccount($scope, filter_accounts);
  }

  $scope.orgUnitChanged = function () {
    usage_db.updateOrganizationalUnit($scope);
    $scope.accountsChanged();
  }

  $scope.accountsChanged = function () {
    $scope.updateRegions($scope);
  }

  $scope.regionsChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.productsChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.operationsChanged = function () {
    $scope.updateUsageTypes($scope);
  }

  var fn = function (data) {
    $scope.predefinedQuery = { operation: $scope.utilizationOps.join(","), forReservation: true };
    usage_db.getParams($location.hash(), $scope);
    usage_db.processParams($scope);

    $scope.updateAccounts($scope);
    $scope.getData();


    jQuery("#start, #end").datetimepicker({
      showTime: false,
      showMinute: false,
      ampm: true,
      timeFormat: 'hhTT',
      dateFormat: 'yy-mm-dd'
    });
    jQuery('#end').datetimepicker().val($scope.end);
    jQuery('#start').datetimepicker().val($scope.start);
  }

  usage_db.getUtilizationOps($scope, fn);
}

function detailCtrl($scope, $location, $http, usage_db, highchart) {

  $scope.init($scope);
  $scope.initUserTagVars($scope);
  $scope.usageUnit = "Instances";
  $scope.groupBys = [
    { name: "None" },
    { name: "CostType" },
    { name: "OrgUnit" },
    { name: "Account" },
    { name: "Region" },
    { name: "Product" },
    { name: "Operation" },
    { name: "UsageType" },
    { name: "Tag" }
  ],
  $scope.groupBy = $scope.groupBys[4];
  var startMonth = $scope.end.getUTCMonth() - 1;
  var startYear = $scope.end.getUTCFullYear();
  if (startMonth < 0) {
    startMonth += 12;
    startYear -= 1;
  }
  $scope.start.setUTCFullYear(startYear);
  $scope.start.setUTCMonth(startMonth);
  $scope.start.setUTCDate(1);
  $scope.start.setUTCHours(0);

  $scope.end = highchart.dateFormat($scope.end); //$filter('date')($scope.end, "y-MM-dd hha");
  $scope.start = highchart.dateFormat($scope.start); //$filter('date')($scope.start, "y-MM-dd hha");

  $scope.isGroupByTag = function () {
    return $scope.groupBy.name === 'Tag';
  }

  $scope.getUserTagDisplayName = function (index) {
    var display = $scope.userTags[index].name;
    if ($scope.userTags[index].aliases.length > 0)
      display += "/" + $scope.userTags[index].aliases.join("/");
    return display;
  }
  
  $scope.updateUrl = function () {
    $scope.end = jQuery('#end').datetimepicker().val();
    $scope.start = jQuery('#start').datetimepicker().val();
    var params = {};
    $scope.addCommonParams($scope, params);
    usage_db.addDimensionParams($scope, params);
    if ($scope.showUserTags) {
      usage_db.addUserTagParams($scope, params);
    }

    usage_db.updateUrl($location, params);
  }

  $scope.download = function () {
    usage_db.getData($scope, null, null, true);
  }

  $scope.getData = function () {
    $scope.loading = true;
    $scope.errorMessage = null;
    usage_db.getData($scope, function (result) {
      var hourlydata = [];
      for (var key in result.data) {
        hourlydata.push({ name: key, data: result.data[key] });
      }
      result.data = hourlydata;
      $scope.legends = [];
      $scope.stats = result.stats;
      highchart.drawGraph(result, $scope);
      $scope.loading = false;

      $scope.legendName = $scope.groupBy.name;
      $scope.legend_usage_cost = $scope.usage_cost;
    }, null, false, function (result, status) {
      $scope.errorMessage = "Error: " + status;
      $scope.loading = false;
    });
  }

  $scope.getUserTagValues = function (index, count) {
    var vals = [];
    for (var i = index; i < index + count && i < $scope.userTagValues.length; i++)
      vals.push($scope.userTagValues[i]);
    return vals;
  }

  $scope.accountsEnabled = function () {
    $scope.updateAccounts($scope);
  }

  $scope.regionsEnabled = function () {
    $scope.updateRegions($scope);
  }
  
  $scope.productsEnabled = function () {
    $scope.updateProducts($scope);
  }
  
  $scope.operationsEnabled = function () {
    $scope.updateOperations($scope);
  }
  
  $scope.usageTypesEnabled = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.filterAccount = function (filter_accounts) {
    return usage_db.filterAccount($scope, filter_accounts);
  }

  $scope.orgUnitChanged = function () {
    usage_db.updateOrganizationalUnit($scope);
    $scope.accountsChanged();
  }

  $scope.includeChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.reservationSharingChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.accountsChanged = function () {
    $scope.updateRegions($scope);
  }

  $scope.regionsChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.productsChanged = function () {
    if ($scope.showUserTags)
      $scope.updateUserTagValues($scope, 0, true);
    else
      $scope.updateOperations($scope);
  }

  $scope.operationsChanged = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.userTagsChanged = function (index) {
    $scope.updateUserTagValues($scope, index, false);
  }

  var getUserTags = function () {
    if ($scope.showUserTags)
      usage_db.getUserTags($scope, fn);
    else
      fn();
  }

  var fn = function () {
    usage_db.processParams($scope);

    usage_db.getAccounts($scope, function () {
        $scope.updateRegions($scope);
    });

    $scope.getData();

    jQuery("#start, #end").datetimepicker({
      showTime: false,
      showMinute: false,
      ampm: true,
      timeFormat: 'hhTT',
      dateFormat: 'yy-mm-dd'
    });
    jQuery('#end').datetimepicker().val($scope.end);
    jQuery('#start').datetimepicker().val($scope.start);
  }

  usage_db.getParams($location.hash(), $scope);

  if ($scope.spans) {
    $http({
      method: "GET",
      url: "getTimeSpan",
      params: { spans: $scope.spans, end: $scope.end, consolidate: $scope.consolidate }
    }).success(function (result) {
      $scope.end = result.end;
      $scope.start = result.start;
      getUserTags();
    });
  }
  else
    getUserTags();
}

function summaryCtrl($scope, $location, usage_db, highchart) {

  $scope.init($scope);
  $scope.usageUnit = "";
  $scope.groupBys = [
    { name: "CostType" },
    { name: "OrgUnit" },
    { name: "Account" },
    { name: "Region" },
    { name: "Product" },
    { name: "Operation" },
    { name: "UsageType" }
  ],
  $scope.groupBy = $scope.groupBys[4];
  var startMonth = $scope.end.getUTCMonth() - 6;
  var startYear = $scope.end.getUTCFullYear();
  if (startMonth < 0) {
    startMonth += 12;
    startYear -= 1;
  }
  $scope.start.setUTCFullYear(startYear);
  $scope.start.setUTCMonth(startMonth);

  $scope.end = highchart.dateFormat($scope.end); //$filter('date')($scope.end, "y-MM-dd hha");
  $scope.start = highchart.dateFormat($scope.start); //$filter('date')($scope.start, "y-MM-dd hha");

  $scope.updateUrl = function () {
    var params = {
      groupBy: $scope.groupBy.name,
    }

    usage_db.addDimensionParams($scope, params);
    usage_db.updateUrl($location, params);
  }

  $scope.order = function (index) {

    if ($scope.predicate != index) {
      $scope.reservse = index === 'name';
      $scope.predicate = index;
    }
    else {
      $scope.reservse = !$scope.reservse;
    }
    var compareName = function (a, b) {
      if (a[index] < b[index])
        return !$scope.reservse ? 1 : -1;
      if (a[index] > b[index])
        return !$scope.reservse ? -1 : 1;
      return 0;
    }
    var compare = function (a, b) {
      a = $scope.data[a.name];
      b = $scope.data[b.name];
      if (a[index] < b[index])
        return !$scope.reservse ? 1 : -1;
      if (a[index] > b[index])
        return !$scope.reservse ? -1 : 1;
      return 0;
    }
    if (index === 'name')
      $scope.legends.sort(compareName);
    else {
      $scope.legends.sort(compare);
    }
  }

  $scope.getData = function () {
    $scope.loading = true;
    $scope.errorMessage = null;
    usage_db.getData($scope, function (result) {
      $scope.data = {};
      $scope.months = usage_db.reverse(result.time);
      $scope.hours = usage_db.reverse(result.hours);

      var keys = [];
      for (var key in result.data) {
        keys.push(key);
        var values = {};
        var totals = usage_db.reverse(result.data[key]);
        $scope.headers = [];
        for (var i in totals) {
          values[2 * i] = totals[i];
          values[2 * i + 1] = (totals[i] / $scope.hours[i]);
          $scope.headers.push({ index: 2 * i, name: "total", start: highchart.dateFormat($scope.months[i]), end: highchart.dateFormat(i == 0 ? new Date().getTime() : $scope.months[i - 1]) });
          $scope.headers.push({ index: 2 * i + 1, name: "hourly", start: highchart.dateFormat($scope.months[i]), end: highchart.dateFormat(i == 0 ? new Date().getTime() : $scope.months[i - 1]) });
        }
        values.name = key;
        $scope.data[key] = (values);
      }
      $scope.resultStart = result.start;

      usage_db.getData($scope, function (result) {
        var hourlydata = [];
        for (var i in keys) {
          if (result.data[keys[i]]) {
            hourlydata.push({ name: keys[i], data: result.data[keys[i]] });
          }
        }
        result.data = hourlydata;
        $scope.legends = [];
        highchart.drawGraph(result, $scope, true);
        $scope.loading = false;
      }, { consolidate: "daily", aggregate: "none", breakdown: false }, false, function (result, status) {
        $scope.errorMessage = "Error: " + status;
        $scope.loading = false;
      });
    }, { consolidate: "monthly", aggregate: "data", breakdown: true }, false, function (result, status) {
      $scope.errorMessage = "Error: " + status;
      $scope.loading = false;
    });
    $scope.legendName = $scope.groupBy.name;
  }

  $scope.nextGroupBy = function (groupBy) {
    for (var i in $scope.groupBys) {
      if ($scope.groupBys[i].name === groupBy) {
        var j = (parseInt(i) + 1) % $scope.groupBys.length;
        return $scope.groupBys[j].name;
      }
    }
  }

  $scope.accountsEnabled = function () {
    $scope.updateAccounts($scope);
  }
  
  $scope.regionsEnabled = function () {
    $scope.updateRegions($scope);
  }
  
  $scope.productsEnabled = function () {
    $scope.updateProducts($scope);
  }
  
  $scope.operationsEnabled = function () {
    $scope.updateOperations($scope);
  }
  
  $scope.usageTypesEnabled = function () {
    $scope.updateUsageTypes($scope);
  }

  $scope.filterAccount = function (filter_accounts) {
    return usage_db.filterAccount($scope, filter_accounts);
  }

  $scope.orgUnitChanged = function () {
    usage_db.updateOrganizationalUnit($scope);
    $scope.accountsChanged();
  }

  $scope.accountsChanged = function () {
    $scope.updateRegions($scope);
  }

  $scope.regionsChanged = function () {
    $scope.updateProducts($scope);
  }

  $scope.productsChanged = function () {
    $scope.updateOperations($scope);
  }

  $scope.operationsChanged = function () {
    $scope.updateUsageTypes($scope);
  }

  usage_db.getParams($location.hash(), $scope);

  usage_db.getAccounts($scope, function (data) {
    $scope.updateRegions($scope);
  });

  $scope.getData();
}

function resourceInfoCtrl($scope, $location, $http) {
  $scope.resource = {};

  $scope.isDisabled = function () {
    return !$scope.resource.name ;
  }

  $scope.getResourceInfo = function() {
    $scope.resourceInfo = "";
    
    $http({
      method: "GET",
      url: "instance",
      params: { id: $scope.resource.name }
    }).success(function (result) {
        $scope.resourceInfo = JSON.stringify(result, null, "    ");
    }).error(function(result, status) {
      if (status === 401 || status === 0)
        $window.location.reload();
      else if (status === 404)
        $scope.resourceInfo = "Resource " + $scope.resource.name + " does not exist.";
      else
        $scope.resourceInfo = "Error getting resource " + $scope.resource.name + ": " + status;
    });
  
  }
}

function accountsCtrl($scope, $location, $http) {
  $scope.accounts = [];
  $scope.revs = {
    name: false,
    awsName: false,
    id: false,
    status: false
  };
  $scope.predicate = null;

  $scope.order = function (data, name) {

    if ($scope.predicate != name) {
      $scope.rev = $scope.revs[name];
      $scope.predicate = name;
    }
    else {
      $scope.rev = $scope.revs[name] = !$scope.revs[name];
    }

    var compare = function (a, b) {
      if (a[name] < b[name])
        return $scope.rev ? 1 : -1;
      if (a[name] > b[name])
        return $scope.rev ? -1 : 1;
      return 0;
    }

    data.sort(compare);
  }

  var getAccounts = function ($scope, fn, download) {
    var params = { all: true };

    if (download) {
      params["dashboard"] = "accounts";
      jQuery("#download_form").empty();

      for (var key in params) {
        jQuery("<input type='text' />")
          .attr("id", key)
          .attr("name", key)
          .attr("value", params[key])
          .appendTo(jQuery("#download_form"));
      }

      jQuery("#download_form").submit();
    }
    else {
      $http({
        method: "GET",
        url: "getAccounts",
        params: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          $scope.accounts = result.data;
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    }
  }
  
  $scope.download = function () {
    getAccounts($scope, null, true);
  }

  getAccounts($scope, function (data) {
    for (var i = 0; i < $scope.accounts.length; i++) {
      var account = $scope.accounts[i];
      var parents = account.parents;
      if (!parents)
        account.path = "Unlinked";
      else
        account.path = parents.length > 0 ? account.parents.join("/") : "";
      if (account.awsName === null)
        account.awsName = "";
      if (account.status === null)
        account.status = "";

      account.accessGroupsStr = "";
      var accessGroups = account.accessGroups;
      if (accessGroups && accessGroups.length > 0)
        account.accessGroupsStr = accessGroups.join(", ");

      account.tagsStr = "";
      var tags = account.tags;
      if (tags) {
        var tagArray = [];
        for (var j in tags)
          tagArray.push(j + "=" + tags[j])
        account.tagsStr = tagArray.join(", ");
      }
    }
    $scope.order($scope.accounts, 'name');
  });
}

function tagconfigsCtrl($scope, $location, $http) {
  $scope.payers = [];
  $scope.tagConfigs = {};
  $scope.mappedValues = {};
  $scope.consolidations = {};
  $scope.revs = {
    destKey: false,
    destValue: false,
    srcKey: false,
    srcValue: false,
    key: false,
    value: false
  };
  $scope.predicate = null;

  $scope.order = function (data, name) {

    if ($scope.predicate != name) {
      $scope.rev = $scope.revs[name];
      $scope.predicate = name;
    }
    else {
      $scope.rev = $scope.revs[name] = !$scope.revs[name];
    }

    var compare = function (a, b) {
      if (a[name] < b[name])
        return $scope.rev ? 1 : -1;
      if (a[name] > b[name])
        return $scope.rev ? -1 : 1;
      return 0;
    }

    data.sort(compare);
  }

  var getTagconfigs = function ($scope, fn) {
    var params = {};

    $http({
      method: "GET",
      url: "getTagConfigs",
      params: params
    }).success(function (result) {
      if (result.status === 200 && result.data) {
        $scope.tagConfigs = result.data;
        if (fn)
          fn(result.data);
      }
    }).error(function (result, status) {
      if (status === 401 || status === 0)
        $window.location.reload();
    });
  }

  getTagconfigs($scope, function (data) {
    $scope.mappedValues = {};
    var tagConfigsForPayer;
    var values;
    var tagConsolidations;
    Object.keys($scope.tagConfigs).forEach(function(payer) {
      $scope.payers.push(payer);
      tagConfigsForPayer = $scope.tagConfigs[payer];
      values = [];
      $scope.mappedValues[payer] = values;
      tagConsolidations = [];
      $scope.consolidations[payer] = tagConsolidations;
      var tagConfigsForDestKey;
      Object.keys(tagConfigsForPayer).forEach(function(destKey) {
        tagConfigsForDestKey = tagConfigsForPayer[destKey];
        if (tagConfigsForDestKey.mapped) {
          // handle mappings
          var tagConfigsForMapsItem;
          Object.keys(tagConfigsForDestKey.mapped).forEach(function(i) {
            tagConfigsForMapsItem = tagConfigsForDestKey.mapped[i];
            if (tagConfigsForMapsItem.maps) {
              var tagConfigsForDestValue;
              var filter = 'None';
              if (tagConfigsForMapsItem.include) {
                filter = 'Include: ' + tagConfigsForMapsItem.include.join(", ");
              }
              if (tagConfigsForMapsItem.exclude) {
                filter = 'Exclude: ' + tagConfigsForMapsItem.exclude.join(", ");
              }
              Object.keys(tagConfigsForMapsItem.maps).forEach(function(destValue) {
                tagConfigsForDestValue = tagConfigsForMapsItem.maps[destValue];
                var tagConfigsForSrcKey;
                Object.keys(tagConfigsForDestValue).forEach(function(srcKey) {
                  var srcValues = tagConfigsForDestValue[srcKey];
                  for (var i = 0; i < srcValues.length; i++) {
                    values.push({
                      destKey: destKey,
                      destValue: destValue,
                      srcKey: srcKey,
                      srcValue: srcValues[i],
                      start: tagConfigsForMapsItem.start,
                      filter: filter
                    });
                  }
                });  
              });
            }
          });
        }
        if (tagConfigsForDestKey.values) {
          // handle consolidations
          Object.keys(tagConfigsForDestKey.values).forEach(function(value) {
            var valueAliases = tagConfigsForDestKey.values[value];
            tagConsolidations.push({
              key: destKey,
              keyAliases: tagConfigsForDestKey.aliases ? tagConfigsForDestKey.aliases.join(', ') : '',
              value: value,
              valueAliases: valueAliases ? valueAliases.join(', ') : ''
            })
          });
        }
        else if (tagConfigsForDestKey.aliases) {
          tagConsolidations.push({
            key: destKey,
            keyAliases: tagConfigsForDestKey.aliases.join(', '),
            value: '',
            valueAliases: ''
          })
        }
      });
      $scope.order(values, 'srcValue');
      $scope.order(values, 'srcKey');
      $scope.order(values, 'destValue');
      $scope.order(values, 'destKey');
    });
  });

  getTagconfigs($scope, function (data) {
  });
}

function processorStatusCtrl($scope, $location, $http) {
  $scope.statusArray = [];
  $scope.processorState = "unknown";

  var getProcessorStatus = function ($scope, fn) {
    var params = {};

    $http({
      method: "GET",
      url: "getProcessorStatus",
      params: params
    }).success(function (result) {
      if (result.status === 200 && result.data) {
        $scope.statusArray = result.data;
        if (fn)
          fn(result.data);
      }
    }).error(function (result, status) {
      if (status === 401 || status === 0)
        $window.location.reload();
    });
  }

  var setReprocessStatus = function ($scope, index, fn) {
    var params = {
      month: $scope.statusArray[index].month,
      state: $scope.statusArray[index].reprocess,
    };
    $http({
      method: "POST",
      url: "setReprocess",
      data: params
    }).success(function (result) {
      if (result.status === 200) {
        if (fn)
          fn();
      }
    }).error(function (result, status) {
      if (status === 401 || status === 0)
        $window.location.reload();
    });
  }

  $scope.updateStatus = function (index) {
    setReprocessStatus($scope, index, function (data) {
      getProcessorStatus($scope, function (data) {
        getProcessorState($scope);
      });
    });
  }

  $scope.isDisabled = function () {
    if ($scope.processorState != "stopped")
      return true;

    for (var i = 0; i < $scope.statusArray.length; i++) {
      if ($scope.statusArray[i].reprocess)
        return false;
    }
    return true;  
  }

  $scope.refresh = function () {
    refreshState($scope);
    if ($scope.processorState === "stopped")
      getProcessorStatus($scope);
  }

  var refreshState = function($scope) {
    getProcessorState($scope, function () {
      if ($scope.processorState === "pending")
        setTimeout(() => refreshState($scope), 5000);
    });
  }

  $scope.process = function() {
    startProcessor($scope, function () {
      $scope.processorState = "pending";
      setTimeout(() => refreshState($scope), 2000);
    });
  }

  var startProcessor = function($scope, fn) {
    $http({
      method: "POST",
      url: "startProcessor",
      data: {}
    }).success(function (result) {
      if (result.status === 200) {
        if (fn)
          fn();
      }
    }).error(function (result, status) {
      if (status === 401 || status === 0)
        $window.location.reload();
    });
  }

  var getProcessorState = function($scope, fn) {
    $http({
      method: "GET",
      url: "getProcessorState",
      data: {}
    }).success(function (result) {
      if (result.status === 200 && result.data) {
        $scope.processorState = result.data;
        if (fn)
          fn();
      }
    }).error(function (result, status) {
      if (status === 401 || status === 0)
        $window.location.reload();
    });
  }

  getProcessorStatus($scope, function (data) {
    getProcessorState($scope);
  });
}

function subscriptionsCtrl($scope, $location, $http) {
  $scope.ri_sp = "RI";
  $scope.month = "";
  $scope.months = [];
  $scope.header = [];
  $scope.subscriptions = [];

  $scope.rev = false;
  $scope.revs = [];
  $scope.predicateIndex = null;

  $scope.order = function (index) {

    if ($scope.predicateIndex != index) {
      $scope.rev = $scope.revs[index];
      $scope.predicateIndex = index;
    }
    else {
      $scope.rev = $scope.revs[index] = !$scope.revs[index];
    }

    var compare = function (a, b) {
      if (a[index] < b[index])
        return $scope.rev ? 1 : -1;
      if (a[index] > b[index])
        return $scope.rev ? -1 : 1;
      return 0;
    }

    $scope.subscriptions.sort(compare);
  }

  var getMonths = function($scope, fn) {
    $http({
      method: "GET",
      url: "getMonths",
      params: {}
    }).success(function (result) {
      if (result.status === 200 && result.data) {
        if (fn)
          fn(result.data);
      }
    }).error(function (result, status) {
      if (status === 401 || status === 0)
        $window.location.reload();
    });
  }

  var getSubscriptions = function ($scope, fn, download) {
    var params = {
      type: $scope.ri_sp,
      month: $scope.month,
    };

    if (download) {
      params["dashboard"] = "subscriptions";
      params["type"] = $scope.ri_sp;
      jQuery("#download_form").empty();

      for (var key in params) {
        jQuery("<input type='text' />")
          .attr("id", key)
          .attr("name", key)
          .attr("value", params[key])
          .appendTo(jQuery("#download_form"));
      }

      jQuery("#download_form").submit();
    }
    else {
      $http({
        method: "GET",
        url: "getSubscriptions",
        params: params
      }).success(function (result) {
        if (result.status === 200 && result.data) {
          if (fn)
            fn(result.data);
        }
      }).error(function (result, status) {
        if (status === 401 || status === 0)
          $window.location.reload();
      });
    }
  }
  
  $scope.update = function () {
    getSubscriptions($scope, function (data) {
      loadData($scope, data);
    })
  }

  $scope.download = function () {
    getSubscriptions($scope, null, true);
  }

  var loadData = function($scope, data) {
    $scope.header = data[0];
    data.shift();
    $scope.subscriptions = data;
    $scope.revs = [];
    for (var i = 0; i < $scope.subscriptions.length; i++)
      $scope.revs.push(false);
    $scope.order($scope.subscriptions, 0);
}

  getMonths($scope, function (data) {
    $scope.months = data;
    $scope.month = $scope.months[$scope.months.length-1];

    getSubscriptions($scope, function (data) {
      loadData($scope, data);
    });
  });
}