/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

$(document).ready(function() {
   //rebuild array of poolInfos from options
   var poolInfoArray = [];
   $("#poolInfoSelect option").each(function(){ poolInfoArray.push($(this).val()); });

   // Multi select objects
   $("#poolGroupSelect").multiselect({
       noneSelectedText: 'Select pool group(s)',
       minWidth: "300",
   });

   var autocompleting = false;
   var updatePoolInfoSelect = function()
      {
        if (autocompleting) return; else autocompleting = true;
        var terms = split( $( "#poolInfoInput" ).val() );
        var termTable = {};

        //build table for efficiency
        for (var i = 0; i < terms.length; i++)
           termTable[terms[i]] = true;

        $('#poolInfoSelect')
           .multiselect('uncheckAll')
           .multiselect('widget').find(':checkbox').each(function(){
              if (this.value in termTable)
                 this.click();
           });
        autocompleting = false;
      }

   var poolInfoSelectChanged = function(){
      if (autocompleting) return;
      var checkedValues = $("#poolInfoSelect").multiselect("getChecked").map(function(){
         return this.value;
      }).get();
      checkedValues.push("");
      $("#poolInfoInput").val(checkedValues.join(", "));
   };

   $("#poolInfoSelect").multiselect({
       noneSelectedText: '',
       minWidth: "300",
   }).bind("multiselectclick",poolInfoSelectChanged)
   .bind("multiselectcheckall",poolInfoSelectChanged)
   .bind("multiselectuncheckall",poolInfoSelectChanged)
   .bind("multiselectbeforeopen",updatePoolInfoSelect);

   function split( val ) {
      return val.split( /,\s*/ );
   }
   function extractLast( term ) {
      return split( term ).pop();
   }

   jQuery.fn.dataTableExt.aTypes.push(
     function(sData) {
       var reg = /^[0-9.]+ (K|M|G)?B$/;
       if (reg.test(sData)) {
         return "memory_usage";
       }

       return null;
     }
   );


   jQuery.fn.dataTableExt.oSort['memory_usage-asc'] = function(x,y) {
     return cmpMemUsage(x, y, 1);
   };

   jQuery.fn.dataTableExt.oSort['memory_usage-desc'] = function(x,y) {
     return cmpMemUsage(x, y, -1);
   };


   $( "#poolInfoInput" )
      .bind( "keydown", function( event ) {
         if ( event.keyCode === $.ui.keyCode.TAB &&
            $( this ).data( "autocomplete" ).menu.active ) {
               event.preventDefault();
         }
      })
      .autocomplete({
         minLength: 0,
         source: function( request, response ) {
            // delegate back to autocomplete, but extract the last term
            response( $.ui.autocomplete.filter(
               poolInfoArray, extractLast( request.term ) ) );
         },
         focus: function() {
            // prevent value inserted on focus
            return false;
         },
         select: function( event, ui ) {
            var terms = split( this.value );
            // remove the current input
            terms.pop();
            // add the selected item
            terms.push( ui.item.value );
            // add placeholder to get the comma-and-space at the end
            terms.push( "" );
            this.value = terms.join( ", " );
            return false;
         }
     });

  // Buttons
  $("button").button();

  //pretty up the toolbar
  $('#toolbar').children().each(function(){this.style.verticalAlign = 'middle'});

  // Add the filtering redirect
  $("#addFilter").click(function() {
    updatePoolInfoSelect();

    var poolGroupValues = $("#poolGroupSelect").val();
    var poolInfoValues = $("#poolInfoSelect").val();

    var queryStringQuery = window.location.href.split('?')[0];
    var queryStringLink = window.location.href.split('#')[0];
    var queryString = "";
    if (queryStringQuery.length < queryStringLink.length) {
      queryString = queryStringQuery;
    } else {
      queryString = queryStringLink;
    }

    if (poolGroupValues == null && poolInfoValues == null) {
      // do nothing
    } else {
      queryString += "?";
      if (poolGroupValues != null) {
        queryString += "poolGroups=" + poolGroupValues + "&";
      }
      if (poolInfoValues != null) {
        queryString += "poolInfos=" + poolInfoValues + "&";
      }
    }
    location.href = queryString;
  });

  // Make all tables data tables
  $("#summaryTable").dataTable({
    "bJQueryUI": true,
    "bPaginate": false,
    "bSearchable": false,
    "bSortClasses": false,
    "sScrollX": "100%",
    "bScrollCollapse": true,
  });
  $("#activeTable").dataTable({
    "bJQueryUI": true,
    "bPaginate": true,
    "bSortClasses": false,
    "bStateSave": false,
    "sScrollX": "100%",
    "bScrollCollapse": true,
    "iDisplayLength": 10,
    "aLengthMenu": [[10, 25, 50, 100, -1],[10, 25, 50, 100, "All"]],
    "bProcessing": true,
    "fnServerParams": function (aoData) {
        aoData.push({
            "name": "users",
            "value": getParameterByName("users")});
        aoData.push({
            "name": "poolGroups",
            "value": getParameterByName("poolGroups")});
        aoData.push({
            "name" : "poolInfos",
            "value": getParameterByName("poolInfos")});
        aoData.push({
            "name": "killSessionsToken",
            "value": getParameterByName("killSessionsToken")});
    },
    "sAjaxSource": "/active_json.jsp",
  });
  $("#poolTable").dataTable({
    "bJQueryUI": true,
    "bPaginate": false,
    "bSortClasses": false,
    "bStateSave": false,
    "sScrollX": "100%",
    "bScrollCollapse": true,
    "bProcessing": true,
    "fnServerParams": function (aoData) {
        aoData.push({
            "name": "users",
            "value": getParameterByName("users")});
        aoData.push({
            "name": "poolGroups",
            "value": getParameterByName("poolGroups")});
        aoData.push({
            "name" : "poolInfos",
            "value": getParameterByName("poolInfos")});
    },
    "sAjaxSource": "/pool_json.jsp",
  });
  $("#retiredTable").dataTable({
    "bJQueryUI": true,
    "bPaginate": true,
    "bSortClasses": false,
    "bStateSave": false,
    "sScrollX": "100%",
    "bScrollCollapse": true,
    "iDisplayLength": 10,
    "aLengthMenu": [[10, 25, 50, 100, -1],[10, 25, 50, 100, "All"]],
    "bProcessing": true,
    "fnServerParams": function (aoData) {
        aoData.push({
            "name": "users",
            "value": getParameterByName("users")});
        aoData.push({
            "name": "poolGroups",
            "value": getParameterByName("poolGroups")});
        aoData.push({
            "name" : "poolInfos",
            "value": getParameterByName("poolInfos")});
    },
    "sAjaxSource": "/retired_json.jsp",
  });

  // Hide the retired table
  $("#retiredTable").hide();

  // Add toggling for showing the tables
  $("#activeToggle").click(function () {
    $("#activeTable").toggle();
  });
  $("#poolToggle").click(function () {
    $("#poolTable").toggle();
  });
  $("#retiredToggle").click(function () {
    $("#retiredTable").toggle();
  });

  $('#switcher').themeswitcher({
    loadTheme: "UI lightness"
  });

  $("#poolInfoSelect + button").first().width(20);

  // Add the kill session handler
  $("#killSession").click(function () {
    var toKillSessions = "";
    $('.case:checked').each(function () {
      toKillSessions += $(this).val() + " ";
    });

    if (toKillSessions.length != 0) {
      var confirmMessage = "Are you sure to kill this session?";
      if ($('.case:checked').length > 1) {
        confirmMessage =
          "Are you sure to kill these " + $('.case:checked').length + " sessions?";
      }
      var confirmed = window.confirm(confirmMessage);
      if (!confirmed) {
        return;
      }

      var form = document.createElement("form");
      form.setAttribute("method", "post");
      form.setAttribute("action", window.location.href);

      var field = document.createElement("input");
      field.setAttribute("type", "hidden");
      field.setAttribute("name", "toKillSessionId");
      field.setAttribute("value", toKillSessions);

      form.appendChild(field);
      document.body.appendChild(form);
      form.submit();
      document.body.removeChild(form);
    }

  });
});

function toggle(id) {
  if ( document.getElementById(id).style.display != 'block') {
    document.getElementById(id).style.display = 'block';
  }
  else {
    document.getElementById(id).style.display = 'none';
  }
}

function getParameterByName(name) {
    var match = RegExp('[?&]' + name + '=([^&]*)')
        .exec(window.location.search);
    return match && decodeURIComponent(match[1].replace(/\+/g, ' '));
}

function memUnitToNum(unit) {
  if (unit === "K") {
    return 1024;
  } else if (unit === "M") {
    return 1024*1024;
  } else if (unit === "G") {
    return 1024*1024*1024;
  }

  return 0;
}

function cmpMemUsage(x, y, asc) {
  var memRegExp = "([0-9.]+) (K|M|G)?"

  var match1 = x.match(memRegExp);
  var match2 = y.match(memRegExp);

  if (match1[2] !== match2[2]) {
    unit1 = memUnitToNum(match1[2]);
    unit2 = memUnitToNum(match2[2]);

    return (unit1 < unit2 ? -asc: asc);
  }

  var num1 = Number(match1[1]);
  var num2 = Number(match2[1]);

  if (num1 < num2) {
    return -asc;
  } else if (num1 == num2) {
    return 0;
  } else {
    return asc;
  }
}

