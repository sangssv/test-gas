/**
 * Runs a BigQuery query and logs the results in a spreadsheet.
 */

function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('POG')
  .addItem('Get product master', 'fillProductMasterByCategories')
  .addItem('Dialog', 'showDialog')
  .addItem('Sidebar', 'showSidebar')
  .addToUi();
}
function showDialog() {
  var html = HtmlService.createHtmlOutputFromFile('page')
      .setWidth(900)
      .setHeight(500);
  SpreadsheetApp.getUi() // Or DocumentApp or SlidesApp or FormApp.
      .showModalDialog(html, 'My custom dialog');
}
function showSidebar() {
  var html = HtmlService.createHtmlOutputFromFile('page')
      .setTitle('My custom sidebar')
      .setWidth(900);
  SpreadsheetApp.getUi() // Or DocumentApp or SlidesApp or FormApp.
      .showSidebar(html);
}

function findImage(productImageFolder, upc) {
  if (upc !== "") {
   var imgs = productImageFolder.searchFiles('title contains "' + upc + '"');
    var img;
    if (imgs.hasNext()) {
      img = imgs.next();
      var imgId = img.getId();
      var url = "https://docs.google.com/uc?export=download&id=" + imgId;
      return url;
    }
  }
  
  return "";
}

function mergeObjects() {
    var resObj = {};
    for(var i=0; i < arguments.length; i += 1) {
         var obj = arguments[i],
             keys = Object.keys(obj);
         for(var j=0; j < keys.length; j += 1) {
             resObj[keys[j]] = obj[keys[j]];
         }
    }
    return resObj;
}

function getCategory() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var pogSheet = spreadsheet.getSheetByName("POG");
  var categoryNo = spreadsheet.getRange("D2").getValue();
  Logger.log(categoryNo);
  var category = pogSheet.getRange("K2:Q2").getValue();
  
  return categoryNo + " " + category;
}

function getPOGData() {
  var productImageFolder = DriveApp.getFolderById('11Y4ApUgxBZHsjCOU0Tokei_pfy_J1Ggz');
  var shelfRow = 9;
  var headerIndexes = {};
  var pogMetaData = {};
  var pogMetaDataIndex = 0;
  
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var productListSheet = spreadsheet.getSheetByName("PRODUCT LIST");
  var dataRange = productListSheet.getDataRange();
  var colNum = dataRange.getNumColumns();
  var data = dataRange.getDisplayValues();
  
  for (var i = 0; i < colNum; i++){
    // Table header start from 3rd row
    headerIndexes[i] = data[2][i];
  }
  
  var productData = data.slice(3)
    .map(function(item) {
      return item.reduce(function(acc, cur, i) {
        acc[headerIndexes[i]] = cur;
        return acc;
      }, {});
    });
  
  var returnData = productData.reduce(function(acc, cur) {
    if(cur["Product ID"] !== "") {
      acc[cur["Shelf"]] = (acc[cur["Shelf"]] || []).concat(mergeObjects(cur, { "Product Image": findImage(productImageFolder, cur["UPC"]) }));
      // acc[cur["Shelf"]] = (acc[cur["Shelf"]] || []).concat(cur);
    }
 
    return acc; 
  }, {});
  
  return returnData;
}
function getCategoryId() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var pogSheet = spreadsheet.getSheetByName("POG");
  var categoryId = pogSheet.getRange(2, 21).getValue();
  return categoryId;
}
function fillProductMasterByCategories() {   
  //Get category from range U2
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var pogSheet = spreadsheet.getSheetByName("POG");
  var categoryIdList = pogSheet.getRange(2, 21).getValue();
  
  var request = {
    query: "SELECT product_id, Short_Name, group_no, group_name, Category_name, Sub_category_name, Manufacturer_Name, Brand_Name, Minimum_Display_Quantity, Expiration_Shelf_Life_Unit, Total_Shelf_Life, Delivery_Limitation, Sales_Limitation, Status, Uom_Upc, Uom_Name, Lot_Size, Sale_Area, Supplier_name, Core_Item, Purchase_Price_Without_Tax, Minimum_Order_Quantity, Store_Orderable, Retail_Selling_Price_With_Tax " +
    "FROM `civil-clarity-205812.planogram.pog_product_master_view` WHERE category_no IN ('" + categoryIdList + "');",
    "useLegacySql": false
  };
  
  var rows = getDataReportFromBigQuery(request);
  
  if(rows) {
    var data = convertDataToArray(rows);
    var productMasterSheet = spreadsheet.getSheetByName("PRODUCT MASTER");
    if (!productMasterSheet){
      productMasterSheet = spreadsheet.insertSheet("PRODUCT MASTER");
    }
    //Create sheet Latest
    var headers = ["Product ID","Short Name","Product Group ID","Product Group","Category","Sub-category","Manufacturer Name","Brand Name","Min Display Quantity",
                   "Expiration Shelf Life Unit","Total Shelf Life","Delivery Limitation","Sale Limitation","Status","Uom Upc","Uom Name","Lot Size","Sale Area","Supplier",
                   "Core Item","Purchase Price Without Tax","Minimum Order Quantity","Store Orderable","Retail Selling Price With Tax"];
    
    productMasterSheet.clearContents();
    
    productMasterSheet.appendRow(headers);
    productMasterSheet.getRange(2,1,data.length, data[0].length).setValues(data);
  }
  
}
function getDataReportFromBigQuery(request) {
  var projectId = 'civil-clarity-205812';
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Get all the rows of results.
  var rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }
  return rows;
}
function convertDataToArray(rows) {
  var data = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    };
  return data;
}