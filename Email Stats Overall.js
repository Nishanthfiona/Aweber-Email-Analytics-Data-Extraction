const CONFIG = {
  AWEBER: {
    CLIENT_ID: '',
    CLIENT_SECRET: '',
    REDIRECT_URI:
      'callback URL',
    SCOPES: [
      'account.read',
      'list.read',
      'email.read',
      'subscriber.read',
      'subscriber.read-extended',
      'email.write',
    ].join(' '),
  },
  SHEET: {
    ID: '',
    OVERALL_SHEET_NAME: 'Email Stats Overall',
    OVERALL_HEADERS: [
      'Year',
      'Month',
      'Total Email Subscribers',
      '% vs previous month',
      'New Subscribers',
      '% vs previous month',
      'Unsubscribes',
      'Unsubscribe Rate (%)',
    ],
    SUBSCRIBER_DETAILS_SHEET_NAME: 'Subscriber Details',
    SUBSCRIBER_DETAILS_HEADERS: [
      'Subscriber ID',
      'Subscribed At',
      'Unsubscribed At',
    ],
  },
};

function updateSubscriberDetails() {
  const startTime = Date.now();
  console.log("Starting subscriber update");
  
  try {
    // Initialize sheet
    const sheet = getOrCreateSheet(
      CONFIG.SHEET.SUBSCRIBER_DETAILS_SHEET_NAME,
      CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS
    );
    
    // Get auth tokens
    const token = getValidAccessToken();
    const accountId = getAWeberAccountId(token);
    const listIds = getListIds(accountId, token);
    
    // Determine sync mode
    const isFirstRun = sheet.getLastRow() <= 1;
    if (isFirstRun) {
      console.log("First run - bulk loading");
      bulkLoadSubscribers(accountId, listIds, token, sheet);
      // Set initial sync timestamp
      PropertiesService.getScriptProperties()
        .setProperty('lastSyncTimestamp', Date.now());
    } else {
      console.log("Incremental update mode");
      incrementalSubscriberUpdate(accountId, listIds, token, sheet);
    }
    
    // Calculate stats (only if we have data)
    if (sheet.getLastRow() > 1) {
      calculateOverallStatsFromSheet();
    }
    
    console.log(`Update completed in ${(Date.now()-startTime)/1000}s`);
  } catch (e) {
    console.error(`Update error: ${e.message}`);
  }
}

function prepareSubscriberSheet(sheet) {
  // Set column formats explicitly
  sheet.getRange("A:A").setNumberFormat("@"); // Text format for IDs
  sheet.getRange("B:C").setNumberFormat("yyyy-mm-dd hh:mm:ss");
  
  // Freeze header row
  sheet.setFrozenRows(1);
  
  // Set header style
  sheet.getRange(1, 1, 1, 3)
    .setFontWeight("bold")
    .setBackground("#f0f0f0");
}

function bulkLoadSubscribers(accountId, listIds, token, sheet) {
  const BATCH_SIZE = 100;
  let totalProcessed = 0;
  
  // Initialize sheet properly
  if (sheet.getLastRow() <= 1) {
    sheet.clear();
    sheet.appendRow(CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS);
  }

  for (const listId of listIds) {
    let nextUrl = `https://api.aweber.com/1.0/accounts/${accountId}/lists/${listId}/subscribers?ws.op=find&ws.size=${BATCH_SIZE}`;
    
    while (nextUrl) {
      try {
        const response = fetchApi(nextUrl, token);
        
        // Properly check response structure
        if (!response || !response.entries || !Array.isArray(response.entries)) {
          console.log("Invalid API response structure", response);
          break;
        }

        // Process only if we have entries
        if (response.entries.length > 0) {
          // Write batch with EXACT dimensions
          const targetRange = sheet.getRange(
            sheet.getLastRow() + 1, 
            1, 
            response.entries.length, 
            3
          );
          const data = response.entries.map(sub => [
            sub.id.toString(),
            formatDateForSheet(sub.subscribed_at),
            sub.unsubscribed_at ? formatDateForSheet(sub.unsubscribed_at) : ''
          ]);
          
          targetRange.setValues(data);
          totalProcessed += response.entries.length;
          console.log(`Processed ${totalProcessed} subscribers`);
        }

        // Handle pagination
        nextUrl = response.next_collection_link;
        Utilities.sleep(1500); // Respectful delay between requests
        
      } catch (e) {
        console.error(`Error processing batch: ${e.message}`);
        console.error('Last URL: ' + nextUrl);
        break;
      }
    }
  }
  
  console.log(`Completed loading ${totalProcessed} subscribers`);
  return totalProcessed;
}

function incrementalSubscriberUpdate(accountId, listIds, token, sheet) {
  const scriptProps = PropertiesService.getScriptProperties();
  let lastSync = scriptProps.getProperty('lastSyncTimestamp');
  
  // Set default to 24 hours ago if no timestamp exists
  if (!lastSync || lastSync < 100000000000) { // Older than 2001
    lastSync = Date.now() - 86400000; // 24 hours ago
    console.log("Resetting invalid timestamp to 24 hours ago");
    scriptProps.setProperty('lastSyncTimestamp', lastSync);
  } else {
    lastSync = parseInt(lastSync);
  }

  console.log(`Fetching changes since ${new Date(lastSync).toISOString()}`);
  
  // Get existing data with proper formatting
  const existingData = getExistingSubscriberData();
  console.log(`Loaded ${Object.keys(existingData).length} existing records`);

  const updates = [];
  const newRows = [];
  let processedCount = 0;

  for (const listId of listIds) {
    let nextUrl = `https://api.aweber.com/1.0/accounts/${accountId}/lists/${listId}/subscribers?ws.op=find&ws.size=100&subscribed_since=${new Date(lastSync).toISOString()}`;
    
    while (nextUrl) {
      const response = fetchApi(nextUrl, token);
      if (response?.entries) {
        response.entries.forEach(sub => {
          processedCount++;
          const existing = existingData[sub.id];
          const apiUnsub = sub.unsubscribed_at ? formatDateForSheet(sub.unsubscribed_at) : '';
          
          if (existing) {
            // Only update if unsubscribe status changed AND is more recent
            const sheetUnsub = existing.unsubscribed_at || '';
            if (sheetUnsub !== apiUnsub) {
              updates.push({
                rowIndex: existing.rowIndex,
                data: [
                  sub.id.toString(),
                  formatDateForSheet(sub.subscribed_at),
                  apiUnsub
                ]
              });
            }
          } else {
            // New subscriber
            newRows.push([
              sub.id.toString(),
              formatDateForSheet(sub.subscribed_at),
              apiUnsub
            ]);
          }
        });
        
        nextUrl = response.next_collection_link;
        Utilities.sleep(1000);
      } else {
        nextUrl = null;
      }
    }
  }

  // Apply changes in batches to avoid timeouts
  if (updates.length > 0) {
    console.log(`Applying ${updates.length} updates`);
    const batchSize = 100;
    for (let i = 0; i < updates.length; i += batchSize) {
      const batch = updates.slice(i, i + batchSize);
      batch.forEach(update => {
        sheet.getRange(update.rowIndex, 1, 1, 3).setValues([update.data]);
      });
      Utilities.sleep(1000);
    }
  }
  
  if (newRows.length > 0) {
    console.log(`Adding ${newRows.length} new subscribers`);
    sheet.getRange(
      sheet.getLastRow() + 1,
      1,
      newRows.length,
      newRows[0].length
    ).setValues(newRows);
  }

  // Update sync timestamp
  scriptProps.setProperty('lastSyncTimestamp', Date.now());
  console.log(`Incremental update complete. Processed: ${processedCount}, Updates: ${updates.length}, New: ${newRows.length}`);
}

function getIncrementalSubscribersData(accountId, listIds, token, sinceTimestamp) {
  let allSubscribers = [];
  const startTime = Date.now();
  const MAX_EXECUTION_TIME = 250000; // 4 minutes 10 seconds
  
  for (const listId of listIds) {
    let nextUrl = `https://api.aweber.com/1.0/accounts/${accountId}/lists/${listId}/subscribers?ws.op=find&ws.size=50&subscribed_since=${new Date(sinceTimestamp).toISOString()}`;
    
    while (nextUrl && (Date.now() - startTime < MAX_EXECUTION_TIME)) {
      const response = fetchApi(nextUrl, token);
      if (response && Array.isArray(response.entries)) {
        allSubscribers = allSubscribers.concat(response.entries);
        nextUrl = response.next_collection_link;
        Utilities.sleep(1000); // 1 second between requests
      } else {
        break;
      }
    }
  }
  return allSubscribers;
}


function getAllSubscribersData(accountId, listIds, token, maxTime = 240000) {
  let allSubscribers = [];
  const startTime = Date.now();
  
  for (const listId of listIds) {
    let nextUrl = `https://api.aweber.com/1.0/accounts/${accountId}/lists/${listId}/subscribers?ws.op=find&ws.size=100`;
    
    while (nextUrl && (Date.now() - startTime < maxTime)) {
      const response = fetchApi(nextUrl, token);
      if (response && Array.isArray(response.entries)) {
        allSubscribers = allSubscribers.concat(response.entries);
        nextUrl = response.next_collection_link;
        Utilities.sleep(1000);
      } else {
        break;
      }
    }
  }
  return allSubscribers;
}


function calculateOverallStatsFromSheet() {
  try {
    const sheet = getOrCreateSheet(
      CONFIG.SHEET.SUBSCRIBER_DETAILS_SHEET_NAME,
      CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS
    );
    
    // Get data and filter out invalid rows
    const rawData = sheet.getDataRange().getValues();
    console.log("Raw data rows:", rawData.length);
    
    if (rawData.length <= 1) {
      console.log("No subscriber data available");
      return;
    }
    
    const validData = rawData.slice(1).filter(row => row[0] && row[1]);
    console.log("Valid data rows:", validData.length);
    
    const overallData = analyzeSubscriberData(validData);
    console.log("Overall data points:", overallData.length);
    
    if (overallData.length === 12) {
      writeOverallDataToSheet(overallData);
    } else {
      console.error("Unexpected data length from analysis:", overallData.length);
    }
  } catch (e) {
    console.error(`Error in calculateOverallStatsFromSheet: ${e.message}`);
  }
}

function writeOverallDataToSheet(data) {
  const sheet = getOrCreateOverallSheet();
  
  // 1. Nuclear reset with full synchronization
  sheet.clearContents();
  sheet.appendRow(CONFIG.SHEET.OVERALL_HEADERS);
  SpreadsheetApp.flush(); // Force sync
  
  // 2. Write data row-by-row with atomic operations
  data.forEach((item, index) => {
    const row = [
      item.year,
      item.month,
      item.totalSubscribers,
      item.percentVsPreviousMonth / 100,
      item.newSubscribers,
      item.newPercentVsPreviousMonth / 100,
      item.unsubscribes,
      item.unsubscribeRate / 100
    ];
    
    // 3. Direct cell-by-cell writing
    const targetRow = index + 2;
    row.forEach((value, col) => {
      sheet.getRange(targetRow, col + 1)
        .setValue(value)
        .setNumberFormat(col === 1 ? "mmmm" : 
          col === 3 || col === 5 || col === 7 ? "0.00%" : 
          "0");
    });
  });
  
  return true;
}

function getExistingSubscriberData() {
  const sheet = getOrCreateSheet(
    CONFIG.SHEET.SUBSCRIBER_DETAILS_SHEET_NAME,
    CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS
  );
  const data = sheet.getDataRange().getValues().slice(1); // Skip headers
  return data.reduce((map, row, index) => {
    map[row[0]] = { // row[0] is subscriber ID
      rowIndex: index + 2, // +2 for header row and 1-based index
      subscribed_at: row[1],
      unsubscribed_at: row[2]
    };
    return map;
  }, {});
}

function getOrCreateSheet(sheetName, headers) {
  const ss = SpreadsheetApp.openById(CONFIG.SHEET.ID);
  let sheet = ss.getSheetByName(sheetName);
  
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setValues([headers])
               .setFontWeight('bold')
               .setBackground('#eeeeee');
  }
  return sheet;
}

function getOrCreateOverallSheet() {
  return getOrCreateSheet(
    CONFIG.SHEET.OVERALL_SHEET_NAME,
    CONFIG.SHEET.OVERALL_HEADERS
  );
}


function mergeSubscriberData(existingData, newSubscribers) {
  const updates = [];
  const newRows = [];
  
  newSubscribers.forEach(subscriber => {
    if (existingData[subscriber.id]) {
      // Check if any data changed
      const existing = existingData[subscriber.id];
      if (existing.subscribed_at !== subscriber.subscribed_at || 
          existing.unsubscribed_at !== (subscriber.unsubscribed_at || '')) {
        updates.push({
          rowIndex: existing.rowIndex,
          data: [
            subscriber.id,
            subscriber.subscribed_at,
            subscriber.unsubscribed_at || ''
          ]
        });
      }
    } else {
      // New subscriber
      newRows.push([
        subscriber.id,
        subscriber.subscribed_at,
        subscriber.unsubscribed_at || ''
      ]);
    }
  });
  
  return { updates, newRows };
}

function writeSubscriberDetailsToSheet(data) {
  const sheet = getOrCreateSheet(
    CONFIG.SHEET.SUBSCRIBER_DETAILS_SHEET_NAME,
    CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS
  );
  
  // Process updates
  if (data.updates.length > 0) {
    data.updates.forEach(update => {
      sheet.getRange(update.rowIndex, 1, 1, 3).setValues([update.data]);
    });
  }
  
  // Process new rows
  if (data.newRows.length > 0) {
    sheet.getRange(
      sheet.getLastRow() + 1,
      1,
      data.newRows.length,
      data.newRows[0].length
    ).setValues(data.newRows);
  }
  
  logOutput(`Updated ${data.updates.length} rows, added ${data.newRows.length} new rows`);
}


function reconcileSubscriberData() {
  const startTime = Date.now();
  console.log("Starting reconciliation");
  
  try {
    // 1. Get existing sheet data FIRST (fast operation)
    const sheet = getOrCreateSheet(
      CONFIG.SHEET.SUBSCRIBER_DETAILS_SHEET_NAME,
      CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS
    );
    const sheetData = sheet.getDataRange().getValues();
    
    // 2. Quick check if sheet is empty (just created)
    if (sheetData.length <= 1) {
      console.log("New sheet detected - skipping reconciliation");
      return 0;
    }
    
    // 3. Get minimal data from AWeber (just IDs)
    const token = getValidAccessToken();
    const accountId = getAWeberAccountId(token);
    const listIds = getListIds(accountId, token);
    
    const aweberIds = new Set();
    for (const listId of listIds) {
      const url = `https://api.aweber.com/1.0/accounts/${accountId}/lists/${listId}/subscribers?ws.op=find&fields=id&ws.size=500`;
      const response = fetchApi(url, token);
      if (response?.entries) {
        response.entries.forEach(sub => aweberIds.add(sub.id));
      }
    }
    
    // 4. Compare with sheet data
    const missingIds = [];
    for (let i = 1; i < sheetData.length; i++) {
      if (!aweberIds.has(sheetData[i][0])) {
        missingIds.push(sheetData[i][0]);
      }
    }
    
    if (missingIds.length > 0) {
      console.log(`Found ${missingIds.length} missing subscribers`);
      markMissingSubscribers(sheet, missingIds);
    }
    
    console.log(`Reconciliation completed in ${(Date.now() - startTime)/1000}s`);
    return missingIds.length;
    
  } catch (e) {
    console.error(`Reconciliation error: ${e.message}`);
    return -1;
  }
}

function writeSubscriberBatch(subscribers, sheet) {
  if (!subscribers || subscribers.length === 0) return;
  
  // Format data with proper types
  const data = subscribers.map(sub => [
    sub.id.toString(), // Force string format
    formatDateForSheet(sub.subscribed_at),
    sub.unsubscribed_at ? formatDateForSheet(sub.unsubscribed_at) : ''
  ]);

  // Get the exact range needed
  const startRow = sheet.getLastRow() <= 1 ? 2 : sheet.getLastRow() + 1;
  const numRows = data.length;
  const numCols = data[0].length;
  
  // Write to exact range size
  const range = sheet.getRange(startRow, 1, numRows, numCols);
  range.setValues(data);
  
  // Apply formatting
  range.setNumberFormats([
    ["@", "yyyy-mm-dd hh:mm:ss", "yyyy-mm-dd hh:mm:ss"]
  ]);
  
  console.log(`Wrote ${numRows} records starting at row ${startRow}`);
}

function formatDateForSheet(isoDate) {
  if (!isoDate) return '';
  
  try {
    // If already formatted, return as-is
    if (typeof isoDate === 'string' && isoDate.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)) {
      return isoDate;
    }
    
    const date = new Date(isoDate);
    return Utilities.formatDate(date, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  } catch (e) {
    console.error(`Error formatting date: ${isoDate}`, e);
    return '';
  }
}

function calculateOverallStatsFromSheet() {
  try {
    const sheet = getOrCreateSheet(
      CONFIG.SHEET.SUBSCRIBER_DETAILS_SHEET_NAME,
      CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS
    );
    
    const rawData = sheet.getDataRange().getValues();
    console.log(`Raw data rows: ${rawData.length}`);
    
    if (rawData.length <= 1) {
      console.log("No subscriber data available");
      return;
    }
    
    const validData = rawData.slice(1).filter(row => row[0] && row[1]);
    console.log(`Valid data rows: ${validData.length}`);
    
    const overallData = analyzeSubscriberData(validData);
    console.log(`Analyzed data points: ${overallData.length}`);
    
    if (!overallData || overallData.length === 0) {
      console.error("No valid data returned from analysis");
      return;
    }
    
    // Additional validation - FIXED SYNTAX ERROR HERE
    if (!Array.isArray(overallData)) {
      console.error("Invalid data format - expected array, got:", typeof overallData);
      return;
    }
    
    const writeSuccess = writeOverallDataToSheet(overallData);
    if (!writeSuccess) {
      console.error("Failed to write data to sheet - check logs for details");
    }
  } catch (e) {
    console.error(`Error in calculateOverallStatsFromSheet: ${e.message}`);
    console.error("Stack trace:", e.stack);
  }
}

function handleMissingSubscribers(missingIds, sheet, sheetData) {
  // Option 1: Mark them as deleted (preferred)
  const missingIdSet = new Set(missingIds);
  const updates = [];
  
  sheetData.forEach((row, index) => {
    if (missingIdSet.has(row[0])) {
      // Update unsubscribed_at if not already set
      if (!row[2] || row[2] === '') {
        updates.push({
          rowIndex: index + 2, // +2 for header and 1-based index
          data: [row[0], row[1], 'MANUAL_DELETE_' + new Date().toISOString()]
        });
      }
    }
  });
  
  // Batch update
  if (updates.length > 0) {
    updates.forEach(update => {
      sheet.getRange(update.rowIndex, 1, 1, 3).setValues([update.data]);
    });
    logOutput(`Marked ${updates.length} missing subscribers as manually deleted`);
  }
  
  /* 
  // Option 2: Delete them from sheet (risky)
  // This would physically remove the rows
  const rowsToDelete = [];
  sheetData.forEach((row, index) => {
    if (missingIdSet.has(row[0])) {
      rowsToDelete.push(index + 2);
    }
  });
  
  // Delete from bottom to top to preserve indices
  rowsToDelete.sort((a,b) => b - a).forEach(row => {
    sheet.deleteRow(row);
  });
  */
}

function analyzeSubscriberData(data) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth();
  const overallData = [];

  // Find date range of our data
  let earliestDate = new Date();
  if (data && data.length > 0) {
    earliestDate = new Date(
      Math.min(...data
        .filter(row => row[1])
        .map(row => new Date(row[1]).getTime())
      )
    );
  }

  // Calculate months to display (1-12)
  const monthsDiff = (currentYear - earliestDate.getFullYear()) * 12 + 
                    (currentMonth - earliestDate.getMonth()) + 1;
  const monthsToShow = Math.min(Math.max(monthsDiff, 1), 12);

  // Process each month
  for (let i = 0; i < monthsToShow; i++) {
    const date = new Date(currentYear, currentMonth - i, 1);
    const year = date.getFullYear();
    const month = date.getMonth();
    const monthName = date.toLocaleString('default', { month: 'long' });
    const startDate = new Date(year, month, 1);
    const endDate = new Date(year, month + 1, 0);

    // Initialize with zero values
    const monthData = {
      year,
      month: monthName,
      totalSubscribers: 0,
      newSubscribers: 0,
      unsubscribes: 0,
      percentVsPreviousMonth: 0,
      newPercentVsPreviousMonth: 0,
      unsubscribeRate: 0
    };

    // Calculate metrics if we have data
    if (data && data.length > 0) {
      monthData.totalSubscribers = data.filter(row => {
        try {
          return new Date(row[1]) <= endDate;
        } catch (e) {
          return false;
        }
      }).length;

      monthData.newSubscribers = data.filter(row => {
        try {
          const subDate = new Date(row[1]);
          return subDate >= startDate && subDate <= endDate;
        } catch (e) {
          return false;
        }
      }).length;

      monthData.unsubscribes = data.filter(row => {
        try {
          return row[2] && new Date(row[2]) >= startDate && new Date(row[2]) <= endDate;
        } catch (e) {
          return false;
        }
      }).length;
    }

    overallData.unshift(monthData);
  }

  // Calculate percentages
  for (let i = 1; i < overallData.length; i++) {
    const prev = overallData[i-1];
    const curr = overallData[i];
    
    curr.percentVsPreviousMonth = prev.totalSubscribers > 0 
      ? ((curr.totalSubscribers - prev.totalSubscribers) / prev.totalSubscribers) * 100 
      : curr.totalSubscribers > 0 ? 100 : 0;
    
    curr.newPercentVsPreviousMonth = prev.newSubscribers > 0
      ? ((curr.newSubscribers - prev.newSubscribers) / prev.newSubscribers) * 100
      : curr.newSubscribers > 0 ? 100 : 0;
    
    curr.unsubscribeRate = curr.totalSubscribers > 0
      ? (curr.unsubscribes / curr.totalSubscribers) * 100
      : 0;
  }

  return overallData;
}


function writeOverallDataToSheet(data) {
  try {
    const sheet = getOrCreateOverallSheet();
    
    // Clear ALL existing data below header
    const lastRow = sheet.getLastRow();
    if (lastRow > 1) {
      sheet.getRange(2, 1, lastRow - 1, CONFIG.SHEET.OVERALL_HEADERS.length).clearContent();
    }
    
    // Convert data to PROPER 2D array format
    const rows = data.map(item => [
      item.year,
      item.month,
      item.totalSubscribers,
      item.percentVsPreviousMonth / 100,
      item.newSubscribers,
      item.newPercentVsPreviousMonth / 100,
      item.unsubscribes,
      item.unsubscribeRate / 100
    ]);

    // Validate we have data to write
    if (rows.length === 0) return true;

    // DEBUG: Verify array structure
    console.log(`Array is 2D: ${Array.isArray(rows[0])}`);
    console.log(`Rows: ${rows.length}, Cols: ${rows[0].length}`);
    
    // Write the data
    sheet.getRange(2, 1, rows.length, rows[0].length)
      .setValues(rows)
      .setNumberFormats([
        ["0", "mmmm", "#,##0", "0.00%", "#,##0", "0.00%", "#,##0", "0.00%"]
      ]);
    
    console.log(`Successfully wrote ${rows.length} months of data`);
    return true;
  } catch (e) {
    console.error("FINAL ERROR in writeOverallDataToSheet:", e.message);
    console.error("Data structure:", JSON.stringify(data.map(item => [
      typeof item.year, 
      typeof item.month,
      typeof item.totalSubscribers
    ])));
    return false;
  }
}

function getOrCreateOverallSheet() {
  const ss = SpreadsheetApp.openById(CONFIG.SHEET.ID);
  let sheet = ss.getSheetByName(CONFIG.SHEET.OVERALL_SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(CONFIG.SHEET.OVERALL_SHEET_NAME);
    sheet.appendRow(CONFIG.SHEET.OVERALL_HEADERS);
    sheet
      .getRange(1, 1, 1, CONFIG.SHEET.OVERALL_HEADERS.length)
      .setFontWeight('bold')
      .setBackground('#eeeeee');
    logOutput(`Created new sheet: ${CONFIG.SHEET.OVERALL_SHEET_NAME}`);
  }
  return sheet;
}

// OAuth Service Functions
function getAWeberService() {
  const service = OAuth2.createService('AWeber')
    .setAuthorizationBaseUrl('https://auth.aweber.com/oauth2/authorize')
    .setTokenUrl('https://auth.aweber.com/oauth2/token')
    .setClientId(CONFIG.AWEBER.CLIENT_ID)
    .setClientSecret(CONFIG.AWEBER.CLIENT_SECRET)
    .setCallbackFunction('authCallback')
    .setPropertyStore(PropertiesService.getScriptProperties())
    .setScope(CONFIG.AWEBER.SCOPES)
    .setRedirectUri(CONFIG.AWEBER.REDIRECT_URI)
    .setParam('grant_type', 'authorization_code');

  service.refreshToken = function () {
    const props = PropertiesService.getScriptProperties();
    const token = props.getProperty('oauth2.AWeber');
    if (!token) {
      console.error('refreshToken: No token found in properties.');
      return null;
    }
    try {
      const refreshToken = JSON.parse(token).refresh_token;
      const headers = {
        Authorization:
          'Basic ' +
          Utilities.base64Encode(
            CONFIG.AWEBER.CLIENT_ID + ':' + CONFIG.AWEBER.CLIENT_SECRET
          ),
        'Content-Type': 'application/x-www-form-urlencoded',
      };
      const payload = { grant_type: 'refresh_token', refresh_token: refreshToken };
      const options = { method: 'post', headers: headers, payload: payload };
      const response = UrlFetchApp.fetch(
        'https://auth.aweber.com/oauth2/token',
        options
      );
      if (response.getResponseCode() !== 200) {
        console.error(
          `refreshToken: Refresh failed with code: ${response.getResponseCode()}, content: ${response.getContentText()}`
        );
        return null;
      }
      const newToken = JSON.parse(response.getContentText());
      props.setProperty('oauth2.AWeber', JSON.stringify(newToken));
      console.log(`refreshToken: New token obtained: ${newToken.access_token.slice(-4)}`);
      return newToken.access_token;
    } catch (e) {
      console.error('refreshToken: Refresh failed:', e);
      return null;
    }
  };
  return service;
}

function validateToken(service) {
  try {
    if (!service.hasAccess()) return false;
    const token = service.getAccessToken();
    if (!token) return false;

    const test = UrlFetchApp.fetch('https://api.aweber.com/1.0/accounts', {
      headers: { Authorization: `Bearer ${token}` },
      muteHttpExceptions: true,
    });

    if (test.getResponseCode() === 401) {
      console.log('Token expired, forcing refresh...');
      const newToken = service.refreshToken();
      return newToken !== null;
    }

    return test.getResponseCode() === 200;
  } catch (e) {
    console.error('Validation error:', e);
    return false;
  }
}

function getValidAccessToken() {
  const service = getAWeberService();
  let token = service.getAccessToken();
  if (!token) {
    console.log('getValidAccessToken: No access token found.');
    return service.refreshToken();
  }
  console.log('getValidAccessToken: Current token:', token.slice(-4));
  const options = {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
    muteHttpExceptions: true,
  };
  const response = UrlFetchApp.fetch(
    'https://api.aweber.com/1.0/accounts',
    options
  );
  const code = response.getResponseCode();
  if (code === 401) {
    console.log('getValidAccessToken: Access token expired, refreshing...');
    token = service.refreshToken();
    if (!token) {
      console.error('getValidAccessToken: Token refresh failed.');
      return null;
    }
    console.log('getValidAccessToken: New access token obtained:', token.slice(-4));
  }
  return token;
}

function fetchApi(url, token, retryCount = 0) {
  const MAX_RETRIES = 3;
  const BASE_DELAY = 2000; // Start with 2 second delay
  
  try {
    const response = UrlFetchApp.fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      },
      muteHttpExceptions: true
    });

    const code = response.getResponseCode();
    const content = response.getContentText();

    // Handle rate limits
    if (code === 429 || code === 403) {
      if (retryCount < MAX_RETRIES) {
        const retryAfter = response.getHeaders()['Retry-After'] || 
                         Math.min(BASE_DELAY * Math.pow(2, retryCount), 30000) / 1000;
        console.log(`Rate limited. Waiting ${retryAfter} seconds...`);
        Utilities.sleep(retryAfter * 1000);
        return fetchApi(url, token, retryCount + 1);
      }
      throw new Error(`Rate limit exceeded after ${MAX_RETRIES} retries`);
    }

    // Handle other errors
    if (code !== 200) {
      throw new Error(`API request failed with ${code}: ${content}`);
    }

    // Parse and validate response structure
    const result = JSON.parse(content);
    if (!result || typeof result !== 'object') {
      throw new Error("Invalid API response format");
    }
    
    return result;
    
  } catch (e) {
    throw new Error(`Error fetching ${url}: ${e.message}`);
  }
}

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('AWeber Integration')
    .addItem('Authorize AWeber', 'authorizeAWeber')
    .addItem('Fetch Broadcast Data', 'fetchAWeberBroadcastData')
    .addItem('Reset Authorization', 'resetAuth')
    .addItem('Debug Storage', 'debugStorage')
    .addToUi();
}

function authorizeAWeber() {
  const service = getAWeberService();
  if (!service.hasAccess()) {
    const authUrl = service.getAuthorizationUrl();
    SpreadsheetApp.getUi().alert(
      'Please authorize using this URL, then run "Fetch Broadcast Data" again: ' +
        authUrl
    );
  } else {
    SpreadsheetApp.getUi().alert('AWeber is already authorized.');
  }
}

function resetAuth() {
  getAWeberService().reset();
  logOutput('Authorization has been reset. Please re-authorize.');
}

function debugStorage() {
  const props = PropertiesService.getScriptProperties();
  const allProps = props.getProperties();
  console.log('Current stored properties:', JSON.stringify(allProps, null, 2));
  const service = getAWeberService();
  const hasAccess = service.hasAccess();
  const token = service.getAccessToken();
  console.log('Service has access:', hasAccess);
  console.log('Access token exists:', !!token);
  return { properties: allProps, hasAccess: hasAccess, tokenExists: !!token };
}

function authCallback(request) {
  const service = getAWeberService();
  const authorized = service.handleCallback(request);
  return HtmlService.createHtmlOutput(
    `<div style="padding:20px;font-family:Arial;text-align:center"><h2>${
      authorized ? '✅ Success!' : '❌ Failed'
    }</h2><p>${
      authorized ? 'Authorization complete!' : 'Authorization failed'
    }</p><p>You can close this window now.</p></div>`
  )
    .setWidth(300)
    .setHeight(150);
}

function getAWeberAccountId(token) {
  const response = fetchApi('https://api.aweber.com/1.0/accounts', token);
  return response.entries[0].id;
}

function getListIds(accountId, token) {
  const response = fetchApi(
    `https://api.aweber.com/1.0/accounts/${accountId}/lists`,
    token
  );
  return response.entries.map((list) => list.id);
}
function logOutput(message) {
  console.log(message);
}


function resetSync() {
  PropertiesService.getScriptProperties().deleteProperty('lastSyncTimestamp');
  console.log("Sync timestamp reset");
}

function forceFullSync() {
  const sheet = getOrCreateSheet(
    CONFIG.SHEET.SUBSCRIBER_DETAILS_SHEET_NAME,
    CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS
  );
  sheet.clearContents();
  sheet.appendRow(CONFIG.SHEET.SUBSCRIBER_DETAILS_HEADERS);
  
  PropertiesService.getScriptProperties().deleteProperty('lastSyncTimestamp');
  updateSubscriberDetails();
}