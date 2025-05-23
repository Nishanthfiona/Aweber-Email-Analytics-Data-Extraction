const CONFIG = {
  AWEBER: {
    CLIENT_ID: '',
    CLIENT_SECRET: '',
    REDIRECT_URI: 'https://script.google.com/macros/d/{placeholder for Script ID}/usercallback',
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
    NAME: 'Email Stats(Broadcasts)',
    BATCH_SIZE: 20, // Keep small for broadcast data
    LAST_PROCESSED_CELL: 'Z1',
    HEADERS: [
      'Sent Date',
      'Broadcast ID',
      'Audience',
      'Subject',
      'Total Sent',
      'Delivered Count',
      'Delivered %',
      'Total Unique Opens',
      'Opens %',
      'Clicks',
      'Clicks %',
      'Complaint Count',
      'Complaint %',
      'Undelivered Count',
      'Undeliv %',
    ],
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
    SUBSCRIBER_SHEET_NAME: 'AWeber Subscriber Data',
    SUBSCRIBER_HEADERS: ['Subscriber ID', 'Subscribed At', 'Unsubscribed At'],
  },
};

// In fetchAndAggregateAWeberData [cite: 4]
function fetchAndAggregateAWeberData() {
  try {
    const token = getValidAccessToken();
    if (!token) {
         // Handle the case where getValidAccessToken failed (e.g., refresh token invalid)
         // Notification should have already been sent by the improved refreshToken function
         logOutput("fetchAndAggregateAWeberData: Halting execution because no valid access token could be obtained.");
         return; // Stop gracefully
    }
    const accountId = getAWeberAccountId(token);
    // Add checks here in case getAWeberAccountId fails
    if (!accountId) {
         logOutput("fetchAndAggregateAWeberData: Failed to get Account ID.");
         // Notification might have been sent by fetchApi inside getAWeberAccountId
         return;
    }
    const listIds = getListIds(accountId, token);
     // Add checks here
    if (!listIds || listIds.length === 0) {
         logOutput("fetchAndAggregateAWeberData: Failed to get List IDs or no lists found.");
         // Notification might have been sent by fetchApi inside getListIds
         return;
    }
    processBroadcastData(accountId, listIds, token);
  } catch (e) {
    // Catch errors thrown explicitly (like auth failures) or unexpected ones
    const errorMessage = `Main script error in fetchAndAggregateAWeberData: ${e.message}\nStack: ${e.stack || 'No stack available'}`;
    logOutput(errorMessage);
    // Send notification for any major uncaught error in the main process
    sendErrorNotification("AWeber Script Failed", errorMessage);
  }
}

// --- Broadcast Data Processing ---

function processBroadcastData(accountId, listIds, token) {
  const broadcastSheet = initSheet(CONFIG.SHEET.NAME, CONFIG.SHEET.HEADERS);
  let lastProcessedId =
    broadcastSheet.getRange(CONFIG.SHEET.LAST_PROCESSED_CELL).getValue() || '';
  const existingIds = getExistingBroadcastIds(broadcastSheet);
  fetchAWeberBroadcastData(
    broadcastSheet,
    token,
    accountId,
    listIds,
    existingIds,
    lastProcessedId
  );
}

function fetchAWeberBroadcastData(broadcastSheet, token,  accountId,  listIds,  existingIds, lastProcessedId) {
  let service = getAWeberService();
  if (!service.hasAccess()) {
      const authUrl = service.getAuthorizationUrl();
      const message = 'AWeber is not authorized. Manual authorization required via the menu. Auth URL (for manual use if needed): ' + authUrl;
      logOutput("ERROR: " + message); // Log instead of alert
      sendErrorNotification("AWeber Authorization Required", message);
      throw new Error(message); // Stop execution and report error clearly in trigger logs
  }
  // if (!service.hasAccess()) {
  //   const authUrl = service.getAuthorizationUrl();
  //   SpreadsheetApp.getUi().alert(
  //     'AWeber is not authorized. Please authorize using this URL, then run "Fetch Broadcast Data" again: ' +
  //       authUrl
  //   );
  //   return; // Stop execution if not authorized
  // }

  // if (!validateToken(service)) {
  //   const authUrl = service.getAuthorizationUrl();
  //   SpreadsheetApp.getUi().alert(
  //     'AWeber token is invalid. Please re-authorize using this URL, then run "Fetch Broadcast Data" again: ' +
  //       authUrl
  //   );
  //   service.reset();
  //   return; // Stop execution if token is invalid
  // }

  try {
    let allBroadcasts = [];
    let processedCount = 0;

    for (const listId of listIds) {
      // Fetch ALL broadcasts (or a limited number to stay within time limits)
      const broadcasts = getBroadcasts(
        accountId,
        listId,
        token,
        null
      ); // Fetch all, ignore lastProcessedId

      for (const broadcast of broadcasts) {
        if (processedCount >= CONFIG.SHEET.BATCH_SIZE) break;
        const broadcastId = extractBroadcastId(broadcast);
        if (!broadcastId) continue;

        try {
          const broadcastDetails = awaitWithRetry(
            () => getBroadcastDetails(accountId, listId, broadcastId, token),
            3
          );
          if (!broadcastDetails || !broadcastDetails.stats) continue;
          const stats = broadcastDetails.stats;
          const sentCount = stats.num_emailed || 0;
          const undeliveredCount = stats.num_undeliv || 0;
          const deliveredCount = sentCount > 0 ? sentCount - undeliveredCount : 0;
          const opens = stats.unique_opens || 0;
          const clicks = stats.unique_clicks || 0;
          const complaints = stats.num_complaints || 0;

          let audience = 'all'; // Placeholder. Replace with your logic to get the audience.

          const broadcastData = {
            sentDate: broadcast.sent_at
              ? new Date(broadcast.sent_at)
              : new Date(broadcast.created_at),
            broadcastId: broadcastId,
            audience: audience,
            subject: broadcast.subject || 'No Subject',
            totalSent: sentCount,
            deliveredCount: deliveredCount,
            deliveredPercentage: sentCount > 0
              ? (deliveredCount / sentCount) * 100
              : 0,
            totalUniqueOpens: opens,
            opensPercentage: sentCount > 0 ? (opens / sentCount) * 100 : 0,
            clicks: clicks,
            clicksPercentage: sentCount > 0 ? (clicks / sentCount) * 100 : 0,
            complaintCount: complaints,
            complaintPercentage: sentCount > 0
              ? (complaints / sentCount) * 100
              : 0,
            undeliveredCount: undeliveredCount,
            undeliveredPercentage: sentCount > 0
              ? (undeliveredCount / sentCount) * 100
              : 0,
          };

          const formattedData = [
            broadcastData.sentDate.toLocaleDateString('en-US', {
              month: 'numeric',
              day: 'numeric',
              year: 'numeric',
            }),
            broadcastData.broadcastId,
            broadcastData.audience,
            broadcastData.subject,
            broadcastData.totalSent,
            broadcastData.deliveredCount,
            broadcastData.deliveredPercentage.toFixed(2),
            broadcastData.totalUniqueOpens,
            broadcastData.opensPercentage.toFixed(1),
            broadcastData.clicks,
            broadcastData.clicksPercentage.toFixed(2),
            broadcastData.complaintCount,
            broadcastData.complaintPercentage.toFixed(2),
            broadcastData.undeliveredCount,
            broadcastData.undeliveredPercentage.toFixed(2),
          ];

          writeBroadcastData(broadcastSheet, formattedData, broadcastId);
          processedCount++;
          Utilities.sleep(2000);

          if (processedCount >= CONFIG.SHEET.BATCH_SIZE) {
            logOutput(
              `Processed ${processedCount} broadcasts. Stopping to avoid execution limit.`
            );
            return;
          }
        } catch (e) {
          logOutput(`Error processing broadcast ${broadcastId}: ${e.message}`);
        }
      }
    }

    logOutput(`Successfully processed ${processedCount} broadcasts.`);
  } catch (e) {
    const errorMessage = `Main execution error: ${e.message}`;
    logOutput(errorMessage);
    if (service) service.reset();
  }
}

function getExistingBroadcastIds(sheet) {
  const data = sheet.getDataRange().getValues();
  if (data.length <= 1) return []; // Skip header row

  return data
    .slice(1) // Skip header
    .map(row => (row[1] ? row[1].toString() : null)) // Column 2 is Broadcast ID
    .filter(id => id); // Remove null/undefined
}

function awaitWithRetry(fn, retries, delay = 2000) {
  try {
    return fn();
  } catch (e) {
    if (retries <= 0) throw e;
    logOutput(`Retrying... (${retries} attempts remaining)`);
    Utilities.sleep(delay);
    return awaitWithRetry(fn, retries - 1, delay * 2); // Exponential backoff
  }
}

function extractBroadcastId(broadcast) {
  if (broadcast.id) return broadcast.id;
  if (broadcast.self_link) {
    const matches = broadcast.self_link.match(/broadcasts\/(\d+)/);
    if (matches && matches[1]) return matches[1];
  }
  if (broadcast.broadcast_id) return broadcast.broadcast_id.toString();
  return null;
}

function logOutput(message) {
  console.log(message);
}

function writeBroadcastData(sheet, data) {
  const dataRange = sheet.getDataRange();
  const dataValues = dataRange.getValues();

  // Find existing row by Broadcast ID (column 2)
  let rowToUpdate = null;
  for (let i = 1; i < dataValues.length; i++) {
    if (dataValues[i][1] && dataValues[i][1].toString() === data[1].toString()) {
      rowToUpdate = i + 1;
      break;
    }
  }

  console.log(`writeBroadcastData: rowToUpdate = ${rowToUpdate}`);

  if (rowToUpdate) {
    // Update existing row
    const newOpens = data[7]; // Assuming "Opens" is at index 7
    const existingOpens = dataValues[rowToUpdate - 1][7];

    console.log('writeBroadcastData: New Opens from AWeber:', JSON.stringify(data));
    console.log(
      'writeBroadcastData: Existing data in sheet:',
      JSON.stringify(dataValues[rowToUpdate - 1])
    );
    sheet
      .getRange(rowToUpdate, 1, 1, CONFIG.SHEET.HEADERS.length)
      .setValues([data]);
    logOutput(`Updated existing row for broadcast ${data[1]}`);
  } else {
    // Insert new row in correct chronological position
    const dates = dataValues.slice(1).map(row =>
      row[0] instanceof Date ? row[0] : new Date(row[0])
    );
    let insertRow = 2; // Start after header

    for (let i = 0; i < dates.length; i++) {
      console.log('writeBroadcastData: dates[' + i + ']:', dates[i], 'data[0]:', data[0]);
      if (data[0] <= dates[i]) {
        insertRow = i + 2;
        break;
      }
    }

    if (insertRow === 2 && dates.length > 0 && data[0] > dates[dates.length - 1]) {
      insertRow = dates.length + 2;
    }

    sheet.insertRowBefore(insertRow);
    sheet
      .getRange(insertRow, 1, 1, CONFIG.SHEET.HEADERS.length)
      .setValues([data]);
    logOutput(`Inserted new row at position ${insertRow} for broadcast ${data[1]}`);
  }
}

function getBroadcasts(accountId, listId, token, lastProcessedId) {
  let allBroadcasts = [];
  let url = `https://api.aweber.com/1.0/accounts/${accountId}/lists/${listId}/broadcasts?status=sent`;
  let foundLastId = false;

  try {
    while (url && allBroadcasts.length < CONFIG.SHEET.BATCH_SIZE * 2) {
      const response = fetchApi(url, token);
      if (response && response.entries && Array.isArray(response.entries)) {
        const validBroadcasts = response.entries;

        if (lastProcessedId) {
          for (const broadcast of validBroadcasts) {
            const broadcastId = extractBroadcastId(broadcast);
            if (broadcastId === lastProcessedId) {
              foundLastId = true;
              break;
            }
            allBroadcasts.push(broadcast);
          }
          if (foundLastId) break;
        } else {
          allBroadcasts = allBroadcasts.concat(validBroadcasts);
        }

        url = response.next_collection_link;
        Utilities.sleep(500);
      } else {
        console.error('getBroadcasts: API response missing or invalid entries:', response);
        break;
      }
    }
  } catch (e) {
    console.error('getBroadcasts: Error fetching broadcasts:', e);
  }

  return allBroadcasts;
}

function getBroadcastDetails(accountId, listId, broadcastId, token) {
  const url = `https://api.aweber.com/1.0/accounts/${accountId}/lists/${listId}/broadcasts/${broadcastId}`;
  return fetchApi(url, token);
}

function getListIds(accountId, token) {
  const response = fetchApi(
    `https://api.aweber.com/1.0/accounts/${accountId}/lists`,
    token
  );
  return response.entries.map(list => list.id);
}

function initSheet() {
  const ss = SpreadsheetApp.openById(CONFIG.SHEET.ID);
  let sheet = ss.getSheetByName(CONFIG.SHEET.NAME);

  if (!sheet) {
    sheet = ss.insertSheet(CONFIG.SHEET.NAME);
    logOutput(`Created new sheet: ${CONFIG.SHEET.NAME}`);
  }

  // Check if headers exist and match
  const headers = sheet
    .getRange(1, 1, 1, CONFIG.SHEET.HEADERS.length)
    .getValues()[0];
  const headersMatch = CONFIG.SHEET.HEADERS.every((h, i) => h === headers[i]);

  if (!headersMatch || sheet.getLastRow() === 0) {
    // Clear sheet if headers don't match
    sheet.clear();
    sheet.appendRow(CONFIG.SHEET.HEADERS);
    sheet
      .getRange(1, 1, 1, CONFIG.SHEET.HEADERS.length)
      .setFontWeight('bold')
      .setBackground('#eeeeee');

    // Format percentage columns
    sheet.getRange(2, 7, sheet.getMaxRows() - 1, 1).setNumberFormat('0.00'); // Delivered %
    sheet.getRange(2, 9, sheet.getMaxRows() - 1, 1).setNumberFormat('0.0'); // Opens %
    sheet
      .getRange(2, 11, sheet.getMaxRows() - 1, 1)
      .setNumberFormat('0.00'); // Clicks %
    sheet
      .getRange(2, 13, sheet.getMaxRows() - 1, 1)
      .setNumberFormat('0.00'); // Complaint %
    sheet
      .getRange(2, 15, sheet.getMaxRows() - 1, 1)
      .setNumberFormat('0.00'); // Undeliv %

    //Format number columns
    sheet.getRange(2, 6, sheet.getMaxRows() - 1, 1).setNumberFormat('#,##0'); // Delivered Count
    sheet.getRange(2, 5, sheet.getMaxRows() - 1, 1).setNumberFormat('#,##0'); // Total Sent
    sheet
      .getRange(2, 4, sheet.getMaxRows() - 1, 1)
      .setNumberFormat('yyyy-mm-dd'); // Date
    sheet.getRange(2, 8, sheet.getMaxRows() - 1, 1).setNumberFormat('#,##0'); // Total Unique Opens
    sheet.getRange(2, 10, sheet.getMaxRows() - 1, 1).setNumberFormat('#,##0'); // Clicks
    sheet
      .getRange(2, 12, sheet.getMaxRows() - 1, 1)
      .setNumberFormat('#,##0'); // Complaint Count
    sheet
      .getRange(2, 14, sheet.getMaxRows() - 1, 1)
      .setNumberFormat('#,##0'); // Undelivered Count

    logOutput('Initialized sheet with headers');
  }

  return sheet;
}

// OAuth Service Functions (same as before)
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

  // Custom refreshToken override
  service.refreshToken = function () {
    const props = PropertiesService.getScriptProperties();
    const tokenJson = props.getProperty('oauth2.AWeber');

    if (!tokenJson) {
      console.error('refreshToken: No token JSON found in properties.');
      return null;
    }

    try {
      const tokenData = JSON.parse(tokenJson);
      const refreshToken = tokenData.refresh_token;

      if (!refreshToken) {
        console.error('refreshToken: Refresh token missing from stored properties.');
        return null;
      }

      const headers = {
        'Authorization': 'Basic ' + Utilities.base64Encode(CONFIG.AWEBER.CLIENT_ID + ':' + CONFIG.AWEBER.CLIENT_SECRET),
        'Content-Type': 'application/x-www-form-urlencoded',
      };

      const payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refreshToken,
      };

      const options = {
        'method': 'post',
        'headers': headers,
        'payload': payload,
        'muteHttpExceptions': true,
      };

      const response = UrlFetchApp.fetch('https://auth.aweber.com/oauth2/token', options);
      const responseCode = response.getResponseCode();
      const responseText = response.getContentText();

      if (responseCode !== 200) {
        console.error(`refreshToken: Refresh failed with code: ${responseCode}, content: ${responseText}`);

        try {
          const errorResponse = JSON.parse(responseText);
          if (errorResponse.error === 'invalid_grant') {
            console.error("!!! REFRESH TOKEN IS INVALID. MANUAL RE-AUTHORIZATION REQUIRED !!!");
            props.deleteProperty('oauth2.AWeber');
            sendErrorNotification(
              "AWeber Refresh Token Invalid",
              "The AWeber refresh token is invalid and has been cleared. Manual re-authorization is required via the 'Authorize AWeber' menu item."
            );
          } else {
            sendErrorNotification(
              "AWeber Token Refresh Failed",
              `Failed to refresh AWeber token. Status: ${responseCode}. Error: ${responseText}. Manual re-authorization may be required.`
            );
          }
        } catch (parseError) {
          sendErrorNotification(
            "AWeber Token Refresh Failed",
            `Failed to refresh AWeber token and parse error response. Status: ${responseCode}. Content: ${responseText}. Manual re-authorization may be required.`
          );
          console.error("Could not parse error response from token endpoint: " + parseError);
        }
        return null;
      }

      const newToken = JSON.parse(responseText);
      props.setProperty('oauth2.AWeber', JSON.stringify(newToken));
      console.log(`refreshToken: New token obtained. Access token ends with: ${newToken.access_token.slice(-4)}`);
      return newToken.access_token;

    } catch (e) {
      const errorMessage = `refreshToken: Unhandled exception during refresh: ${e.message}\nStack: ${e.stack}`;
      console.error(errorMessage);
      sendErrorNotification("AWeber Token Refresh Error", errorMessage);
      return null;
    }
  };

  return service;
}

/**
 * Sends an email notification for script errors.
 * @param {string} subject The subject line prefix.
 * @param {string} body The email body content.
 */
function sendErrorNotification(subject, body) {
  // Ensure this runs only once per script execution for a specific error type if needed,
  // e.g., by using ScriptProperties or CacheService as a flag, to avoid spamming.
  // For simplicity here, it sends every time it's called.
  try {
    const recipient = Session.getEffectiveUser().getEmail(); // Gets email of user who authorized/owns the script
    if (!recipient) {
       // Fallback if effective user has no email (rare, but possible in some contexts)
       // Consider hardcoding an admin email if necessary:
       // recipient = "your-admin-email@example.com";
       if (!recipient) {
         console.error("sendErrorNotification: Could not determine recipient email.");
         return; // Cannot send email
       }
    }
    const fullSubject = `[AWeber Script Alert] ${subject}`;
    // Limit body length to avoid truncation issues with MailApp limits
    const maxBodyLength = 1500;
    const emailBody = body.length > maxBodyLength ? body.substring(0, maxBodyLength) + "\n..." : body;

    MailApp.sendEmail(recipient, fullSubject, emailBody);
    console.log("Sent error notification email to " + recipient);

  } catch (e) {
    // Log failure to send email, but don't let this stop the script entirely
    console.error(`Failed to send error notification email. Subject: ${subject}. Error: ${e.message}`);
  }
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
  let validToken = token;
  if (!validToken) {
    validToken = getValidAccessToken();
    if (!validToken) throw new Error(`fetchApi: No valid token available for ${url}`);
  }

  const options = {
    headers: {
      Authorization: `Bearer ${validToken}`,
      Accept: 'application/json',
    },
    muteHttpExceptions: true,
  };

  logOutput(`Fetching API: ${url}`); // Log the URL

  try {
    const response = UrlFetchApp.fetch(url, options);
    const code = response.getResponseCode();
    const content = response.getContentText();

    // logOutput(`API Response Code: ${code}`); // Log the response code
    // logOutput(`API Response Content: ${content}`); // Log the content

    if (code === 401) {
      if (retryCount < 3) {
        console.log(
          'fetchApi: API 401 error, refreshing token (attempt ' +
            (retryCount + 1) +
            ')...'
        );
        validToken = getValidAccessToken();
        if (validToken) {
          Utilities.sleep(5000);
          console.log('fetchApi: New token after 401 refresh:', validToken.slice(-4));
          return fetchApi(url, validToken, retryCount + 1);
        }
      }
      throw new Error(`API ${url} failed with 401 after retries: ${content}, code: ${code}`);
    }

    if (code === 429) {
      const retryAfter = response.getHeaders()['Retry-After'] || 10;
      console.log(`fetchApi: Rate limit exceeded. Retrying after ${retryAfter} seconds.`);
      Utilities.sleep(retryAfter * 1000);
      return fetchApi(url, token, retryCount);
    }

    if (code !== 200) {
      throw new Error(`API ${url} failed with ${code}: ${content}`);
    }

    try {
      return JSON.parse(content);
    } catch (e) {
      throw new Error(`Invalid JSON from API: ${content.substring(0, 200)}...`);
    }
  } catch (e) {
    throw new Error(`Error fetching from ${url}: ${e.message}`);
  }
}

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('AWeber Integration')
    .addItem('Authorize AWeber', 'authorizeAWeber')
    .addItem('Fetch Broadcast Data', 'fetchAndAggregateAWeberData')
    .addItem('Reset Authorization', 'resetAuth')
    .addItem('Debug Storage', 'debugStorage')
    .addToUi();
}

function authorizeAWeber() {
  const service = getAWeberService();
  if (!service.hasAccess()) {
    const authUrl = service.getAuthorizationUrl();
    // Use HTML service for a better user experience and trigger safety
    const htmlOutput = HtmlService.createHtmlOutput(
        `<p>Please authorize this script to access your AWeber account:</p>
         <p><a href="${authUrl}" target="_blank" onclick="google.script.host.close()">Click here to Authorize</a></p>
         <p>After authorizing in AWeber, close that window. You may need to run the 'Update' function again manually once.</p>
         <input type="button" value="Close" onclick="google.script.host.close()" />` // Add close button
      )
      .setWidth(400)
      .setHeight(150);
    SpreadsheetApp.getUi().showModalDialog(htmlOutput, 'AWeber Authorization Required'); // Show modal
  } else {
    // Still use alert here as it's manually triggered from menu
    SpreadsheetApp.getUi().alert('AWeber connection is already authorized.');
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
  return response.entries.map(list => list.id);
}
