# 📧 AWeber Email Analytics → Google Sheets → Looker (Free Tier)

This project automates the extraction of email marketing stats from **AWeber’s API**, transforms it, and sends the cleaned data to **Google Sheets**, ready for use in **Looker Studio dashboards**.

---

## ⚙️ Tech Stack

- **Google Apps Script** – backend automation (1000+ lines, modular)
- **AWeber API** – email data source (OAuth2 auth, free tier)
- **Google Sheets** – storage + calculations
- **Looker Studio** – optional dashboards

---

## 📁 Output Sheets

### 1️⃣ `Email Stats Overall`

📈 **Monthly performance summary** including subscriber growth, churn, and unsubscribe rates.

| Year | Month   | Total Subscribers | % Growth MoM | New Subs | % New MoM | Unsubs | Unsub Rate (%) |
|------|---------|-------------------|--------------|----------|-----------|--------|----------------|
| 2025 | Feb     | 4912              | 0.00         | 4912     | 0.00      | 118    | 0.00           |
| 2025 | Mar     | 12552             | 155.54%      | 7637     | 55.47%    | 214    | 1.70%          |
| 2025 | Apr     | 13742             | 9.48%        | 1124     | -85.28%   | 113    | 0.82%          |

✅ Generated dynamically via script (`calculateOverallStatsFromSheet`)

---

### 2️⃣ `Subscriber Details`

🧑‍💼 **Raw subscriber activity** showing signup and unsubscribe timestamps.

| Subscriber ID | Subscribed At         | Unsubscribed At       |
|---------------|------------------------|------------------------|
| 507487        | 2025-02-12 21:20:38    | 2025-04-03 02:31:46    |
| 507488        | 2025-02-12 21:20:38    |                        |
| 508586        | 2025-02-12 23:36:35    |                        |

✅ Populated via `bulkLoadSubscribers()` and `incrementalSubscriberUpdate()`

---

### 3️⃣ `Email Stats (Broadcasts)`

📬 **Campaign-level metrics** for each broadcast email sent.

| Sent Date | Broadcast ID | Subject                               | Total Sent | Delivered % | Opens % | Clicks % | Undeliv % |
|-----------|---------------|----------------------------------------|------------|-------------|---------|----------|------------|
| 2/13/2025 | 59247077      | Today we have launched a new landing page | 259        | 98.07       | 44.8%   | 0.77%    | 1.93%      |
| 2/15/2025 | 59248033      | Independents have some growing to do     | 418        | 89.23       | 28.7%   | 0.72%    | 10.77%     |

✅ Pulled from AWeber using `fetchBroadcastStats()` logic

---

## 🔄 Sync Modes

- **Initial Run**: Bulk historical load
- **Next Runs**: Incremental updates (no duplicates)
- **Timestamps** tracked using `PropertiesService`

---

## 📊 Use Cases

- Monthly growth dashboards
- Campaign performance comparison
- Engagement and unsubscribe trend analysis
- Real-time Looker Studio integration

---

## 🆓 Cost

- ✅ 100% free using native tools
- ❌ No paid API connectors or third-party apps

---

## 🚀 Project Status

✅ Fully working  
📌 Scalable  
🧼 Deduplicated  
⚡ Auto-refresh enabled (via triggers or manual)

---


