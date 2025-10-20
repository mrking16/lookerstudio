const axios = require("axios");
const { BigQuery } = require("@google-cloud/bigquery");
require("dotenv").config();


require("dotenv").config();


// ✅ Initialize BigQuery client (using gcloud login — no key file needed)
const bigquery = new BigQuery({
  projectId: process.env.GOOGLE_PROJECT_ID,
});


// 📅 Date Range (you can modify as needed)
const SINCE = "2025-10-01";
const UNTIL = "2025-10-19";

// 📊 Function: Fetch data from Facebook Marketing API
async function fetchData(level) {
  const url = `https://graph.facebook.com/v20.0/${process.env.FB_AD_ACCOUNT_ID}/insights`;
  const params = {
    access_token: process.env.FB_ACCESS_TOKEN,
    fields: `${level}_id,${level}_name,impressions,clicks,spend,date_start,date_stop`,
    level,
    time_range: { since: SINCE, until: UNTIL },
  };

  try {
    const res = await axios.get(url, { params });
    console.log(`📥 Fetched ${res.data.data.length} rows from Facebook for level: ${level}`);
    return res.data.data || [];
  } catch (err) {
    console.error(`❌ Facebook API error (${level}):`, err.response?.data || err.message);
    return [];
  }
}

// 🧾 Function: Upload fetched data to BigQuery
async function uploadToBigQuery(table, rows) {
  if (!rows.length) {
    console.log(`⚠️ No data for ${table}`);
    return;
  }

  const datasetId = "fb_ads_data";
 try {
    await bigquery.dataset(datasetId).table(table).insert(rows);
    console.log(`✅ Inserted ${rows.length} rows into BigQuery table: ${table}`);
  } catch (err) {
    console.error(`❌ BigQuery insert error (${table}):`, err.errors || err.message);
  }
}

// 🚀 Main workflow
async function main() {
  const levels = [
    { name: "campaign", table: "campaign_insights" },
    { name: "adset", table: "fb_adset_insights" },
    { name: "ad", table: "fb_ad_insights" },
  ];

  for (const { name, table } of levels) {
    console.log(`🔍 Processing level: ${name}`);
    const data = await fetchData(name);

    const rows = data.map((d) => ({
      [`${name}_id`]: d[`${name}_id`],
      [`${name}_name`]: d[`${name}_name`],
      impressions: parseInt(d.impressions || 0),
      clicks: parseInt(d.clicks || 0),
      spend: parseFloat(d.spend || 0),
      date_start: d.date_start,
      date_stop: d.date_stop,
    }));

    await uploadToBigQuery(table, rows);
  }

  console.log("🎯 All Facebook insights uploaded to BigQuery successfully!");
}

main();

