const axios = require("axios");
const { BigQuery } = require("@google-cloud/bigquery");
require("dotenv").config();


const bigquery = new BigQuery({
  projectId: "yunibuffa-looker-studio-report",
});

// ✅ Facebook API setup
const ACCESS_TOKEN = process.env.ACCESS_TOKEN;
const AD_ACCOUNT_ID = process.env.AD_ACCOUNT_ID; // e.g., act_1234567890
const DATASET_ID = "fb_ads_data";

// ✅ Fields for all levels
const FIELDS = [
  "campaign_id",
  "campaign_name",
  "adset_id",
  "adset_name",
  "ad_id",
  "ad_name",
  "reach",
  "impressions",
  "clicks",
  "spend",
  "cpc",
  "ctr",
  "actions",
  "date_start",
  "date_stop"
].join(",");

// ✅ Fetch data from Facebook
async function fetchData(level) {
  const url = `https://graph.facebook.com/v20.0/${AD_ACCOUNT_ID}/insights`;

  const params = {
    access_token: ACCESS_TOKEN,
    level,
    fields: FIELDS,
    date_preset: "maximum", // 🔥 all available data
    filtering: [],
    limit: 5000,
  };

  try {
    const res = await axios.get(url, { params });
    console.log(`📥 ${level}: ${res.data.data.length} rows fetched`);
    return res.data.data || [];
  } catch (err) {
    console.error(`❌ Facebook API error (${level}):`, err.response?.data || err.message);
    return [];
  }
}

// ✅ Upload to BigQuery (append-only)
async function uploadToBigQuery(table, rows) {
  if (!rows.length) {
    console.log(`⚠️ No new data for ${table}`);
    return;
  }

  try {
    await bigquery
      .dataset(DATASET_ID)
      .table(table)
      .insert(rows, { ignoreUnknownValues: true }); // 🔥 allows new columns

    console.log(`✅ ${rows.length} rows added to ${table}`);
  } catch (err) {
    console.error(`❌ BigQuery insert error (${table}):`, err.errors || err.message);
  }
}

// ✅ Main process
async function main() {
  const levels = [
    { name: "campaign", table: "campaign_insights" },
    { name: "adset", table: "fb_adset_insights" },
    { name: "ad", table: "fb_ad_insights" },
  ];

  for (const { name, table } of levels) {
    console.log(`🔍 Fetching ${name} data...`);
    const data = await fetchData(name);

    const rows = data.map((d) => ({
      campaign_id: d.campaign_id || null,
      campaign_name: d.campaign_name || null,
      adset_id: d.adset_id || null,
      adset_name: d.adset_name || null,
      ad_id: d.ad_id || null,
      ad_name: d.ad_name || null,
      impressions: Number(d.impressions || 0),
      reach: Number(d.reach || 0),
      clicks: Number(d.clicks || 0),
      spend: Number(d.spend || 0),
      cpc: Number(d.cpc || 0),
      ctr: Number(d.ctr || 0),
      actions: JSON.stringify(d.actions || []),
      date_start: d.date_start,
      date_stop: d.date_stop,
    }));

    await uploadToBigQuery(table, rows);
  }

  console.log("🎯 All Facebook insights synced successfully!");
}

main();
