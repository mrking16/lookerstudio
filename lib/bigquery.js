// lib/bigquery.js
import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery({
  projectId: process.env.GOOGLE_PROJECT_ID,
});

const datasetId = process.env.GOOGLE_BIGQUERY_DATASET_ID;

export async function insertAdInsights(tableId, rows) {
  // Har row mein reporting_date add karna
  const rowsWithTimestamp = rows.map(row => ({
    ...row,
    // Ensure date_start and date_stop are DATE type if needed, otherwise BigQuery handles it
    reporting_date: new Date().toISOString().split('T')[0], // YYYY-MM-DD format
    // Ensure float fields like spend, cpc, ctr are converted to numbers if they come as string from FB
    spend: parseFloat(row.spend) || 0,
    cpc: parseFloat(row.cpc) || 0,
    ctr: parseFloat(row.ctr) || 0,
    cpm: parseFloat(row.cpm) || 0,
    frequency: parseFloat(row.frequency) || 0,
    reach: parseInt(row.reach) || 0,
    impressions: parseInt(row.impressions) || 0,
    clicks: parseInt(row.clicks) || 0,
  }));

  try {
    // Streaming insert
    await bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert(rowsWithTimestamp, { skipUnknownFields: true });

    console.log(`Inserted ${rowsWithTimestamp.length} rows into ${datasetId}.${tableId}`);
    return true;
  } catch (error) {
    console.error(`ERROR inserting data into ${tableId}:`, error);
    if (error.response && error.response.insertErrors) {
      console.error('Streaming Insert Errors:', JSON.stringify(error.response.insertErrors, null, 2));
    }
    throw error;
  }
}