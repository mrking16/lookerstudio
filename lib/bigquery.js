import { BigQuery } from '@google-cloud/bigquery';

// ADC will automatically pick credentials
const bigquery = new BigQuery({
  projectId: 'yunibuffa-looker-studio-report'
});

export async function appendToBigQuery(tableName, rows) {
  if (!rows || rows.length === 0) {
    console.log(`No data to insert for ${tableName}`);
    return;
  }

  try {
    const dataset = bigquery.dataset('fb_ads_data');
    const table = dataset.table(tableName);
    await table.insert(rows);
    console.log(`${rows.length} rows inserted into ${tableName}`);
  } catch (error) {
    console.error(`BigQuery insert error for ${tableName}:`, error);
  }
}
