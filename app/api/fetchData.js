import axios from 'axios';
import { appendToBigQuery } from '../../lib/bigquery';

export default async function handler(req, res) {
  try {
    // Fetch data from marketing API
    const response = await axios.get('https://api.marketing.com/data', {
      headers: {
        'Authorization': `Bearer ${process.env.MARKETING_API_KEY}`
      }
    });

    const data = response.data;

    /**
     * Map data to relevant tables
     * Assume API returns objects like:
     * { campaigns: [...], adsets: [...], ads: [...] }
     */
    if (data.campaigns) {
      await appendToBigQuery('campaign_insights', data.campaigns);
    }

    if (data.adsets) {
      await appendToBigQuery('fb_adset_insights', data.adsets);
    }

    if (data.ads) {
      await appendToBigQuery('fb_ad_insights', data.ads);
    }

    res.status(200).json({ message: 'Data appended to relevant tables successfully' });

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to fetch or insert data', details: error.message });
  }
}
