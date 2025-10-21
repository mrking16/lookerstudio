// app/api/fetchData/route.js

import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
// Import path ko adjust kiya gaya hai: '../../lib/bigquery' se '../../../lib/bigquery'
import { insertAdInsights } from '../../../lib/bigquery'; 

const api = FacebookAdsApi.init(process.env.FACEBOOK_ACCESS_TOKEN);
const account = new AdAccount(process.env.FACEBOOK_AD_ACCOUNT_ID);

// Pichle 2 din ka data fetch karne ke liye (ya aap jitne din ka chahen)
const dateStop = new Date();
dateStop.setDate(dateStop.getDate() - 1); // Yesterday
const dateStart = new Date(dateStop);
dateStart.setDate(dateStart.getDate() - 2); // 2 days ago

const SINCE_DATE = dateStart.toISOString().split('T')[0];
const UNTIL_DATE = dateStop.toISOString().split('T')[0];


async function fetchAndInsert(level, tableId) {
    let fields = [
        // Standard Metrics
        'impressions', 'clicks', 'spend', 'cpc', 'ctr', 'cpm', 'reach', 'frequency',
        'date_start', 'date_stop'
    ];
    
    // Level-specific IDs aur Names
    if (level === 'campaign') {
        // objective aur status insights se nahi milenge, lekin BigQuery schema ke liye hum inhe fields mein shamil kar rahe hain.
        fields.push('campaign_id', 'campaign_name', 'account_id', 'objective', 'status'); 
    } else if (level === 'adset') {
        // Configuration fields (budget_remaining, optimization_goal) insights se nahi milenge.
        fields.push('adset_id', 'adset_name', 'campaign_id', 'campaign_name');
    } else if (level === 'ad') {
        // Creative fields insights se nahi milenge.
        fields.push('ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'campaign_name');
    }

    const params = {
        time_range: { 'since': SINCE_DATE, 'until': UNTIL_DATE },
        level: level,
        time_increment: 1, 
    };

    // Data fetch karna (Automatic pagination handling)
    const insights = await account.getInsights(fields, params);
    let insightsData = insights.map(insight => insight._data);

    // 'account_id' ko campaign_insights mein add karna
    if (level === 'campaign') {
        insightsData = insightsData.map(row => ({
            ...row,
            // Facebook Ad Account ID ko "act_" se hata kar seedhe dalna
            account_id: process.env.FACEBOOK_AD_ACCOUNT_ID.replace('act_', ''), 
            // Agar objective/status Insights se nahi mile, toh yahan null/undefined rahenge
        }));
    }

    if (insightsData.length > 0) {
        // Data ko BigQuery mein insert karna
        await insertAdInsights(tableId, insightsData);
        return insightsData.length;
    }
    return 0;
}


export async function GET(request) {
    const results = {};
    const dateRange = { SINCE_DATE, UNTIL_DATE }; // Date range ko capture karen

    try {
        // Campaign Data Fetch and Insert
        results.campaigns = await fetchAndInsert('campaign', 'campaign_insights');
        
        // AdSet Data Fetch and Insert
        results.adsets = await fetchAndInsert('adset', 'fb_adset_insights');
        
        // Ad Data Fetch and Insert
        results.ads = await fetchAndInsert('ad', 'fb_ad_insights');

        // Success Response (Detailed)
        return new Response(JSON.stringify({
            message: `Data fetch complete for ${SINCE_DATE} to ${UNTIL_DATE}.`,
            results: results,
            dateRange: dateRange, // Frontend ke liye date range bhejna
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });

    } catch (error) {
        console.error('Master Fetch/Insert Error:', error);
        
        // --- Detailed Error Reporting ---
        let errorSource = 'Server Execution Error';
        let errorMessage = error.message;

        if (error.response) {
            // Facebook API Error (e.g., Auth, Rate Limit)
            errorSource = 'Facebook API Error';
            
            // Facebook error details ko JSON stringify karke bhejna
            const fbError = error.response.error || error.response;
            errorMessage = JSON.stringify(fbError, null, 2);

        } else if (error.code && error.code === 'ERR_BIGQUERY_INSERT') {
            // BigQuery Custom Error (Agar aapne lib/bigquery.js mein custom error code set kiya ho)
            errorSource = 'BigQuery Insert Error';
            errorMessage = 'BigQuery streaming failed. Check schema or permissions.';
        } else if (error.errors && error.errors[0].reason === 'invalid') {
            // BigQuery Native Error (e.g., Schema mismatch)
            errorSource = 'BigQuery Schema/Permission Error';
            errorMessage = JSON.stringify(error.errors, null, 2);
        }

        // Error Response (Detailed)
        return new Response(JSON.stringify({
            error: `Process Failed at ${errorSource}`,
            source: errorSource,
            details: errorMessage,
            dateRange: dateRange,
        }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
}