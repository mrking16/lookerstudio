// app/page.js
'use client'

import { useState } from 'react';

export default function HomePage() {
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('Click the button to start the ETL process.');
  const [errorDetails, setErrorDetails] = useState(null);

  const fetchData = async () => {
    setLoading(true);
    setMessage('Fetching data from Facebook and inserting into BigQuery...');
    setErrorDetails(null);

    try {
      const response = await fetch('/api/fetchData');
      const data = await response.json();

      if (response.ok) {
        // SUCCESS CASE
        setMessage(`Success! Data Sync Complete. Total rows appended: 
          Campaigns: ${data.results.campaigns}, 
          AdSets: ${data.results.adsets}, 
          Ads: ${data.results.ads}. 
          Date Range: ${data.dateRange.SINCE_DATE} to ${data.dateRange.UNTIL_DATE}.`);
        
        setErrorDetails(null); // Clear previous errors

      } else {
        // API Route returned a 500 status (Server Error)
        
        // Error source ko determine karna
        let source = 'Unknown Error';
        if (data.source) {
            source = data.source;
        } else if (data.details.includes('FacebookAdsApi')) {
            source = 'Facebook API (Auth/Token/RateLimit)';
        } else if (data.details.includes('BigQuery')) {
            source = 'BigQuery (Schema/Connection)';
        }

        setMessage('Process Failed. Check Error Details below.');
        setErrorDetails({
          source: source,
          summary: data.error,
          details: data.details,
        });
      }
    } catch (e) {
      // Network Error (e.g., Server down, Next.js could not connect)
      console.error(e);
      setMessage('Process Failed due to Network/JSON Error.');
      setErrorDetails({
        source: 'Network/JSON Error',
        summary: 'Could not connect to the API or received invalid JSON.',
        details: e.message,
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: '20px', fontFamily: 'Arial' }}>
      <h1>Facebook Ads to BigQuery ETL 🚀</h1>
      <p>This page triggers the backend API route (`/api/fetchData`) to pull data from Facebook Marketing API and append it to your BigQuery tables.</p>
      
      <button 
        onClick={fetchData} 
        disabled={loading}
        style={{
          padding: '10px 20px',
          fontSize: '16px',
          cursor: loading ? 'not-allowed' : 'pointer',
          backgroundColor: loading ? '#ccc' : '#4CAF50',
          color: 'white',
          border: 'none',
          borderRadius: '5px',
          marginTop: '15px'
        }}
      >
        {loading ? 'Processing...' : 'Run Data Sync Now'}
      </button>

      <div style={{ 
        marginTop: '30px', 
        border: '1px solid #eee', 
        padding: '15px',
        backgroundColor: errorDetails ? '#fdecea' : '#e6ffe6',
        borderRadius: '5px'
      }}>
        <h2>Status</h2>
        <p style={{ color: errorDetails ? 'red' : 'green', fontWeight: 'bold' }}>{message}</p>
        
        {errorDetails && (
          <div style={{ marginTop: '15px', borderLeft: '3px solid red', paddingLeft: '10px' }}>
            <h3 style={{ color: 'red' }}>🛑 Error Details:</h3>
            <p><strong>Source:</strong> {errorDetails.source}</p>
            <p><strong>Summary:</strong> {errorDetails.summary}</p>
            <p><strong>Details/Exception:</strong> 
              <pre style={{ 
                whiteSpace: 'pre-wrap', 
                backgroundColor: '#f8f8f8', 
                padding: '10px', 
                borderRadius: '3px',
                fontSize: '12px'
              }}>
                {errorDetails.details}
              </pre>
            </p>
            
            {/* Contextual Guidance */}
            {errorDetails.source.includes('Facebook API') && (
                <p style={{ fontWeight: 'bold', color: '#c00' }}>Guidance: Check your `FACEBOOK_ACCESS_TOKEN` in `.env.local` and ensure the `FACEBOOK_AD_ACCOUNT_ID` is correct and active.</p>
            )}
            {errorDetails.source.includes('BigQuery') && (
                <p style={{ fontWeight: 'bold', color: '#c00' }}>Guidance: Check BigQuery logs and verify that all necessary columns were added correctly and the service account has permission to insert data.</p>
            )}

          </div>
        )}
      </div>
    </div>
  );
}