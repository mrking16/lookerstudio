// app/page.js
'use client'

import { useState } from 'react';
import './page.css';

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
    <div className="etl-container">
      <div className="etl-card">
        <div className="etl-header">
          <div className="etl-title-container">
            <h1 className="etl-title">Facebook Ads to BigQuery ETL</h1>
            <div className="etl-icon">🚀</div>
          </div>
          <p className="etl-description">
            This page triggers the backend API route (<code>/api/fetchData</code>) to pull data from Facebook Marketing API and append it to your BigQuery tables.
          </p>
        </div>
        
        <div className="etl-action">
          <button 
            onClick={fetchData} 
            disabled={loading}
            className={`etl-button ${loading ? 'loading' : ''}`}
          >
            {loading ? (
              <span className="button-content">
                <span className="spinner"></span>
                Processing...
              </span>
            ) : (
              'Run Data Sync Now'
            )}
          </button>
        </div>

        <div className={`etl-status ${errorDetails ? 'error' : 'success'}`}>
          <div className="status-header">
            <h2>Status</h2>
            <div className={`status-indicator ${errorDetails ? 'error' : 'success'}`}></div>
          </div>
          <p className="status-message">{message}</p>
          
          {errorDetails && (
            <div className="error-details">
              <h3 className="error-title">🛑 Error Details:</h3>
              <div className="error-info">
                <p><span className="error-label">Source:</span> {errorDetails.source}</p>
                <p><span className="error-label">Summary:</span> {errorDetails.summary}</p>
                <div className="error-details-container">
                  <span className="error-label">Details/Exception:</span>
                  <pre className="error-code">{errorDetails.details}</pre>
                </div>
              </div>
              
              {/* Contextual Guidance */}
              <div className="error-guidance">
                {errorDetails.source.includes('Facebook API') && (
                  <div className="guidance-item">
                    <span className="guidance-icon">🔑</span>
                    <p>Check your <code>FACEBOOK_ACCESS_TOKEN</code> in <code>.env.local</code> and ensure the <code>FACEBOOK_AD_ACCOUNT_ID</code> is correct and active.</p>
                  </div>
                )}
                {errorDetails.source.includes('BigQuery') && (
                  <div className="guidance-item">
                    <span className="guidance-icon">📊</span>
                    <p>Check BigQuery logs and verify that all necessary columns were added correctly and the service account has permission to insert data.</p>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}