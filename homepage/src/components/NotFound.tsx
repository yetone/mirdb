import React from 'react';
import './NotFound.css';

export const NotFound: React.FC = () => {
  return (
    <div className="not-found" data-testid="not-found-page">
      <div className="not-found-container">
        <h1 className="not-found-title">404</h1>
        <h2 className="not-found-subtitle">Page Not Found</h2>
        <p className="not-found-message">
          The page you're looking for doesn't exist or has been moved.
        </p>
        <a href="/" className="not-found-link" data-testid="home-link">
          Return to Homepage
        </a>
      </div>
    </div>
  );
};
