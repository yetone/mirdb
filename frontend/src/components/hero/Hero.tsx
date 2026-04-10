/**
 * Hero section component
 *
 * Owner: Scenario 1 - Hero Section and Branding Display
 *
 * Features:
 * - MirDB branding and tagline
 * - Value proposition text
 * - Quick Start Guide button
 * - GitHub Repository button
 */

import { Link } from 'react-router-dom';
import './Hero.css';

export default function Hero() {
  return (
    <section className="hero" aria-labelledby="hero-title">
      <div className="hero-container">
        <div className="hero-logo" aria-hidden="true">
          <svg
            className="hero-logo-icon"
            width="80"
            height="80"
            viewBox="0 0 80 80"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <rect width="80" height="80" rx="16" fill="currentColor" />
            <path
              d="M20 30h40M20 40h30M20 50h20"
              stroke="white"
              strokeWidth="4"
              strokeLinecap="round"
            />
          </svg>
        </div>

        <h1 id="hero-title" className="hero-title">
          MirDB
        </h1>

        <p className="hero-tagline">
          Persistent Key-Value Store with Memcached Protocol
        </p>

        <p className="hero-description">
          MirDB combines the simplicity and speed of Memcached with the durability of
          persistent storage. Get started in seconds with your existing Memcached clients
          while gaining automatic data persistence, real-time metrics, and a modern web
          interface for monitoring and management.
        </p>

        <div className="hero-features">
          <div className="hero-feature">
            <span className="hero-feature-icon" aria-hidden="true">⚡</span>
            <span className="hero-feature-text">Lightning Fast</span>
          </div>
          <div className="hero-feature">
            <span className="hero-feature-icon" aria-hidden="true">💾</span>
            <span className="hero-feature-text">Persistent Storage</span>
          </div>
          <div className="hero-feature">
            <span className="hero-feature-icon" aria-hidden="true">🔄</span>
            <span className="hero-feature-text">Drop-in Replacement</span>
          </div>
        </div>

        <div className="hero-actions">
          <Link to="/docs" className="hero-button hero-button--primary">
            Quick Start Guide
          </Link>
          <a
            href="https://github.com/mirdb/mirdb"
            target="_blank"
            rel="noopener noreferrer"
            className="hero-button hero-button--secondary"
          >
            <svg
              className="hero-button-icon"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="currentColor"
              aria-hidden="true"
            >
              <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z" />
            </svg>
            GitHub Repository
          </a>
        </div>
      </div>
    </section>
  );
}
