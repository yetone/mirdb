/**
 * Footer Component
 * Owner: Scenario 10 - Footer and Project Metadata
 *
 * Footer section containing:
 * - CircleCI status badge
 * - GitHub repository link
 * - Project metadata
 * - Copyright information
 */

import React from 'react';
import { GITHUB_URL, SITE_TITLE } from '../../utils/constants';

const CIRCLECI_BADGE_URL = 'https://circleci.com/gh/yetone/mirdb.svg?style=svg';
const CIRCLECI_DASHBOARD_URL = 'https://circleci.com/gh/yetone/mirdb';

const footerStyles: React.CSSProperties = {
  backgroundColor: 'var(--bg-secondary)',
  borderTop: '1px solid var(--border-color)',
  padding: 'var(--spacing-2xl) var(--spacing-lg)',
  marginTop: 'auto',
};

const footerContainerStyles: React.CSSProperties = {
  maxWidth: 'var(--container-max-width)',
  margin: '0 auto',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  gap: 'var(--spacing-lg)',
};

const badgesSectionStyles: React.CSSProperties = {
  display: 'flex',
  flexWrap: 'wrap',
  justifyContent: 'center',
  gap: 'var(--spacing-md)',
  alignItems: 'center',
};

const linksSectionStyles: React.CSSProperties = {
  display: 'flex',
  flexWrap: 'wrap',
  justifyContent: 'center',
  gap: 'var(--spacing-lg)',
  alignItems: 'center',
};

const linkStyles: React.CSSProperties = {
  color: 'var(--accent-primary)',
  textDecoration: 'none',
  fontSize: 'var(--font-size-sm)',
  transition: 'color 0.2s ease',
};

const metadataSectionStyles: React.CSSProperties = {
  textAlign: 'center',
  color: 'var(--text-muted)',
  fontSize: 'var(--font-size-sm)',
};

const copyrightStyles: React.CSSProperties = {
  color: 'var(--text-muted)',
  fontSize: 'var(--font-size-sm)',
};

const badgeLinkStyles: React.CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
};

const badgeImageStyles: React.CSSProperties = {
  height: '20px',
};

export const Footer: React.FC = () => {
  const currentYear = new Date().getFullYear();

  return (
    <footer style={footerStyles} role="contentinfo" aria-label="Site footer">
      <div style={footerContainerStyles}>
        {/* Badges Section */}
        <div style={badgesSectionStyles} aria-label="Project badges">
          <a
            href={CIRCLECI_DASHBOARD_URL}
            target="_blank"
            rel="noopener noreferrer"
            style={badgeLinkStyles}
            aria-label="View CircleCI build status"
            data-testid="circleci-badge-link"
          >
            <img
              src={CIRCLECI_BADGE_URL}
              alt="CircleCI build status"
              style={badgeImageStyles}
              data-testid="circleci-badge"
            />
          </a>
        </div>

        {/* Links Section */}
        <nav style={linksSectionStyles} aria-label="Footer navigation">
          <a
            href={GITHUB_URL}
            target="_blank"
            rel="noopener noreferrer"
            style={linkStyles}
            aria-label="View MirDB on GitHub"
            data-testid="github-link"
          >
            GitHub Repository
          </a>
          <a
            href={`${GITHUB_URL}/issues`}
            target="_blank"
            rel="noopener noreferrer"
            style={linkStyles}
            aria-label="Report issues on GitHub"
          >
            Report Issues
          </a>
          <a
            href={`${GITHUB_URL}#readme`}
            target="_blank"
            rel="noopener noreferrer"
            style={linkStyles}
            aria-label="View documentation"
          >
            Documentation
          </a>
        </nav>

        {/* Project Metadata */}
        <div style={metadataSectionStyles}>
          <p>
            {SITE_TITLE} - A Persistent Key-Value Store with Memcached Protocol
          </p>
          <p>Built with Rust and Tokio for maximum performance</p>
        </div>

        {/* Copyright */}
        <div style={copyrightStyles}>
          <p>&copy; {currentYear} {SITE_TITLE}. MIT License.</p>
        </div>
      </div>
    </footer>
  );
};

export default Footer;
