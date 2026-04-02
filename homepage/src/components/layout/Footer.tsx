/**
 * Footer component with links.
 * Owner: Scenario 7 - Footer Links and Information
 *
 * Contains:
 * - GitHub link (opens in new tab)
 * - License information
 * - Contact/Issues link
 * - Copyright notice
 */
import React from 'react';
import { Container } from '@/components/common/Container';
import { GITHUB_URL } from '@/utils/constants';
import styles from './Footer.module.css';

const ISSUES_URL = `${GITHUB_URL}/issues`;
const LICENSE_URL = `${GITHUB_URL}/blob/master/LICENSE`;

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer className={styles.footer} role="contentinfo" data-testid="footer">
      <Container className={styles.container}>
        <div className={styles.content}>
          <div className={styles.brand}>
            <span className={styles.logo}>MirDB</span>
            <p className={styles.copyright}>
              &copy; {currentYear} MirDB. All rights reserved.
            </p>
          </div>

          <nav className={styles.links} aria-label="Footer navigation">
            <a
              href={GITHUB_URL}
              target="_blank"
              rel="noopener noreferrer"
              className={styles.link}
              data-testid="footer-github-link"
            >
              GitHub
              <ExternalLinkIcon />
            </a>
            <a
              href={LICENSE_URL}
              target="_blank"
              rel="noopener noreferrer"
              className={styles.link}
              data-testid="footer-license-link"
            >
              License
              <ExternalLinkIcon />
            </a>
            <a
              href={ISSUES_URL}
              target="_blank"
              rel="noopener noreferrer"
              className={styles.link}
              data-testid="footer-issues-link"
            >
              Contact / Issues
              <ExternalLinkIcon />
            </a>
          </nav>
        </div>
      </Container>
    </footer>
  );
}

function ExternalLinkIcon() {
  return (
    <svg
      className={styles.externalIcon}
      width="12"
      height="12"
      viewBox="0 0 12 12"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <path
        d="M10.5 1.5L1.5 10.5M10.5 1.5H4.5M10.5 1.5V7.5"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
