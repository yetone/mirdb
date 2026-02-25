/**
 * Footer Component.
 * Owner: Scenario 8 - Footer and Resources
 *
 * Requirements:
 * - REQ-8: License information and project resource links
 *
 * Content:
 * - GitHub repository link
 * - Documentation link
 * - License information
 * - Copyright notice
 */

import { GITHUB_URL } from '../../utils/constants';
import styles from './Footer.module.css';

const FOOTER_LINKS = [
  {
    label: 'GitHub',
    href: GITHUB_URL,
    isExternal: true,
  },
  {
    label: 'Documentation',
    href: `${GITHUB_URL}#readme`,
    isExternal: true,
  },
  {
    label: 'Quick Start',
    href: `${GITHUB_URL}#usage`,
    isExternal: true,
  },
];

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer className={styles.footer} role="contentinfo" id="resources">
      <div className={styles.container}>
        <div className={styles.content}>
          <div className={styles.links}>
            <h3 className={styles.linksTitle}>Resources</h3>
            <nav aria-label="Footer navigation">
              <ul className={styles.linksList} role="list">
                {FOOTER_LINKS.map((link) => (
                  <li key={link.label}>
                    <a
                      href={link.href}
                      className={styles.link}
                      target="_blank"
                      rel="noopener noreferrer"
                      aria-label={`${link.label} (opens in new tab)`}
                    >
                      {link.label}
                      <svg
                        className={styles.externalIcon}
                        width="12"
                        height="12"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        aria-hidden="true"
                      >
                        <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                        <polyline points="15 3 21 3 21 9" />
                        <line x1="10" y1="14" x2="21" y2="3" />
                      </svg>
                    </a>
                  </li>
                ))}
              </ul>
            </nav>
          </div>

          <div className={styles.info}>
            <div className={styles.license}>
              <span className={styles.licenseLabel}>License:</span>
              <a
                href={`${GITHUB_URL}/blob/master/LICENSE`}
                className={styles.licenseLink}
                target="_blank"
                rel="noopener noreferrer"
              >
                MIT License
              </a>
            </div>
          </div>
        </div>

        <div className={styles.bottom}>
          <p className={styles.copyright}>
            &copy; {currentYear} MirDB. All rights reserved.
          </p>
          <p className={styles.tagline}>
            A Persistent Key-Value Store with Memcached Protocol
          </p>
        </div>
      </div>
    </footer>
  );
}
