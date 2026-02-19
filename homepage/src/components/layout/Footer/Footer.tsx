/**
 * Footer Component
 * Owner: Scenario 8 - Footer & External Links
 *
 * Page footer containing:
 * - Copyright notice
 * - License information
 * - GitHub repository link
 * - CI/CD status badges
 * - Quick links
 *
 * Requirements: REQ-7, REQ-12
 * User Story: US-5
 */
import { siteConfig } from '@/data/config'
import { GITHUB_URL, GITHUB_REPO } from '@/utils/constants'
import styles from './Footer.module.css'

const CIRCLECI_BADGE_URL = `https://dl.circleci.com/status-badge/img/gh/${GITHUB_REPO}/tree/master.svg?style=svg`
const CIRCLECI_PIPELINE_URL = `https://dl.circleci.com/status-badge/redirect/gh/${GITHUB_REPO}/tree/master`

const currentYear = new Date().getFullYear()

export function Footer() {
  return (
    <footer className={styles.footer} role="contentinfo" data-testid="footer">
      <div className={styles.container}>
        <div className={styles.content}>
          {/* Branding and description */}
          <div className={styles.brand}>
            <span className={styles.brandName}>{siteConfig.name}</span>
            <p className={styles.description}>
              A high-performance persistent key-value store with Memcached protocol compatibility.
            </p>
          </div>

          {/* Links section */}
          <div className={styles.links}>
            <h3 className={styles.linksTitle}>Links</h3>
            <ul className={styles.linkList}>
              <li>
                <a
                  href={GITHUB_URL}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={styles.link}
                  data-testid="github-link"
                >
                  GitHub Repository
                </a>
              </li>
              <li>
                <a
                  href={`${GITHUB_URL}#readme`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={styles.link}
                  data-testid="docs-link"
                >
                  Documentation
                </a>
              </li>
              <li>
                <a
                  href={`${GITHUB_URL}/issues`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={styles.link}
                  data-testid="issues-link"
                >
                  Report Issues
                </a>
              </li>
            </ul>
          </div>

          {/* CI Badges section */}
          <div className={styles.badges}>
            <h3 className={styles.badgesTitle}>Build Status</h3>
            <a
              href={CIRCLECI_PIPELINE_URL}
              target="_blank"
              rel="noopener noreferrer"
              className={styles.badgeLink}
              aria-label="View CircleCI build status"
              data-testid="circleci-badge-link"
            >
              <img
                src={CIRCLECI_BADGE_URL}
                alt="CircleCI build status"
                className={styles.badgeImage}
                data-testid="circleci-badge"
              />
            </a>
          </div>
        </div>

        {/* Bottom bar with copyright and license */}
        <div className={styles.bottom}>
          <p className={styles.copyright} data-testid="copyright">
            &copy; {currentYear} {siteConfig.name}. All rights reserved.
          </p>
          <p className={styles.license} data-testid="license">
            Released under the{' '}
            <a
              href={`${GITHUB_URL}/blob/master/LICENSE`}
              target="_blank"
              rel="noopener noreferrer"
              className={styles.licenseLink}
              data-testid="license-link"
            >
              MIT License
            </a>
          </p>
        </div>
      </div>
    </footer>
  )
}
