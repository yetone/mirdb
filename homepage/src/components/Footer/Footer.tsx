/**
 * Footer Component
 * Owner: Scenario 17 - Footer and Supplementary Content
 *
 * Page footer containing:
 * - GitHub repository link
 * - License information
 * - Copyright/attribution
 */

import { ExternalLink } from '../common/ExternalLink';
import { GITHUB_URL, PRODUCT_NAME } from '../../utils/constants';
import './Footer.css';

const LICENSE_URL = `${GITHUB_URL}/blob/master/LICENSE`;
const CURRENT_YEAR = new Date().getFullYear();

export function Footer() {
  return (
    <footer id="footer" className="footer" role="contentinfo">
      <div className="container footer-content">
        <div className="footer-links">
          <ExternalLink href={GITHUB_URL} className="footer-link">
            GitHub Repository
          </ExternalLink>
          <span className="footer-separator" aria-hidden="true">|</span>
          <ExternalLink href={LICENSE_URL} className="footer-link">
            MIT License
          </ExternalLink>
        </div>
        <p className="footer-attribution">
          &copy; {CURRENT_YEAR} {PRODUCT_NAME}. Built with Rust.
        </p>
      </div>
    </footer>
  );
}

export default Footer;
