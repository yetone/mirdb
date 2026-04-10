/**
 * Footer component
 *
 * Owner: Scenario 1 - Hero Section and Branding Display
 */

import './Footer.css';

export default function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="footer" role="contentinfo">
      <div className="footer-container">
        <p className="footer-text">
          &copy; {currentYear} MirDB. Persistent Key-Value Store with Memcached Protocol.
        </p>
        <p className="footer-links">
          <a
            href="https://github.com/mirdb/mirdb"
            target="_blank"
            rel="noopener noreferrer"
            className="footer-link"
          >
            GitHub
          </a>
          <span className="footer-separator" aria-hidden="true">
            &middot;
          </span>
          <a
            href="https://github.com/mirdb/mirdb/issues"
            target="_blank"
            rel="noopener noreferrer"
            className="footer-link"
          >
            Report Issue
          </a>
        </p>
      </div>
    </footer>
  );
}
