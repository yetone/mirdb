/**
 * Usage Demonstration Component
 * Owner: Scenario 4 - Usage Demonstration
 *
 * Displays the usage.gif animation with explanatory text and a link to documentation.
 */

import { DOCUMENTATION_URL } from '../../utils/constants';
import './Usage.css';

export function Usage() {
  return (
    <section id="usage" className="usage" aria-labelledby="usage-title" data-testid="usage-section">
      <div className="usage-container">
        <h2 id="usage-title" className="usage-title">
          See It in Action
        </h2>
        <p className="usage-description" data-testid="usage-explanation">
          Watch how easy it is to use MirDB. The demonstration below shows basic key-value operations
          using the Memcached protocol, including setting and retrieving values.
        </p>

        <div className="usage-demo" data-testid="usage-demo">
          <img
            src="/assets/usage.gif"
            alt="MirDB usage demonstration showing key-value operations with Memcached protocol commands"
            className="usage-gif"
            data-testid="usage-gif"
            loading="lazy"
          />
        </div>

        <div className="usage-cta">
          <p className="usage-cta-text">
            Ready to dive deeper? Check out the full documentation for detailed usage examples,
            configuration options, and API reference.
          </p>
          <a
            href={DOCUMENTATION_URL}
            className="usage-docs-link"
            target="_blank"
            rel="noopener noreferrer"
            data-testid="documentation-link"
          >
            View Full Documentation
            <ExternalLinkIcon />
          </a>
        </div>
      </div>
    </section>
  );
}

function ExternalLinkIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      className="external-link-icon"
    >
      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
      <polyline points="15 3 21 3 21 9" />
      <line x1="10" y1="14" x2="21" y2="3" />
    </svg>
  );
}
