/**
 * Documentation Links Section Component
 * Owner: Scenario 6 - Documentation Links
 *
 * Navigation to documentation sections:
 * - Architecture docs
 * - API Reference
 * - Configuration guide
 */

import { documentationLinks } from '../../data/navigation';
import './Documentation.css';

export function Documentation() {
  return (
    <section className="documentation" id="documentation" aria-labelledby="documentation-heading">
      <div className="container">
        <h2 id="documentation-heading" className="documentation__title">
          Documentation
        </h2>
        <p className="documentation__subtitle">
          Everything you need to get started and master MirDB
        </p>
        <nav className="documentation__nav" aria-label="Documentation navigation">
          <ul className="documentation__list">
            {documentationLinks.map((link) => (
              <li key={link.id} className="documentation__item">
                <a
                  href={link.href}
                  className="documentation__link"
                  target={link.external ? '_blank' : undefined}
                  rel={link.external ? 'noopener noreferrer' : undefined}
                  data-testid={`doc-link-${link.id}`}
                >
                  <span className="documentation__link-label">{link.label}</span>
                  {link.description && (
                    <span className="documentation__link-description">{link.description}</span>
                  )}
                  {link.external && (
                    <span className="documentation__external-icon" aria-hidden="true">
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                        <polyline points="15,3 21,3 21,9" />
                        <line x1="10" y1="14" x2="21" y2="3" />
                      </svg>
                    </span>
                  )}
                </a>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </section>
  );
}
