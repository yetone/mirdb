/**
 * Contributing Section Component
 * Owner: Scenario 7 - Contributing Section and GitHub Link
 *
 * Displays contribution information:
 * - GitHub repository link
 * - Contribution guidelines link
 * - Community links
 *
 * External links open in new tab with rel="noopener"
 */

import { CONTRIBUTING_LINKS } from '../../utils/constants';
import './Contributing.css';

interface ContributingLink {
  id: string;
  href: string;
  label: string;
  description: string;
}

const contributingLinks: ContributingLink[] = [
  {
    id: 'github',
    href: CONTRIBUTING_LINKS.github,
    label: 'GitHub Repository',
    description: 'View the source code and star the project',
  },
  {
    id: 'contributing-guidelines',
    href: CONTRIBUTING_LINKS.contributing,
    label: 'Contribution Guidelines',
    description: 'Learn how to contribute to MirDB',
  },
  {
    id: 'issues',
    href: CONTRIBUTING_LINKS.issues,
    label: 'Report Issues',
    description: 'Report bugs or request new features',
  },
  {
    id: 'discussions',
    href: CONTRIBUTING_LINKS.discussions,
    label: 'Discussions',
    description: 'Join the community conversation',
  },
];

export function Contributing() {
  return (
    <section className="contributing" id="contributing" aria-labelledby="contributing-heading">
      <div className="container">
        <h2 id="contributing-heading" className="contributing__title">
          Contribute
        </h2>
        <p className="contributing__subtitle">
          MirDB is open source. Join our community and help make it better.
        </p>
        <nav className="contributing__nav" aria-label="Contributing navigation">
          <ul className="contributing__list">
            {contributingLinks.map((link) => (
              <li key={link.id} className="contributing__item">
                <a
                  href={link.href}
                  className="contributing__link"
                  target="_blank"
                  rel="noopener noreferrer"
                  data-testid={`contributing-link-${link.id}`}
                >
                  <span className="contributing__link-icon" aria-hidden="true">
                    {getIcon(link.id)}
                  </span>
                  <span className="contributing__link-content">
                    <span className="contributing__link-label">{link.label}</span>
                    <span className="contributing__link-description">{link.description}</span>
                  </span>
                  <span className="contributing__external-icon" aria-hidden="true">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                      <polyline points="15,3 21,3 21,9" />
                      <line x1="10" y1="14" x2="21" y2="3" />
                    </svg>
                  </span>
                </a>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </section>
  );
}

function getIcon(id: string): JSX.Element {
  switch (id) {
    case 'github':
      return (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="currentColor">
          <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z" />
        </svg>
      );
    case 'contributing-guidelines':
      return (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
          <polyline points="14,2 14,8 20,8" />
          <line x1="16" y1="13" x2="8" y2="13" />
          <line x1="16" y1="17" x2="8" y2="17" />
          <polyline points="10,9 9,9 8,9" />
        </svg>
      );
    case 'issues':
      return (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <line x1="12" y1="8" x2="12" y2="12" />
          <line x1="12" y1="16" x2="12.01" y2="16" />
        </svg>
      );
    case 'discussions':
      return (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
        </svg>
      );
    default:
      return (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
        </svg>
      );
  }
}
