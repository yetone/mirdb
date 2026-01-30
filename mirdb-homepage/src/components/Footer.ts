/**
 * Footer Component.
 * Owner: Scenario 6 - Footer and Project Badges
 *
 * Displays:
 * - CI status badge (CircleCI)
 * - Version badge (Crates.io)
 * - License badge
 * - GitHub repository link
 * - Author attribution
 *
 * All external links use rel="noopener noreferrer"
 *
 * Expected exports:
 * - renderFooter(): HTMLElement
 * - BADGES: Badge[]
 */

import type { Badge } from '../types/index';

/**
 * Badge data for MirDB project
 */
export const BADGES: Badge[] = [
  {
    type: 'ci',
    url: 'https://circleci.com/gh/pjtatlow/mirdb.svg?style=shield',
    alt: 'CircleCI CI Status',
    link: 'https://circleci.com/gh/pjtatlow/mirdb',
  },
  {
    type: 'version',
    url: 'https://img.shields.io/crates/v/mirdb.svg',
    alt: 'Crates.io Version',
    link: 'https://crates.io/crates/mirdb',
  },
  {
    type: 'license',
    url: 'https://img.shields.io/badge/license-MIT-blue.svg',
    alt: 'MIT License',
    link: 'https://github.com/pjtatlow/mirdb/blob/main/LICENSE',
  },
];

/**
 * Creates a badge element with link
 */
function createBadge(badge: Badge): HTMLElement {
  const link = document.createElement('a');
  link.href = badge.link;
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  link.className = 'badge-link inline-block transition-opacity duration-200 hover:opacity-80';
  link.setAttribute('data-testid', 'badge');

  const container = document.createElement('span');
  container.setAttribute('data-testid', `badge-${badge.type}`);
  container.className = 'badge-container';

  const img = document.createElement('img');
  img.src = badge.url;
  img.alt = badge.alt;
  img.className = 'badge-image h-5';
  img.loading = 'lazy';

  container.appendChild(img);
  link.appendChild(container);

  return link;
}

/**
 * Creates the badges section
 */
function createBadgesSection(): HTMLElement {
  const section = document.createElement('div');
  section.className = 'badges-section flex flex-wrap gap-4 justify-center mb-6';
  section.setAttribute('data-testid', 'badges-section');

  BADGES.forEach((badge) => {
    const badgeElement = createBadge(badge);
    section.appendChild(badgeElement);
  });

  return section;
}

/**
 * Creates the GitHub repository link
 */
function createGitHubLink(): HTMLElement {
  const link = document.createElement('a');
  link.href = 'https://github.com/pjtatlow/mirdb';
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  link.className = 'github-link flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors duration-200';
  link.setAttribute('data-testid', 'github-link');

  // GitHub SVG icon
  const icon = document.createElement('span');
  icon.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" class="w-6 h-6">
    <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd"/>
  </svg>`;
  icon.className = 'github-icon';

  const text = document.createElement('span');
  text.textContent = 'View on GitHub';
  text.className = 'github-text font-medium';

  link.appendChild(icon);
  link.appendChild(text);

  return link;
}

/**
 * Creates the license information section
 */
function createLicenseInfo(): HTMLElement {
  const container = document.createElement('div');
  container.className = 'license-info text-gray-500 text-sm';
  container.setAttribute('data-testid', 'license-info');

  const text = document.createElement('span');
  text.textContent = 'Licensed under ';

  const link = document.createElement('a');
  link.href = 'https://github.com/pjtatlow/mirdb/blob/main/LICENSE';
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  link.textContent = 'MIT License';
  link.className = 'text-blue-600 hover:text-blue-800 hover:underline';

  container.appendChild(text);
  container.appendChild(link);

  return container;
}

/**
 * Creates the author attribution section
 */
function createAuthorAttribution(): HTMLElement {
  const container = document.createElement('div');
  container.className = 'author-attribution text-gray-500 text-sm mt-2';
  container.setAttribute('data-testid', 'author-attribution');

  const text = document.createElement('span');
  text.textContent = 'Created and maintained by the MirDB contributors';

  container.appendChild(text);

  return container;
}

/**
 * Renders the footer component
 * @returns HTMLElement - The footer element
 */
export function renderFooter(): HTMLElement {
  const footer = document.createElement('footer');
  footer.className = 'footer-section bg-gray-100 py-12 px-4 border-t border-gray-200';
  footer.id = 'footer';

  const container = document.createElement('div');
  container.className = 'footer-container max-w-6xl mx-auto text-center';

  // Add badges section
  const badgesSection = createBadgesSection();
  container.appendChild(badgesSection);

  // Add GitHub link
  const linksSection = document.createElement('div');
  linksSection.className = 'links-section flex flex-col items-center gap-4 mt-6';

  const githubLink = createGitHubLink();
  linksSection.appendChild(githubLink);

  // Add license info
  const licenseInfo = createLicenseInfo();
  linksSection.appendChild(licenseInfo);

  // Add author attribution
  const authorAttribution = createAuthorAttribution();
  linksSection.appendChild(authorAttribution);

  container.appendChild(linksSection);
  footer.appendChild(container);

  return footer;
}
