/**
 * Footer Component.
 * Owner: Scenario 8 - Footer Section
 *
 * Requirements: REQ-9
 *
 * Renders the footer section with GitHub repository link,
 * license information, and project credits.
 */

const GITHUB_URL = 'https://github.com/jzwdsb/mirdb';
const CURRENT_YEAR = new Date().getFullYear();

export function renderFooter(): HTMLElement {
  const footer = document.createElement('footer');
  footer.id = 'footer';
  footer.className = 'footer-section';
  footer.setAttribute('aria-label', 'Site footer');

  const container = document.createElement('div');
  container.className = 'container footer-container';

  // Top section with logo/branding and links
  const topSection = document.createElement('div');
  topSection.className = 'footer-top';

  // Branding/Logo section
  const branding = document.createElement('div');
  branding.className = 'footer-branding';

  const brandTitle = document.createElement('span');
  brandTitle.className = 'footer-brand-title';
  brandTitle.textContent = 'MirDB';
  branding.appendChild(brandTitle);

  const brandTagline = document.createElement('p');
  brandTagline.className = 'footer-brand-tagline';
  brandTagline.textContent = 'A Persistent Key-Value Store with Memcached Protocol';
  branding.appendChild(brandTagline);

  // Links section
  const linksSection = document.createElement('div');
  linksSection.className = 'footer-links';

  // GitHub link with icon
  const githubLink = document.createElement('a');
  githubLink.href = GITHUB_URL;
  githubLink.target = '_blank';
  githubLink.rel = 'noopener noreferrer';
  githubLink.className = 'footer-link footer-github-link';
  githubLink.setAttribute('aria-label', 'View MirDB on GitHub');

  // GitHub SVG icon
  const githubIcon = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  githubIcon.setAttribute('width', '20');
  githubIcon.setAttribute('height', '20');
  githubIcon.setAttribute('viewBox', '0 0 24 24');
  githubIcon.setAttribute('fill', 'currentColor');
  githubIcon.setAttribute('aria-hidden', 'true');
  githubIcon.innerHTML = '<path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>';
  githubLink.appendChild(githubIcon);

  const githubText = document.createElement('span');
  githubText.textContent = 'GitHub';
  githubLink.appendChild(githubText);

  linksSection.appendChild(githubLink);

  topSection.appendChild(branding);
  topSection.appendChild(linksSection);

  // Bottom section with license and credits
  const bottomSection = document.createElement('div');
  bottomSection.className = 'footer-bottom';

  // License information
  const license = document.createElement('div');
  license.className = 'footer-license';
  license.innerHTML = `Released under the <a href="${GITHUB_URL}/blob/master/LICENSE" target="_blank" rel="noopener noreferrer" class="footer-license-link">MIT License</a>`;

  // Credits section
  const credits = document.createElement('div');
  credits.className = 'footer-credits';
  credits.innerHTML = `<span>© ${CURRENT_YEAR} MirDB. Built with Rust.</span>`;

  bottomSection.appendChild(license);
  bottomSection.appendChild(credits);

  container.appendChild(topSection);
  container.appendChild(bottomSection);
  footer.appendChild(container);

  return footer;
}
