/**
 * Hero Section Component.
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Requirements: REQ-1
 *
 * Renders the hero section with project title, tagline, brief description,
 * logo animation, and call-to-action buttons.
 */

const GITHUB_URL = 'https://github.com/jzwdsb/mirdb';

export function renderHero(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'hero';
  section.className = 'hero-section';
  section.setAttribute('aria-labelledby', 'hero-title');

  const container = document.createElement('div');
  container.className = 'container hero-container';

  // Logo animation
  const logoContainer = document.createElement('div');
  logoContainer.className = 'hero-logo';

  const logo = document.createElement('img');
  logo.src = '/assets/logo.gif';
  logo.alt = 'MirDB Logo';
  logo.className = 'hero-logo-image';
  logoContainer.appendChild(logo);

  // Content section
  const content = document.createElement('div');
  content.className = 'hero-content';

  // Title
  const title = document.createElement('h1');
  title.id = 'hero-title';
  title.className = 'hero-title';
  title.textContent = 'MirDB';

  // Tagline
  const tagline = document.createElement('p');
  tagline.className = 'hero-tagline';
  tagline.textContent = 'A Persistent Key-Value Store with Memcached Protocol';

  // Description
  const description = document.createElement('p');
  description.className = 'hero-description';
  description.textContent = 'MirDB is a high-performance, persistent key-value store written in Rust. ' +
    'It uses an LSM tree architecture for efficient storage and supports the Memcached protocol ' +
    'for seamless integration with existing applications.';

  // CTA Buttons container
  const ctaContainer = document.createElement('div');
  ctaContainer.className = 'hero-cta';

  // Get Started button
  const getStartedBtn = document.createElement('a');
  getStartedBtn.href = '#quick-start';
  getStartedBtn.className = 'hero-btn hero-btn-primary';
  getStartedBtn.textContent = 'Get Started';
  getStartedBtn.addEventListener('click', (e) => {
    e.preventDefault();
    const quickStartSection = document.getElementById('quick-start');
    if (quickStartSection) {
      quickStartSection.scrollIntoView({ behavior: 'smooth' });
    }
  });

  // View on GitHub button
  const githubBtn = document.createElement('a');
  githubBtn.href = GITHUB_URL;
  githubBtn.className = 'hero-btn hero-btn-secondary';
  githubBtn.textContent = 'View on GitHub';
  githubBtn.target = '_blank';
  githubBtn.rel = 'noopener noreferrer';

  ctaContainer.appendChild(getStartedBtn);
  ctaContainer.appendChild(githubBtn);

  content.appendChild(title);
  content.appendChild(tagline);
  content.appendChild(description);
  content.appendChild(ctaContainer);

  container.appendChild(logoContainer);
  container.appendChild(content);
  section.appendChild(container);

  return section;
}
