/**
 * Header Navigation Component.
 * Owner: Scenario 7 - Navigation and Header
 *
 * Requirements: REQ-8
 *
 * Provides:
 * - Logo
 * - Navigation links to all sections
 * - Smooth scrolling behavior
 * - Mobile hamburger menu
 */

import { initSmoothScroll } from '../utils/smoothScroll';

/** Navigation link definition */
interface NavLink {
  label: string;
  href: string;
}

/** Navigation links for all main sections */
const navLinks: NavLink[] = [
  { label: 'Features', href: '#features' },
  { label: 'Quick Start', href: '#quickstart' },
  { label: 'Protocol', href: '#protocol' },
  { label: 'Configuration', href: '#configuration' },
  { label: 'Architecture', href: '#architecture' },
];

/**
 * Creates the header navigation element.
 * @returns The header HTMLElement with logo, navigation, and mobile menu
 */
export function renderHeader(): HTMLElement {
  const header = document.createElement('header');
  header.className = 'header';
  header.setAttribute('role', 'banner');

  // Logo
  const logo = document.createElement('a');
  logo.className = 'header__logo';
  logo.href = '#';
  logo.setAttribute('aria-label', 'MirDB Home');

  const logoImg = document.createElement('img');
  logoImg.src = '/assets/logo.gif';
  logoImg.alt = 'MirDB Logo';
  logoImg.className = 'header__logo-img';
  logo.appendChild(logoImg);

  const logoText = document.createElement('span');
  logoText.className = 'header__logo-text';
  logoText.textContent = 'MirDB';
  logo.appendChild(logoText);

  // Navigation
  const nav = document.createElement('nav');
  nav.className = 'header__nav';
  nav.setAttribute('role', 'navigation');
  nav.setAttribute('aria-label', 'Main navigation');

  const navList = document.createElement('ul');
  navList.className = 'header__nav-list';
  navList.id = 'nav-list';

  navLinks.forEach((link) => {
    const li = document.createElement('li');
    li.className = 'header__nav-item';

    const a = document.createElement('a');
    a.className = 'header__nav-link';
    a.href = link.href;
    a.textContent = link.label;
    a.setAttribute('data-section', link.href.substring(1));

    li.appendChild(a);
    navList.appendChild(li);
  });

  nav.appendChild(navList);

  // Hamburger menu button (for mobile)
  const hamburger = document.createElement('button');
  hamburger.className = 'header__hamburger';
  hamburger.setAttribute('type', 'button');
  hamburger.setAttribute('aria-label', 'Toggle navigation menu');
  hamburger.setAttribute('aria-expanded', 'false');
  hamburger.setAttribute('aria-controls', 'nav-list');

  // Hamburger icon (3 lines)
  for (let i = 0; i < 3; i++) {
    const line = document.createElement('span');
    line.className = 'header__hamburger-line';
    hamburger.appendChild(line);
  }

  // Toggle menu on hamburger click
  hamburger.addEventListener('click', () => {
    const isExpanded = hamburger.getAttribute('aria-expanded') === 'true';
    hamburger.setAttribute('aria-expanded', String(!isExpanded));
    nav.classList.toggle('header__nav--open');
    hamburger.classList.toggle('header__hamburger--active');
  });

  // Close menu when clicking a nav link (mobile)
  navList.addEventListener('click', (event) => {
    const target = event.target as HTMLElement;
    if (target.classList.contains('header__nav-link')) {
      hamburger.setAttribute('aria-expanded', 'false');
      nav.classList.remove('header__nav--open');
      hamburger.classList.remove('header__hamburger--active');
    }
  });

  // Container for layout
  const container = document.createElement('div');
  container.className = 'header__container';
  container.appendChild(logo);
  container.appendChild(nav);
  container.appendChild(hamburger);

  header.appendChild(container);

  // Initialize smooth scrolling
  initSmoothScroll();

  return header;
}
