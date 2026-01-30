/**
 * Navigation Component.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Provides:
 * - Navigation menu with links to sections
 * - Active state highlighting for current section
 * - Mobile-responsive hamburger menu (if applicable)
 * - Integration with smooth-scroll utility
 *
 * Expected exports:
 * - renderNavigation(): HTMLElement
 * - initNavigation(): void
 * - NAV_ITEMS: NavigationItem[]
 * - setActiveNavItem(sectionId: string): void
 */

import type { NavigationItem } from '../types';

/**
 * Navigation items configuration.
 * Each item corresponds to a section on the homepage.
 */
export const NAV_ITEMS: NavigationItem[] = [
  { label: 'Features', href: '#features' },
  { label: 'Usage', href: '#usage' },
  { label: 'Architecture', href: '#architecture' },
  { label: 'Getting Started', href: '#getting-started' },
];

// Store reference to the rendered navigation element
let navElement: HTMLElement | null = null;

/**
 * Creates a navigation link element.
 * @param item - The navigation item configuration
 * @returns The anchor element for the navigation link
 */
function createNavLink(item: NavigationItem): HTMLAnchorElement {
  const link = document.createElement('a');
  link.href = item.href;
  link.textContent = item.label;
  link.className = 'nav-link';
  link.setAttribute('aria-current', 'false');
  return link;
}

/**
 * Sets the active navigation item based on the current section.
 * @param sectionId - The ID of the currently visible section (without hash)
 */
export function setActiveNavItem(sectionId: string): void {
  if (!navElement) return;

  const links = navElement.querySelectorAll('.nav-link');
  links.forEach((link) => {
    const href = link.getAttribute('href');
    const isActive = href === `#${sectionId}`;

    link.classList.toggle('active', isActive);
    link.setAttribute('aria-current', isActive ? 'true' : 'false');
  });
}

/**
 * Creates and returns the navigation element.
 * @returns The navigation HTMLElement
 */
export function renderNavigation(): HTMLElement {
  const nav = document.createElement('nav');
  nav.id = 'navigation';
  nav.className = 'main-nav';
  nav.setAttribute('aria-label', 'Main navigation');

  // Create navigation container
  const container = document.createElement('div');
  container.className = 'nav-container';

  // Create brand/logo link
  const brandLink = document.createElement('a');
  brandLink.href = '#';
  brandLink.className = 'nav-brand';
  brandLink.textContent = 'MirDB';
  container.appendChild(brandLink);

  // Create navigation links container
  const linksContainer = document.createElement('div');
  linksContainer.className = 'nav-links';

  // Add navigation links
  NAV_ITEMS.forEach((item) => {
    const link = createNavLink(item);
    linksContainer.appendChild(link);
  });

  container.appendChild(linksContainer);
  nav.appendChild(container);

  // Store reference for later use
  navElement = nav;

  return nav;
}

/**
 * Sets up the Intersection Observer to track which section is currently visible.
 * Updates the active navigation item based on scroll position.
 */
function setupScrollTracking(): void {
  const sections = document.querySelectorAll('section[id]');

  const observerOptions: IntersectionObserverInit = {
    root: null,
    rootMargin: '-20% 0px -60% 0px', // Trigger when section is in upper portion of viewport
    threshold: 0,
  };

  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        const sectionId = entry.target.getAttribute('id');
        if (sectionId) {
          setActiveNavItem(sectionId);
        }
      }
    });
  }, observerOptions);

  sections.forEach((section) => {
    observer.observe(section);
  });
}

/**
 * Initializes the navigation component.
 * - Sets up scroll tracking for active state
 * - Handles initial active state based on URL hash or scroll position
 */
export function initNavigation(): void {
  // Set up scroll tracking for active state highlighting
  setupScrollTracking();

  // Set initial active state based on URL hash
  if (window.location.hash) {
    const hash = window.location.hash.slice(1);
    setActiveNavItem(hash);
  }
}
