/**
 * Navigation utilities.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Functions:
 * - initSmoothScroll(): void
 * - updateActiveSection(): void
 * - scrollToSection(sectionId: string): void
 */

/** Navigation section IDs that can be scrolled to */
export const NAV_SECTIONS = ['features', 'quick-start', 'architecture', 'commands'] as const;
export type NavSection = (typeof NAV_SECTIONS)[number];

/**
 * Scrolls to a section by ID with smooth behavior
 * @param sectionId - The ID of the section to scroll to (without #)
 */
export function scrollToSection(sectionId: string): void {
  const element = document.getElementById(sectionId);
  if (element) {
    element.scrollIntoView({ behavior: 'smooth', block: 'start' });
    // Update URL hash without triggering scroll (already scrolling)
    history.pushState(null, '', `#${sectionId}`);
  }
}

/**
 * Updates the active navigation link based on scroll position
 */
export function updateActiveSection(): void {
  const sections = NAV_SECTIONS.map((id) => document.getElementById(id)).filter(
    (el): el is HTMLElement => el !== null
  );

  const navLinks = document.querySelectorAll<HTMLAnchorElement>('.nav-link');

  // Find the section that's currently most visible
  let activeSection: string | null = null;
  const scrollY = window.scrollY;
  const viewportHeight = window.innerHeight;

  for (const section of sections) {
    const rect = section.getBoundingClientRect();
    const sectionTop = rect.top + scrollY;

    // Consider a section active if its top is within the top half of viewport
    // or if we've scrolled past the start but not past the end
    if (sectionTop <= scrollY + viewportHeight / 3) {
      activeSection = section.id;
    }
  }

  // Update active class on nav links
  navLinks.forEach((link) => {
    const href = link.getAttribute('href');
    if (href && href.startsWith('#')) {
      const sectionId = href.slice(1);
      if (sectionId === activeSection) {
        link.classList.add('active');
        link.setAttribute('aria-current', 'true');
      } else {
        link.classList.remove('active');
        link.removeAttribute('aria-current');
      }
    }
  });
}

/**
 * Handles navigation link clicks for smooth scrolling
 * @param event - The click event
 */
function handleNavClick(event: Event): void {
  const target = event.target as HTMLElement;
  const link = target.closest('a[href^="#"]') as HTMLAnchorElement | null;

  if (link) {
    event.preventDefault();
    const href = link.getAttribute('href');
    if (href && href.startsWith('#')) {
      const sectionId = href.slice(1);
      scrollToSection(sectionId);
    }
  }
}

/**
 * Handles initial page load with hash in URL
 */
function handleInitialHash(): void {
  const hash = window.location.hash;
  if (hash && hash.length > 1) {
    const sectionId = hash.slice(1);
    // Small delay to ensure DOM is ready
    setTimeout(() => {
      scrollToSection(sectionId);
    }, 100);
  }
}

/**
 * Initializes smooth scroll functionality for navigation
 */
export function initSmoothScroll(): void {
  // Handle navigation link clicks
  const nav = document.querySelector('.nav');
  if (nav) {
    nav.addEventListener('click', handleNavClick);
  }

  // Update active section on scroll
  let ticking = false;
  window.addEventListener('scroll', () => {
    if (!ticking) {
      window.requestAnimationFrame(() => {
        updateActiveSection();
        ticking = false;
      });
      ticking = true;
    }
  });

  // Handle browser back/forward navigation
  window.addEventListener('popstate', () => {
    const hash = window.location.hash;
    if (hash && hash.length > 1) {
      const sectionId = hash.slice(1);
      const element = document.getElementById(sectionId);
      if (element) {
        element.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }
  });

  // Handle initial hash on page load
  handleInitialHash();

  // Initial active section update
  updateActiveSection();
}
