/**
 * Smooth Scroll Utility.
 * Owner: Scenario 7 - Navigation and Header
 *
 * Provides smooth scrolling functionality for navigation between sections.
 */

/**
 * Smoothly scrolls to a section with the given ID.
 * @param sectionId - The ID of the section to scroll to (without # prefix)
 */
export function scrollToSection(sectionId: string): void {
  const element = document.getElementById(sectionId);
  if (element) {
    element.scrollIntoView({
      behavior: 'smooth',
      block: 'start',
    });
  }
}

/**
 * Initializes smooth scrolling for all anchor links with hash references.
 * Attaches click handlers to links that point to page sections.
 */
export function initSmoothScroll(): void {
  document.addEventListener('click', (event: MouseEvent) => {
    const target = event.target as HTMLElement;
    const anchor = target.closest('a[href^="#"]') as HTMLAnchorElement | null;

    if (anchor) {
      const href = anchor.getAttribute('href');
      if (href && href.startsWith('#') && href.length > 1) {
        event.preventDefault();
        const sectionId = href.substring(1);
        scrollToSection(sectionId);

        // Update URL hash without jumping
        if (window.history.pushState) {
          window.history.pushState(null, '', href);
        }
      }
    }
  });
}
