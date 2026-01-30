/**
 * Smooth Scroll Utility.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Provides smooth scrolling functionality for anchor links.
 *
 * Expected exports:
 * - initSmoothScroll(): void
 * - scrollToElement(elementId: string): void
 * - handleHashNavigation(hash: string): void
 */

/**
 * Scrolls to an element by its ID with smooth scrolling behavior.
 * @param elementId - The ID of the element to scroll to (with or without # prefix)
 */
export function scrollToElement(elementId: string): void {
  // Remove hash prefix if present
  const cleanId = elementId.startsWith('#') ? elementId.slice(1) : elementId;

  const element = document.getElementById(cleanId);
  if (element) {
    element.scrollIntoView({
      behavior: 'smooth',
      block: 'start',
    });
  }
}

/**
 * Handles navigation to a hash/anchor in the URL.
 * Called on page load to scroll to the correct section if hash is present.
 * @param hash - The URL hash (e.g., '#features')
 */
export function handleHashNavigation(hash: string): void {
  if (hash && hash.length > 1) {
    scrollToElement(hash);
  }
}

/**
 * Sets up click handlers for all anchor links pointing to local sections.
 * Enables smooth scrolling when clicking on navigation links.
 */
function setupAnchorLinkHandlers(): void {
  document.addEventListener('click', (event) => {
    const target = event.target as HTMLElement;
    const anchor = target.closest('a[href^="#"]') as HTMLAnchorElement | null;

    if (anchor) {
      const href = anchor.getAttribute('href');
      if (href && href.startsWith('#') && href.length > 1) {
        event.preventDefault();
        scrollToElement(href);

        // Update URL hash without triggering scroll
        history.pushState(null, '', href);
      }
    }
  });
}

/**
 * Initializes smooth scrolling functionality for the page.
 * - Sets scroll-behavior: smooth on the html element
 * - Sets up click handlers for anchor links
 * - Handles initial hash navigation on page load
 */
export function initSmoothScroll(): void {
  // Apply smooth scroll behavior to the document
  document.documentElement.style.scrollBehavior = 'smooth';

  // Set up click handlers for anchor links
  setupAnchorLinkHandlers();

  // Handle initial hash navigation if present
  if (window.location.hash) {
    // Small delay to ensure page is fully loaded
    requestAnimationFrame(() => {
      handleHashNavigation(window.location.hash);
    });
  }

  // Handle hash changes (e.g., browser back/forward)
  window.addEventListener('hashchange', () => {
    handleHashNavigation(window.location.hash);
  });
}
