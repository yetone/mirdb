/**
 * Smooth Scroll Module
 * Owner: Scenario 8 - Navigation and Footer
 *
 * Features:
 * - Smooth scrolling for navigation links
 * - Respects prefers-reduced-motion preference
 * - Updates URL hash on scroll
 */

/**
 * Check if user prefers reduced motion
 * @returns {boolean} True if reduced motion is preferred
 */
function prefersReducedMotion() {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/**
 * Initialize smooth scroll for all anchor links that point to page sections
 * Attaches click listeners to anchor links with href starting with '#'
 */
function initSmoothScroll() {
    const anchors = document.querySelectorAll('a[href^="#"]');

    anchors.forEach(function(anchor) {
        anchor.addEventListener('click', function(e) {
            const href = this.getAttribute('href');

            // Skip if just '#' or empty
            if (!href || href === '#') return;

            const target = document.querySelector(href);

            if (target) {
                e.preventDefault();

                // Use scrollToElement for consistent behavior
                scrollToElement(href);
            }
        });
    });
}

/**
 * Scroll to a specific element on the page
 * @param {string} selector - CSS selector for the target element
 */
function scrollToElement(selector) {
    const target = document.querySelector(selector);

    if (!target) {
        return;
    }

    // Check for reduced motion preference
    const useReducedMotion = prefersReducedMotion();

    target.scrollIntoView({
        behavior: useReducedMotion ? 'auto' : 'smooth',
        block: 'start'
    });

    // Update URL hash if selector starts with #
    if (selector.startsWith('#')) {
        history.pushState(null, '', selector);
    }
}

// Export functions for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { initSmoothScroll, scrollToElement, prefersReducedMotion };
}

// Also expose globally for non-module usage
if (typeof window !== 'undefined') {
    window.initSmoothScroll = initSmoothScroll;
    window.scrollToElement = scrollToElement;
}
