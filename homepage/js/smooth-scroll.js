/**
 * Smooth Scroll Module for Navigation
 * Owner: Scenario 1 - Hero Section & Navigation
 *
 * Expected exports:
 * - initSmoothScroll(): void - Initialize smooth scroll for anchor links
 * - scrollToElement(id: string): void
 *
 * Handles navigation link clicks with smooth scrolling animation.
 */

/**
 * Scrolls the page smoothly to the element with the given ID.
 * Updates the URL hash without triggering a page reload.
 *
 * @param {string} id - The ID of the element to scroll to (without #)
 * @returns {void}
 */
function scrollToElement(id) {
    var element = document.getElementById(id);
    if (!element) {
        return;
    }

    // Get the header height for offset calculation
    var header = document.querySelector('.main-nav');
    var headerHeight = header ? header.offsetHeight : 0;

    // Calculate the target position
    var elementPosition = element.getBoundingClientRect().top;
    var offsetPosition = elementPosition + window.pageYOffset - headerHeight;

    // Perform smooth scroll
    window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth'
    });

    // Update URL hash without triggering scroll
    if (history.pushState) {
        history.pushState(null, null, '#' + id);
    } else {
        // Fallback for older browsers
        window.location.hash = id;
    }
}

/**
 * Initializes smooth scrolling for all anchor links on the page.
 * Attaches click event handlers to links that start with '#'.
 *
 * @returns {void}
 */
function initSmoothScroll() {
    // Select all anchor links that point to an ID on the page
    var anchorLinks = document.querySelectorAll('a[href^="#"]');

    anchorLinks.forEach(function(link) {
        link.addEventListener('click', function(event) {
            var href = this.getAttribute('href');

            // Skip if it's just "#" (top of page)
            if (href === '#') {
                event.preventDefault();
                window.scrollTo({
                    top: 0,
                    behavior: 'smooth'
                });
                if (history.pushState) {
                    history.pushState(null, null, window.location.pathname);
                }
                return;
            }

            // Extract the ID from the href
            var targetId = href.substring(1);
            var targetElement = document.getElementById(targetId);

            // Only handle if target element exists
            if (targetElement) {
                event.preventDefault();
                scrollToElement(targetId);
            }
        });
    });
}

// Export for module systems if available
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initSmoothScroll: initSmoothScroll,
        scrollToElement: scrollToElement
    };
}
