/**
 * Main Application JavaScript
 * Owner: First builder (application initialization)
 *
 * Includes:
 * - Application initialization
 * - Auto-refresh setup (5-second interval)
 * - Event listeners for navigation
 */

/**
 * Initialize the application when DOM is ready
 */
document.addEventListener('DOMContentLoaded', function() {
    initApp();
});

/**
 * Main application initialization
 */
function initApp() {
    // Initialize UI utilities
    if (window.UI && window.UI.initCopyButtons) {
        window.UI.initCopyButtons();
    }

    // Initialize navigation
    initNavigation();

    // Initialize smooth scrolling
    initSmoothScroll();

    // Initialize Key Browser (Scenario 4)
    if (typeof MirDBKeys !== 'undefined' && MirDBKeys.initKeyBrowser) {
        MirDBKeys.initKeyBrowser();
    }

    // Initialize auto-refresh
    initAutoRefresh();

    console.log('MirDB Homepage initialized');
}

/**
 * Initialize navigation highlighting based on scroll position
 */
function initNavigation() {
    var navLinks = document.querySelectorAll('.nav-link');
    var sections = document.querySelectorAll('section[id]');

    function updateActiveNav() {
        var scrollPosition = window.scrollY + 100;

        sections.forEach(function(section) {
            var sectionTop = section.offsetTop;
            var sectionHeight = section.offsetHeight;
            var sectionId = section.getAttribute('id');

            if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
                navLinks.forEach(function(link) {
                    link.classList.remove('active');
                    if (link.getAttribute('href') === '#' + sectionId) {
                        link.classList.add('active');
                    }
                });
            }
        });

        // If at top of page, highlight Home
        if (scrollPosition < 200) {
            navLinks.forEach(function(link) { link.classList.remove('active'); });
            var homeLink = document.querySelector('.nav-link[href="#home"]');
            if (homeLink) homeLink.classList.add('active');
        }
    }

    window.addEventListener('scroll', updateActiveNav);
    updateActiveNav();
}

/**
 * Initialize smooth scrolling for anchor links
 */
function initSmoothScroll() {
    document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
        anchor.addEventListener('click', function(e) {
            var href = this.getAttribute('href');
            if (href === '#' || href === '#home') {
                e.preventDefault();
                window.scrollTo({ top: 0, behavior: 'smooth' });
                return;
            }

            var target = document.querySelector(href);
            if (target) {
                e.preventDefault();
                target.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });
}

/**
 * Initialize auto-refresh for status dashboard (5-second interval)
 */
function initAutoRefresh() {
    // Status refresh will be initialized by status.js
    // This is a placeholder for the refresh coordinator
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { initApp: initApp, initNavigation: initNavigation, initSmoothScroll: initSmoothScroll };
}
