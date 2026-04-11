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

    console.log('MirDB Homepage initialized');
}

/**
 * Initialize navigation highlighting based on scroll position
 */
function initNavigation() {
    const navLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('section[id]');

    function updateActiveNav() {
        const scrollPosition = window.scrollY + 100;

        sections.forEach(section => {
            const sectionTop = section.offsetTop;
            const sectionHeight = section.offsetHeight;
            const sectionId = section.getAttribute('id');

            if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
                navLinks.forEach(link => {
                    link.classList.remove('active');
                    if (link.getAttribute('href') === `#${sectionId}`) {
                        link.classList.add('active');
                    }
                });
            }
        });

        // If at top of page, highlight Home
        if (scrollPosition < 200) {
            navLinks.forEach(link => link.classList.remove('active'));
            const homeLink = document.querySelector('.nav-link[href="#"]');
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
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            const href = this.getAttribute('href');
            if (href === '#') {
                e.preventDefault();
                window.scrollTo({ top: 0, behavior: 'smooth' });
                return;
            }

            const target = document.querySelector(href);
            if (target) {
                e.preventDefault();
                target.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { initApp, initNavigation, initSmoothScroll };
}
