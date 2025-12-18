/**
 * MirDB Homepage Main JavaScript
 * Handles mobile navigation toggle and other interactive features
 */

(function() {
    'use strict';

    // Mobile navigation toggle
    function initMobileNav() {
        var navToggle = document.querySelector('.nav-toggle');
        var navMenu = document.querySelector('.nav-menu');

        if (!navToggle || !navMenu) {
            return;
        }

        navToggle.addEventListener('click', function() {
            var isExpanded = navToggle.getAttribute('aria-expanded') === 'true';
            navToggle.setAttribute('aria-expanded', !isExpanded);
            navMenu.classList.toggle('active');
        });

        // Close menu when clicking outside
        document.addEventListener('click', function(event) {
            if (!navToggle.contains(event.target) && !navMenu.contains(event.target)) {
                navMenu.classList.remove('active');
                navToggle.setAttribute('aria-expanded', 'false');
            }
        });

        // Close menu when clicking on a link
        navMenu.querySelectorAll('a').forEach(function(link) {
            link.addEventListener('click', function() {
                navMenu.classList.remove('active');
                navToggle.setAttribute('aria-expanded', 'false');
            });
        });

        // Handle escape key
        document.addEventListener('keydown', function(event) {
            if (event.key === 'Escape' && navMenu.classList.contains('active')) {
                navMenu.classList.remove('active');
                navToggle.setAttribute('aria-expanded', 'false');
                navToggle.focus();
            }
        });
    }

    // Smooth scroll for anchor links (with polyfill for older browsers)
    function initSmoothScroll() {
        document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
            anchor.addEventListener('click', function(event) {
                var targetId = this.getAttribute('href');
                if (targetId === '#') {
                    return;
                }

                var target = document.querySelector(targetId);
                if (target) {
                    event.preventDefault();
                    target.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });

                    // Update URL without triggering scroll
                    if (history.pushState) {
                        history.pushState(null, null, targetId);
                    }
                }
            });
        });
    }

    // Initialize all features when DOM is ready
    function init() {
        initMobileNav();
        initSmoothScroll();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
