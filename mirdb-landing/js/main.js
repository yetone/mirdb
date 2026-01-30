/**
 * Main JavaScript Entry Point
 * Owner: Scenario 2 - Navigation and Smooth Scrolling
 *
 * Imports and initializes:
 * - Smooth scroll functionality
 * - Navigation toggle for mobile
 * - Tab interactions (Scenario 4)
 * - Copy to clipboard (Scenario 4)
 * - Scroll animations (Scenario 3)
 *
 * Progressive enhancement: Page works without JS
 */

(function() {
    'use strict';

    /**
     * Initialize all modules when DOM is ready
     */
    function init() {
        initNavigation();
        initScrollTracking();

        // Initialize smooth scroll if not already done
        if (typeof window.initSmoothScroll === 'function') {
            window.initSmoothScroll();
        }

        // Initialize other modules if they exist
        if (typeof window.initTabs === 'function') {
            window.initTabs();
        }

        if (typeof window.initCopyButtons === 'function') {
            window.initCopyButtons();
        }

        if (typeof window.initScrollAnimations === 'function') {
            window.initScrollAnimations();
        }

        console.log('MirDB Landing Page initialized');
    }

    /**
     * Initialize mobile navigation toggle
     */
    function initNavigation() {
        const toggle = document.querySelector('.nav__toggle');
        const links = document.querySelector('.nav__links');

        if (!toggle || !links) {
            return;
        }

        toggle.addEventListener('click', function() {
            const isExpanded = toggle.getAttribute('aria-expanded') === 'true';
            toggle.setAttribute('aria-expanded', String(!isExpanded));
            links.classList.toggle('nav__links--open');
        });

        // Close menu when clicking outside
        document.addEventListener('click', function(event) {
            if (!event.target.closest('.nav')) {
                toggle.setAttribute('aria-expanded', 'false');
                links.classList.remove('nav__links--open');
            }
        });

        // Close menu on escape key
        document.addEventListener('keydown', function(event) {
            if (event.key === 'Escape') {
                toggle.setAttribute('aria-expanded', 'false');
                links.classList.remove('nav__links--open');
            }
        });

        // Handle keyboard navigation in menu
        links.addEventListener('keydown', function(event) {
            const focusableLinks = links.querySelectorAll('.nav__link');
            const currentIndex = Array.from(focusableLinks).indexOf(document.activeElement);

            if (event.key === 'ArrowDown' || event.key === 'ArrowRight') {
                event.preventDefault();
                const nextIndex = (currentIndex + 1) % focusableLinks.length;
                focusableLinks[nextIndex].focus();
            }

            if (event.key === 'ArrowUp' || event.key === 'ArrowLeft') {
                event.preventDefault();
                const prevIndex = (currentIndex - 1 + focusableLinks.length) % focusableLinks.length;
                focusableLinks[prevIndex].focus();
            }
        });
    }

    /**
     * Track scroll position to add visual feedback to navigation
     */
    function initScrollTracking() {
        const nav = document.querySelector('.nav');
        const sections = document.querySelectorAll('section[id]');
        const navLinks = document.querySelectorAll('.nav__link[href^="#"]');

        if (!nav || sections.length === 0) {
            return;
        }

        // Add scrolled class for shadow effect
        function handleScroll() {
            if (window.scrollY > 50) {
                nav.classList.add('nav--scrolled');
            } else {
                nav.classList.remove('nav--scrolled');
            }

            // Update active link based on scroll position
            updateActiveLink();
        }

        function updateActiveLink() {
            let currentSection = '';
            const navHeight = nav.offsetHeight;
            const scrollPos = window.scrollY + navHeight + 100;

            sections.forEach(function(section) {
                const sectionTop = section.offsetTop;
                const sectionHeight = section.offsetHeight;

                if (scrollPos >= sectionTop && scrollPos < sectionTop + sectionHeight) {
                    currentSection = section.getAttribute('id');
                }
            });

            navLinks.forEach(function(link) {
                link.classList.remove('nav__link--active');
                if (link.getAttribute('href') === '#' + currentSection) {
                    link.classList.add('nav__link--active');
                }
            });
        }

        // Use passive event listener for better scroll performance
        window.addEventListener('scroll', handleScroll, { passive: true });

        // Initial check
        handleScroll();
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
