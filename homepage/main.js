/**
 * MirDB Homepage - Main JavaScript
 * Handles smooth scrolling and any interactive elements
 */

(function() {
    'use strict';

    /**
     * Initialize smooth scrolling for anchor links
     */
    function initSmoothScroll() {
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', function(e) {
                e.preventDefault();
                const targetId = this.getAttribute('href');
                const targetElement = document.querySelector(targetId);

                if (targetElement) {
                    targetElement.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            });
        });
    }

    /**
     * Initialize hero section functionality
     */
    function initHeroSection() {
        const heroSection = document.getElementById('hero');
        const heroTitle = document.querySelector('[data-testid="hero-title"]');
        const heroTagline = document.querySelector('[data-testid="hero-tagline"]');
        const getStartedBtn = document.querySelector('[data-testid="get-started-btn"]');
        const githubBtn = document.querySelector('[data-testid="github-btn"]');

        // Validate hero section elements exist
        if (!heroSection || !heroTitle || !heroTagline) {
            console.error('Hero section: Required elements not found');
            return false;
        }

        // Add keyboard accessibility for buttons
        [getStartedBtn, githubBtn].forEach(btn => {
            if (btn) {
                btn.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        this.click();
                    }
                });
            }
        });

        return true;
    }

    /**
     * Check for any JavaScript errors during initialization
     * @returns {boolean} - True if no errors, false otherwise
     */
    function validateHeroRender() {
        try {
            const hero = document.querySelector('[data-testid="hero-section"]');
            const title = document.querySelector('[data-testid="hero-title"]');
            const tagline = document.querySelector('[data-testid="hero-tagline"]');
            const description = document.querySelector('[data-testid="hero-description"]');
            const ctaButtons = document.querySelector('[data-testid="hero-cta-buttons"]');
            const getStartedBtn = document.querySelector('[data-testid="get-started-btn"]');
            const githubBtn = document.querySelector('[data-testid="github-btn"]');

            // Verify all required elements exist
            const elements = {
                'hero-section': hero,
                'hero-title': title,
                'hero-tagline': tagline,
                'hero-description': description,
                'hero-cta-buttons': ctaButtons,
                'get-started-btn': getStartedBtn,
                'github-btn': githubBtn
            };

            for (const [name, element] of Object.entries(elements)) {
                if (!element) {
                    console.error(`Hero section validation failed: ${name} not found`);
                    return false;
                }
            }

            // Verify content is present
            if (!title.textContent.includes('MirDB')) {
                console.error('Hero section validation failed: Title does not contain MirDB');
                return false;
            }

            console.log('Hero section rendered successfully');
            return true;
        } catch (error) {
            console.error('Hero section validation error:', error);
            return false;
        }
    }

    /**
     * Main initialization function
     */
    function init() {
        initSmoothScroll();
        initHeroSection();
        validateHeroRender();
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Expose validation function for testing
    window.MirDBHomepage = {
        validateHeroRender: validateHeroRender
    };
})();
