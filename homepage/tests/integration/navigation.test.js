/**
 * Integration Tests for Navigation
 * Owner: Scenario 1 - Hero Section & Navigation
 *
 * Tests the integration between smooth scrolling and navigation elements.
 */

// Import the smooth-scroll module
const smoothScroll = require('../../js/smooth-scroll');
const { initSmoothScroll, scrollToElement } = smoothScroll;

describe('Navigation Integration', () => {
    beforeEach(() => {
        // Create DOM structure matching index.html
        document.body.innerHTML = `
            <a href="#main-content" class="skip-link">Skip to main content</a>
            <header class="site-header">
                <nav class="main-nav" aria-label="Main navigation">
                    <div class="nav-container">
                        <a href="#" class="nav-logo">MirDB</a>
                        <ul class="nav-links">
                            <li><a href="#features">Features</a></li>
                            <li><a href="#quick-start">Quick Start</a></li>
                            <li><a href="#architecture">Architecture</a></li>
                            <li><a href="#benchmarks">Benchmarks</a></li>
                            <li><a href="https://github.com/mirdb/mirdb" target="_blank" rel="noopener">GitHub</a></li>
                        </ul>
                    </div>
                </nav>
            </header>
            <main id="main-content">
                <section id="hero" class="hero">
                    <div class="hero-content">
                        <h1>MirDB</h1>
                        <p class="hero-tagline">Persistent Memcached-Compatible Key-Value Store</p>
                        <div class="hero-cta">
                            <a href="https://github.com/mirdb/mirdb" class="btn btn-primary" target="_blank" rel="noopener">Get Started on GitHub</a>
                            <a href="#quick-start" class="btn btn-secondary">View Quick Start</a>
                        </div>
                    </div>
                </section>
                <section id="features">Features Section</section>
                <section id="quick-start">Quick Start Section</section>
                <section id="architecture">Architecture Section</section>
                <section id="benchmarks">Benchmarks Section</section>
            </main>
        `;

        // Mock getBoundingClientRect
        Element.prototype.getBoundingClientRect = jest.fn(() => ({
            top: 500,
            left: 0,
            right: 0,
            bottom: 0,
            width: 0,
            height: 0
        }));

        // Mock scrollTo
        window.scrollTo = jest.fn();

        // Mock history.pushState
        window.history.pushState = jest.fn();

        Object.defineProperty(window, 'pageYOffset', {
            value: 0,
            writable: true
        });
    });

    describe('Smooth scroll initialization on page with anchor links', () => {
        test('all anchor links have click handlers attached for smooth scrolling', () => {
            initSmoothScroll();

            const anchorLinks = document.querySelectorAll('a[href^="#"]');
            expect(anchorLinks.length).toBeGreaterThan(0);

            // Each link should trigger smooth scroll on click
            anchorLinks.forEach(link => {
                const href = link.getAttribute('href');
                if (href && href.length > 1) {
                    window.scrollTo.mockClear();
                    link.click();

                    const targetId = href.substring(1);
                    if (document.getElementById(targetId)) {
                        expect(window.scrollTo).toHaveBeenCalled();
                    }
                }
            });
        });

        test('navigation links integrate with section IDs', () => {
            initSmoothScroll();

            // Verify all navigation links have corresponding sections
            const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

            navLinks.forEach(link => {
                const href = link.getAttribute('href');
                const targetId = href.substring(1);
                const targetSection = document.getElementById(targetId);

                expect(targetSection).not.toBeNull();
            });
        });

        test('CTA button in hero section triggers smooth scroll to quick-start', () => {
            initSmoothScroll();

            const quickStartButton = document.querySelector('a[href="#quick-start"].btn');
            expect(quickStartButton).not.toBeNull();

            quickStartButton.click();

            expect(window.scrollTo).toHaveBeenCalledWith(
                expect.objectContaining({
                    behavior: 'smooth'
                })
            );
            expect(window.history.pushState).toHaveBeenCalledWith(
                null,
                null,
                '#quick-start'
            );
        });

        test('multiple sections can be navigated sequentially', () => {
            initSmoothScroll();

            const sections = ['features', 'quick-start', 'architecture', 'benchmarks'];

            sections.forEach(sectionId => {
                window.scrollTo.mockClear();
                scrollToElement(sectionId);

                expect(window.scrollTo).toHaveBeenCalled();
                expect(window.history.pushState).toHaveBeenLastCalledWith(
                    null,
                    null,
                    `#${sectionId}`
                );
            });
        });

        test('header height is accounted for in scroll position', () => {
            initSmoothScroll();

            const nav = document.querySelector('.main-nav');
            Object.defineProperty(nav, 'offsetHeight', { value: 60 });

            // Mock element position
            const featuresSection = document.getElementById('features');
            featuresSection.getBoundingClientRect = jest.fn(() => ({
                top: 1000,
                left: 0,
                right: 0,
                bottom: 0,
                width: 0,
                height: 0
            }));

            scrollToElement('features');

            // The scroll position should be offset by header height (60px)
            expect(window.scrollTo).toHaveBeenCalledWith({
                top: 1000 - 60, // Element top - header height
                behavior: 'smooth'
            });
        });
    });

    describe('Navigation accessibility', () => {
        test('all navigation links are keyboard accessible', () => {
            initSmoothScroll();

            const links = document.querySelectorAll('.nav-links a');
            links.forEach(link => {
                // Links should be focusable by default (tabIndex 0 or not set)
                expect(link.tabIndex).toBeLessThanOrEqual(0);
            });
        });

        test('skip link functionality', () => {
            initSmoothScroll();

            const skipLink = document.querySelector('.skip-link');
            expect(skipLink).not.toBeNull();
            expect(skipLink.getAttribute('href')).toBe('#main-content');

            skipLink.click();

            expect(window.scrollTo).toHaveBeenCalled();
        });
    });
});
