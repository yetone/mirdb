/**
 * Unit Tests for Smooth Scroll Module
 * Owner: Scenario 1 - Hero Section & Navigation
 */

// Import the smooth-scroll module
const smoothScroll = require('../../js/smooth-scroll');
const { initSmoothScroll, scrollToElement } = smoothScroll;

describe('Smooth Scroll Module', () => {
    beforeEach(() => {
        // Create basic DOM structure
        document.body.innerHTML = `
            <header class="site-header">
                <nav class="main-nav" style="height: 60px;">
                    <a href="#" class="nav-logo">MirDB</a>
                    <ul class="nav-links">
                        <li><a href="#features">Features</a></li>
                        <li><a href="#quick-start">Quick Start</a></li>
                        <li><a href="#architecture">Architecture</a></li>
                        <li><a href="#benchmarks">Benchmarks</a></li>
                    </ul>
                </nav>
            </header>
            <main id="main-content">
                <section id="hero">
                    <h1>MirDB</h1>
                    <a href="#quick-start" class="btn btn-secondary">View Quick Start</a>
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

        // Reset pageYOffset
        Object.defineProperty(window, 'pageYOffset', {
            value: 0,
            writable: true
        });
    });

    describe('scrollToElement', () => {
        test('scrolls to element with given id', () => {
            scrollToElement('features');

            expect(window.scrollTo).toHaveBeenCalled();
            expect(window.scrollTo).toHaveBeenCalledWith(
                expect.objectContaining({
                    behavior: 'smooth'
                })
            );
        });

        test('updates URL hash via history.pushState', () => {
            scrollToElement('features');

            expect(window.history.pushState).toHaveBeenCalledWith(
                null,
                null,
                '#features'
            );
        });

        test('does nothing when element does not exist', () => {
            scrollToElement('nonexistent');

            expect(window.scrollTo).not.toHaveBeenCalled();
        });

        test('calculates offset based on header height', () => {
            // Set nav height
            const nav = document.querySelector('.main-nav');
            Object.defineProperty(nav, 'offsetHeight', { value: 60 });

            scrollToElement('features');

            // Should have been called with position adjusted for header
            expect(window.scrollTo).toHaveBeenCalledWith(
                expect.objectContaining({
                    top: expect.any(Number),
                    behavior: 'smooth'
                })
            );
        });

        test('handles missing nav gracefully', () => {
            document.querySelector('.main-nav').remove();

            scrollToElement('features');

            expect(window.scrollTo).toHaveBeenCalled();
        });
    });

    describe('initSmoothScroll', () => {
        test('attaches click handlers to all anchor links', () => {
            initSmoothScroll();

            // Simulate click on features link
            const featuresLink = document.querySelector('a[href="#features"]');
            featuresLink.click();

            expect(window.scrollTo).toHaveBeenCalled();
        });

        test('prevents default behavior on anchor link click', () => {
            initSmoothScroll();

            const featuresLink = document.querySelector('a[href="#features"]');
            const event = new MouseEvent('click', {
                bubbles: true,
                cancelable: true
            });

            featuresLink.dispatchEvent(event);

            // Default was prevented (page doesn't jump to hash)
            expect(event.defaultPrevented).toBe(true);
        });

        test('scrolls to top when clicking link with href="#"', () => {
            initSmoothScroll();

            const logoLink = document.querySelector('a[href="#"]');
            logoLink.click();

            expect(window.scrollTo).toHaveBeenCalledWith({
                top: 0,
                behavior: 'smooth'
            });
        });

        test('updates URL when clicking link with href="#"', () => {
            initSmoothScroll();

            const logoLink = document.querySelector('a[href="#"]');
            logoLink.click();

            expect(window.history.pushState).toHaveBeenCalledWith(
                null,
                null,
                window.location.pathname
            );
        });

        test('handles links that point to nonexistent elements', () => {
            // Add a link that points to a nonexistent element
            const brokenLink = document.createElement('a');
            brokenLink.href = '#nonexistent';
            document.body.appendChild(brokenLink);

            initSmoothScroll();
            brokenLink.click();

            // Should not throw an error
            expect(window.scrollTo).not.toHaveBeenCalled();
        });
    });
});
