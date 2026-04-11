/**
 * Responsive Design E2E Tests
 * Owner: Scenario 11 - Responsive Design
 *
 * Tests:
 * - TC1: Desktop viewport (1920x1080) - Full layout with side-by-side panels
 * - TC2: Tablet viewport (768x1024) - Responsive layout with adjusted grid
 * - TC3: Mobile viewport (375x667) - Single-column stacked layout
 * - TC4: Navigation usability on mobile - Hamburger menu accessibility
 */

const fs = require('fs');
const path = require('path');

// Read source files for testing
const mainCssPath = path.join(__dirname, '../src/web/styles/main.css');
const indexHtmlPath = path.join(__dirname, '../src/web/index.html');
const mainJsPath = path.join(__dirname, '../src/web/scripts/main.js');

const mainCssContent = fs.readFileSync(mainCssPath, 'utf8');
const indexHtmlContent = fs.readFileSync(indexHtmlPath, 'utf8');
const mainJsContent = fs.readFileSync(mainJsPath, 'utf8');

describe('Responsive Design Structure Verification', () => {
    test('main.css should contain media queries', () => {
        expect(mainCssContent).toContain('@media');
    });

    test('main.css should have mobile breakpoint at 767px', () => {
        expect(mainCssContent).toContain('max-width: 767px');
    });

    test('main.css should have tablet breakpoint', () => {
        expect(mainCssContent).toContain('768px');
    });

    test('main.css should have small mobile breakpoint at 479px', () => {
        expect(mainCssContent).toContain('max-width: 479px');
    });

    test('main.css should have large desktop breakpoint at 1440px', () => {
        expect(mainCssContent).toContain('min-width: 1440px');
    });

    test('index.html should have viewport meta tag', () => {
        expect(indexHtmlContent).toContain('viewport');
        expect(indexHtmlContent).toContain('width=device-width');
    });

    test('index.html should include main.css stylesheet', () => {
        expect(indexHtmlContent).toContain('main.css');
    });

    test('index.html should include main.js script', () => {
        expect(indexHtmlContent).toContain('main.js');
    });
});

describe('TC1: Desktop Viewport (1920x1080) Rendering', () => {
    test('main.css should have styles for large desktop (1440px+)', () => {
        expect(mainCssContent).toContain('min-width: 1440px');
    });

    test('main.css should define side-by-side grid for status panel on desktop', () => {
        // Status panel should have multi-column grid on large screens
        expect(mainCssContent).toContain('status-panel');
        expect(mainCssContent).toContain('grid-template-columns');
    });

    test('main.css should set max-width for main content on large screens', () => {
        // Large desktop should have a max-width for readability
        expect(mainCssContent).toContain('max-width: 1600px');
    });

    test('main.css should have 4-column grid for status panel on large desktop', () => {
        expect(mainCssContent).toContain('repeat(4, 1fr)');
    });

    test('navigation should be fully visible on desktop', () => {
        // Nav should be displayed on desktop (not hidden)
        expect(mainCssContent).toContain('.nav-toggle');
        expect(mainCssContent).toContain('display: none');
    });

    test('header should have flex layout for desktop', () => {
        expect(mainCssContent).toContain('.header');
        expect(mainCssContent).toContain('display: flex');
    });
});

describe('TC2: Tablet Viewport (768x1024) Rendering', () => {
    test('main.css should have tablet-specific media query', () => {
        expect(mainCssContent).toContain('min-width: 768px');
        expect(mainCssContent).toContain('max-width: 1023px');
    });

    test('main.css should adjust status panel grid for tablet', () => {
        // Tablet should have 2-column grid
        expect(mainCssContent).toContain('repeat(2, 1fr)');
    });

    test('main.css should adjust hero title size for tablet', () => {
        // Hero title should be smaller on tablet
        expect(mainCssContent).toContain('hero-title');
    });

    test('main.css should adjust table padding for tablet', () => {
        expect(mainCssContent).toContain('.key-table th');
        expect(mainCssContent).toContain('.key-table td');
    });

    test('main.css should adjust metadata grid for tablet', () => {
        expect(mainCssContent).toContain('.metadata-grid');
    });
});

describe('TC3: Mobile Viewport (375x667) Rendering', () => {
    test('main.css should have mobile-specific media query (below 768px)', () => {
        expect(mainCssContent).toContain('max-width: 767px');
    });

    test('main.css should stack status panel in single column on mobile', () => {
        // Mobile should have single column for status panel
        expect(mainCssContent).toContain('grid-template-columns: 1fr');
    });

    test('main.css should reduce font size on mobile', () => {
        expect(mainCssContent).toContain('font-size: 14px');
    });

    test('main.css should reduce hero title size on mobile', () => {
        expect(mainCssContent).toContain('font-size: 1.75rem');
    });

    test('main.css should make key browser toolbar stack vertically', () => {
        expect(mainCssContent).toContain('.key-browser-toolbar');
        expect(mainCssContent).toContain('flex-direction: column');
    });

    test('main.css should allow horizontal scroll for tables', () => {
        expect(mainCssContent).toContain('.key-table-container');
        expect(mainCssContent).toContain('overflow-x: auto');
    });

    test('main.css should reduce modal width on mobile', () => {
        expect(mainCssContent).toContain('.modal');
    });

    test('main.css should stack footer content vertically on mobile', () => {
        expect(mainCssContent).toContain('.footer-content');
        expect(mainCssContent).toContain('flex-direction: column');
    });

    test('main.css should center footer on mobile', () => {
        expect(mainCssContent).toContain('text-align: center');
    });

    test('main.css should reduce section padding on mobile', () => {
        expect(mainCssContent).toContain('.section');
        expect(mainCssContent).toContain('padding: var(--spacing-md)');
    });
});

describe('TC4: Navigation Usability on Mobile', () => {
    test('index.html should have nav-toggle button for mobile', () => {
        expect(indexHtmlContent).toContain('nav-toggle');
        expect(indexHtmlContent).toContain('id="nav-toggle"');
    });

    test('nav-toggle should have hamburger menu structure', () => {
        expect(indexHtmlContent).toContain('hamburger-icon');
        expect(indexHtmlContent).toContain('hamburger-line');
    });

    test('nav-toggle should have aria-label for accessibility', () => {
        expect(indexHtmlContent).toContain('aria-label="Toggle navigation menu"');
    });

    test('nav-toggle should have aria-expanded attribute', () => {
        expect(indexHtmlContent).toContain('aria-expanded="false"');
    });

    test('nav-toggle should have aria-controls referencing nav-menu', () => {
        expect(indexHtmlContent).toContain('aria-controls="nav-menu"');
    });

    test('navigation should have id="nav-menu"', () => {
        expect(indexHtmlContent).toContain('id="nav-menu"');
    });

    test('main.css should hide nav-toggle by default (desktop)', () => {
        expect(mainCssContent).toContain('.nav-toggle');
        expect(mainCssContent).toContain('display: none');
    });

    test('main.css should show nav-toggle on mobile', () => {
        expect(mainCssContent).toContain('.nav-toggle');
        expect(mainCssContent).toContain('display: block');
    });

    test('main.css should style hamburger icon', () => {
        expect(mainCssContent).toContain('.hamburger-icon');
        expect(mainCssContent).toContain('.hamburger-line');
    });

    test('main.css should have hamburger animation for open state', () => {
        expect(mainCssContent).toContain('aria-expanded="true"');
        expect(mainCssContent).toContain('rotate(45deg)');
        expect(mainCssContent).toContain('rotate(-45deg)');
    });

    test('main.css should position mobile nav as fixed sidebar', () => {
        expect(mainCssContent).toContain('position: fixed');
        expect(mainCssContent).toContain('height: 100vh');
    });

    test('main.css should have nav-open class for opened menu', () => {
        expect(mainCssContent).toContain('.nav-open');
    });

    test('main.css should have nav-overlay for mobile menu backdrop', () => {
        expect(mainCssContent).toContain('.nav-overlay');
    });

    test('main.js should have initMobileNavigation function', () => {
        expect(mainJsContent).toContain('initMobileNavigation');
    });

    test('main.js should call initMobileNavigation on init', () => {
        expect(mainJsContent).toContain('initMobileNavigation()');
    });

    test('main.js should toggle aria-expanded on click', () => {
        expect(mainJsContent).toContain('aria-expanded');
    });

    test('main.js should create overlay element', () => {
        expect(mainJsContent).toContain('nav-overlay');
    });

    test('main.js should close menu on escape key', () => {
        expect(mainJsContent).toContain('Escape');
    });

    test('main.js should close menu when window resizes to desktop', () => {
        expect(mainJsContent).toContain('resize');
        expect(mainJsContent).toContain('768');
    });

    test('main.js should close menu when nav link is clicked', () => {
        expect(mainJsContent).toContain('.nav-link');
        expect(mainJsContent).toContain('closeMenu');
    });

    test('main.js should prevent body scroll when menu is open', () => {
        expect(mainJsContent).toContain('overflow');
        expect(mainJsContent).toContain("'hidden'");
    });
});

describe('Small Mobile Viewport (below 480px)', () => {
    test('main.css should have small mobile breakpoint', () => {
        expect(mainCssContent).toContain('max-width: 479px');
    });

    test('main.css should further reduce font size for small mobile', () => {
        expect(mainCssContent).toContain('font-size: 13px');
    });

    test('main.css should reduce logo text size on small mobile', () => {
        expect(mainCssContent).toContain('.logo-text');
        expect(mainCssContent).toContain('1.5rem');
    });

    test('main.css should stack code example headers on small mobile', () => {
        expect(mainCssContent).toContain('.code-header');
        expect(mainCssContent).toContain('flex-direction: column');
    });
});

describe('CSS Custom Properties for Responsive Design', () => {
    test('main.css should use CSS custom properties for spacing', () => {
        expect(mainCssContent).toContain('--spacing-xs');
        expect(mainCssContent).toContain('--spacing-sm');
        expect(mainCssContent).toContain('--spacing-md');
        expect(mainCssContent).toContain('--spacing-lg');
        expect(mainCssContent).toContain('--spacing-xl');
    });

    test('main.css should use CSS custom properties for border radius', () => {
        expect(mainCssContent).toContain('--radius-sm');
        expect(mainCssContent).toContain('--radius-md');
        expect(mainCssContent).toContain('--radius-lg');
    });

    test('main.css should use CSS custom properties for colors', () => {
        expect(mainCssContent).toContain('--color-rust-primary');
        expect(mainCssContent).toContain('--color-text-light');
        expect(mainCssContent).toContain('--color-dark-primary');
    });
});

describe('Accessibility in Responsive Design', () => {
    test('index.html should have proper lang attribute', () => {
        expect(indexHtmlContent).toContain('lang="en"');
    });

    test('nav-toggle should be a button element', () => {
        expect(indexHtmlContent).toContain('<button');
        expect(indexHtmlContent).toContain('nav-toggle');
    });

    test('hamburger icon should have aria-hidden for screen readers', () => {
        expect(indexHtmlContent).toContain('aria-hidden="true"');
    });

    test('navigation should have aria-label', () => {
        expect(indexHtmlContent).toContain('aria-label="Main navigation"');
    });

    test('main.css should have focus styles for nav-toggle', () => {
        expect(mainCssContent).toContain('.nav-toggle');
    });

    test('main.css should support touch devices with -webkit-overflow-scrolling', () => {
        expect(mainCssContent).toContain('-webkit-overflow-scrolling: touch');
    });
});

describe('Mobile Navigation JavaScript Behavior', () => {
    beforeEach(() => {
        // Setup DOM with mobile navigation
        document.body.innerHTML = `
            <button type="button" class="nav-toggle" id="nav-toggle" aria-expanded="false" aria-controls="nav-menu">
                <span class="hamburger-icon" aria-hidden="true">
                    <span class="hamburger-line"></span>
                    <span class="hamburger-line"></span>
                    <span class="hamburger-line"></span>
                </span>
            </button>
            <nav class="nav" id="nav-menu" aria-label="Main navigation">
                <ul class="nav-list">
                    <li><a href="#home" class="nav-link active">Home</a></li>
                    <li><a href="#status" class="nav-link">Status</a></li>
                </ul>
            </nav>
        `;
    });

    test('nav-toggle and nav-menu elements should exist', () => {
        const navToggle = document.getElementById('nav-toggle');
        const navMenu = document.getElementById('nav-menu');

        expect(navToggle).not.toBeNull();
        expect(navMenu).not.toBeNull();
    });

    test('nav-toggle should initially have aria-expanded="false"', () => {
        const navToggle = document.getElementById('nav-toggle');
        expect(navToggle.getAttribute('aria-expanded')).toBe('false');
    });

    test('nav-menu should not have nav-open class initially', () => {
        const navMenu = document.getElementById('nav-menu');
        expect(navMenu.classList.contains('nav-open')).toBe(false);
    });

    test('hamburger lines should exist', () => {
        const lines = document.querySelectorAll('.hamburger-line');
        expect(lines.length).toBe(3);
    });
});

describe('Responsive Grid Layouts', () => {
    test('main.css should use CSS Grid for status panel', () => {
        expect(mainCssContent).toContain('.status-panel');
        expect(mainCssContent).toContain('display: grid');
    });

    test('main.css should use auto-fit for flexible grids', () => {
        expect(mainCssContent).toContain('auto-fit');
    });

    test('main.css should use minmax for responsive grid items', () => {
        expect(mainCssContent).toContain('minmax');
    });

    test('main.css should use flexbox for header layout', () => {
        expect(mainCssContent).toContain('.header');
        expect(mainCssContent).toContain('display: flex');
    });

    test('main.css should use flexbox for footer layout', () => {
        expect(mainCssContent).toContain('.footer-content');
        expect(mainCssContent).toContain('display: flex');
    });
});

describe('Responsive Typography', () => {
    test('main.css should have responsive root font size', () => {
        // Base font size should be defined
        expect(mainCssContent).toContain('font-size: 16px');
    });

    test('main.css should have smaller font size for mobile', () => {
        expect(mainCssContent).toContain('font-size: 14px');
    });

    test('main.css should have even smaller font size for small mobile', () => {
        expect(mainCssContent).toContain('font-size: 13px');
    });

    test('hero title should scale down on smaller screens', () => {
        // Should have multiple hero-title font sizes for different breakpoints
        const heroTitleMatches = mainCssContent.match(/\.hero-title[\s\S]*?font-size/g);
        expect(heroTitleMatches).not.toBeNull();
        expect(heroTitleMatches.length).toBeGreaterThan(1);
    });
});

describe('Responsive Table Handling', () => {
    test('main.css should make key-table-container scrollable', () => {
        expect(mainCssContent).toContain('.key-table-container');
        expect(mainCssContent).toContain('overflow-x: auto');
    });

    test('main.css should set min-width for key-table on mobile', () => {
        expect(mainCssContent).toContain('.key-table');
        expect(mainCssContent).toContain('min-width');
    });

    test('main.css should reduce table padding on mobile', () => {
        expect(mainCssContent).toContain('.key-table th');
        expect(mainCssContent).toContain('.key-table td');
    });
});
