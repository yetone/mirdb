/**
 * Shared Test Utilities
 * Owner: First builder scenario
 *
 * Contains common selectors, helper functions, viewport presets,
 * and accessibility testing utilities
 */

// Common selectors for MirDB homepage
const selectors = {
    // Hero section
    hero: '#hero',
    heroTitle: '.hero-title',
    heroLogo: '.hero-logo',
    heroTagline: '.hero-tagline',
    heroCta: '.hero-cta',
    getStartedBtn: '.hero-cta .btn-primary',
    githubBtn: '.hero-cta .btn-secondary',

    // Navigation
    header: '.header',
    nav: '.nav',
    navLink: '.nav-link',

    // Features section
    features: '#features',
    featuresGrid: '.features-grid',
    featureCard: '.feature-card',

    // Quick Start section
    quickstart: '#quickstart',
    codeBlock: '.code-block',

    // Footer
    footer: '.footer',
    footerContent: '.footer-content',

    // Badges
    badge: '.badge',
    circleciBadge: '.badge-circleci',
};

// Viewport presets
const viewports = {
    desktop: { width: 1920, height: 1080 },
    laptop: { width: 1366, height: 768 },
    tablet: { width: 768, height: 1024 },
    mobile: { width: 375, height: 667 },
};

// GitHub repository URL
const GITHUB_URL = 'https://github.com/yetone/mirdb';

/**
 * Navigate to homepage and wait for it to load
 * @param {import('@playwright/test').Page} page
 */
async function gotoHomepage(page) {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
}

/**
 * Check if element is visible
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 */
async function isVisible(page, selector) {
    const element = page.locator(selector);
    return element.isVisible();
}

/**
 * Get text content of an element
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 */
async function getText(page, selector) {
    const element = page.locator(selector);
    return element.textContent();
}

/**
 * Set viewport size
 * @param {import('@playwright/test').Page} page
 * @param {'desktop' | 'laptop' | 'tablet' | 'mobile'} preset
 */
async function setViewport(page, preset) {
    const viewport = viewports[preset] || viewports.desktop;
    await page.setViewportSize(viewport);
}

module.exports = {
    selectors,
    viewports,
    GITHUB_URL,
    gotoHomepage,
    isVisible,
    getText,
    setViewport,
};
