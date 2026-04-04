/**
 * Shared Test Utilities
 * Common selectors, helpers, and viewport presets for E2E tests
 */

// Common selectors for MirDB homepage
const SELECTORS = {
    // Navigation
    header: '.header',
    nav: '.nav',
    navLogo: '.nav-logo',
    navLinks: '.nav-links',
    navLink: '.nav-links a',

    // Hero Section
    hero: '#hero',
    heroLogo: '.hero-logo',
    heroTitle: '.hero-title',
    heroTagline: '.hero-tagline',
    heroCtaPrimary: '.hero-cta .btn-primary',
    heroCtaSecondary: '.hero-cta .btn-secondary',
    heroCta: '.hero-cta',
    getStartedBtn: '.hero-cta .btn-primary',
    githubBtn: '.hero-cta .btn-secondary',

    // Badges
    badges: '.badges',
    badge: '.badge',

    // Features Section
    features: '#features',
    featuresHeading: '#features-heading',
    featuresGrid: '.features-grid',
    featureCard: '.feature-card',
    featureIcon: '.feature-icon',
    featureTitle: '.feature-title',
    featureDescription: '.feature-description',

    // Quick Start Section
    quickstart: '#quickstart',
    quickstartHeading: '#quickstart-heading',
    codeBlock: '.code-block',

    // Footer
    footer: '.footer',
    footerLicense: '.footer-license',
    footerCopyright: '.footer-copyright',
    footerLink: '.footer-link',
    footerContent: '.footer-content',
};

// Viewport presets
const VIEWPORTS = {
    desktop: { width: 1920, height: 1080 },
    laptop: { width: 1366, height: 768 },
    tablet: { width: 768, height: 1024 },
    mobile: { width: 375, height: 667 },
    mobileSmall: { width: 320, height: 568 },
};

// Feature titles expected on the page
const FEATURE_TITLES = [
    'Memcached Protocol Compatible',
    'Persistent Storage',
    'LSM Tree Architecture',
    'Async/Tokio Networking',
];

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
 * Navigate to a section by clicking its nav link
 */
async function navigateToSection(page, sectionId) {
    await page.click(`a[href="#${sectionId}"]`);
    await page.waitForSelector(`#${sectionId}`, { state: 'visible' });
}

/**
 * Check if an element is visible in the viewport
 */
async function isVisibleInViewport(page, selector) {
    const element = await page.$(selector);
    if (!element) return false;

    const boundingBox = await element.boundingBox();
    const viewportSize = page.viewportSize();

    if (!boundingBox || !viewportSize) return false;

    return (
        boundingBox.y >= 0 &&
        boundingBox.y + boundingBox.height <= viewportSize.height
    );
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
    const viewport = VIEWPORTS[preset] || VIEWPORTS.desktop;
    await page.setViewportSize(viewport);
}

module.exports = {
    SELECTORS,
    VIEWPORTS,
    FEATURE_TITLES,
    GITHUB_URL,
    gotoHomepage,
    navigateToSection,
    isVisibleInViewport,
    isVisible,
    getText,
    setViewport,
    // Aliases for backward compatibility
    selectors: SELECTORS,
    viewports: VIEWPORTS,
};
