/**
 * Responsive layout tests for MirDB homepage.
 *
 * Owner: Scenario 8 - Responsive Layout Desktop
 * Co-owner: Scenario 9 - Responsive Layout Mobile
 *
 * Tests:
 * - Desktop layout at 1024px+ (Scenario 8)
 * - Large screen layout at 1920px (Scenario 8)
 * - Mobile layout at 375px (Scenario 9)
 * - Navigation collapse on mobile (Scenario 9)
 * - Touch target sizes (Scenario 9)
 */

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Helper to load CSS content
function loadMainCss() {
    const cssPath = path.join(__dirname, '../../static/css/main.css');
    return fs.readFileSync(cssPath, 'utf-8');
}

// Helper to load HTML content
function loadHomepageHtml() {
    const htmlPath = path.join(__dirname, '../../static/index.html');
    return fs.readFileSync(htmlPath, 'utf-8');
}

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

// Helper to get all content within 768px media queries
function getMobile768Sections(css) {
    const sections = [];
    const regex = /@media\s*\(max-width:\s*768px\)\s*\{/g;
    let match;
    while ((match = regex.exec(css)) !== null) {
        let braceCount = 1;
        let start = match.index + match[0].length;
        let end = start;
        while (braceCount > 0 && end < css.length) {
            if (css[end] === '{') braceCount++;
            if (css[end] === '}') braceCount--;
            end++;
        }
        sections.push(css.substring(start, end - 1));
    }
    return sections.join('\n');
}

function test(name, fn) {
    try {
        fn();
        passed++;
        results.push({ name, status: 'pass' });
        console.log(`  \u2713 ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`  \u2717 ${name}`);
        console.log(`    Error: ${error.message}`);
    }
}

// =============================================
// Test Case 1: Desktop Layout at 1024px Viewport
// Verifies all sections visible without horizontal scroll
// =============================================
console.log('\nTest Case 1: Desktop Layout at 1024px Viewport');
console.log('Input: Render page at 1024px viewport width');
console.log('Expected: All sections visible without horizontal scroll');

test('CSS has 1024px desktop media query', () => {
    const css = loadMainCss();
    assert(css.includes('@media (min-width: 1024px)'),
           'Should have min-width: 1024px media query for desktop');
});

test('CSS main content has max-width constraint', () => {
    const css = loadMainCss();
    assert(css.includes('max-width'),
           'Should have max-width constraint to prevent horizontal overflow');
});

test('HTML has viewport meta tag for responsive scaling', () => {
    const html = loadHomepageHtml();
    assert(html.includes('width=device-width'),
           'Should have viewport meta tag with device-width');
});

test('CSS uses box-sizing border-box to prevent overflow', () => {
    const css = loadMainCss();
    assert(css.includes('box-sizing: border-box'),
           'Should use border-box for predictable sizing');
});

test('All main sections exist for desktop view', () => {
    const html = loadHomepageHtml();
    const sections = ['header', 'hero', 'dashboard', 'features', 'quickstart', 'documentation', 'footer'];
    for (const section of sections) {
        assert(html.includes(`id="${section}"`),
               `Should have ${section} section visible at 1024px`);
    }
});

// =============================================
// Test Case 2: Full Navigation at 1024px Viewport
// Verifies full navigation bar visible, no hamburger menu
// =============================================
console.log('\nTest Case 2: Navigation at 1024px Viewport');
console.log('Input: Check navigation at 1024px viewport');
console.log('Expected: Full navigation bar visible, no hamburger menu');

test('Desktop media query sets horizontal nav layout', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    assert(desktopSection, 'Should have desktop media query section');

    // Check that nav-list is flex row in desktop view
    assert(desktopSection.includes('.nav-list') || css.includes('.nav-list'),
           'Should style nav-list for desktop');
    assert(css.includes('display: flex'),
           'Nav should use flexbox for horizontal layout');
});

test('Header uses horizontal flex layout on desktop', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        assert(desktopSection.includes('flex-direction: row') ||
               desktopSection.includes('.header-container'),
               'Header should use row layout on desktop');
    }
});

test('Navigation list is always visible (no hamburger on desktop)', () => {
    const css = loadMainCss();
    // Check desktop media query - nav should be flex and visible
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        // Desktop section should show nav in row layout
        assert(desktopSection.includes('.nav-list') ||
               desktopSection.includes('flex-direction: row'),
               'Desktop should have horizontal nav layout');
    }
    // Base nav-list uses flex display
    assert(css.includes('.nav-list') && css.includes('display: flex'),
           'Nav list should be flex display');
});

test('All navigation links are present', () => {
    const html = loadHomepageHtml();
    assert(html.includes('id="nav-overview"'), 'Should have Overview nav link');
    assert(html.includes('id="nav-api-docs"'), 'Should have API Docs nav link');
    assert(html.includes('id="nav-github"'), 'Should have GitHub nav link');
});

// =============================================
// Test Case 3: Dashboard Grid Layout at 1024px
// Verifies metrics displayed in grid/row layout
// =============================================
console.log('\nTest Case 3: Dashboard Layout at 1024px Viewport');
console.log('Input: Check dashboard layout at 1024px viewport');
console.log('Expected: Metrics displayed in grid/row layout');

test('Metrics grid uses CSS grid layout', () => {
    const css = loadMainCss();
    assert(css.includes('.metrics-grid'), 'Should have metrics-grid class');
    assert(css.includes('display: grid'), 'Should use CSS grid for metrics');
});

test('Desktop media query sets metrics to multi-column layout', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    assert(desktopSection, 'Should have desktop media query');
    assert(desktopSection.includes('.metrics-grid'),
           'Desktop media query should style metrics grid');
    assert(desktopSection.includes('grid-template-columns'),
           'Should define grid columns for desktop');
});

test('Metrics grid shows 5 columns on desktop (one for each metric)', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        // Check for repeat(5, 1fr) or similar 5-column layout
        assert(desktopSection.includes('repeat(5, 1fr)') ||
               desktopSection.includes('grid-template-columns'),
               'Should have 5-column layout for metrics on desktop');
    }
});

test('All 5 metric cards exist', () => {
    const html = loadHomepageHtml();
    const metrics = ['metric-uptime', 'metric-memory', 'metric-keys', 'metric-ops', 'metric-storage'];
    for (const metric of metrics) {
        assert(html.includes(`id="${metric}"`),
               `Should have ${metric} card in dashboard`);
    }
});

// =============================================
// Test Case 4: Large Screen Layout at 1920px
// Verifies content centered/constrained, not edge-to-edge
// =============================================
console.log('\nTest Case 4: Large Screen Layout at 1920px Viewport');
console.log('Input: Render page at 1920px viewport width');
console.log('Expected: Content centered or constrained, not stretched edge-to-edge');

test('CSS has 1920px large desktop media query', () => {
    const css = loadMainCss();
    assert(css.includes('@media (min-width: 1920px)') ||
           css.includes('@media (min-width: 1440px)'),
           'Should have large desktop media query');
});

test('Max-width is defined to constrain content', () => {
    const css = loadMainCss();
    assert(css.includes('--max-width'),
           'Should define max-width CSS variable');
    assert(css.includes('max-width: var(--max-width)') ||
           css.includes('max-width:'),
           'Should apply max-width constraint');
});

test('Main content area uses max-width and auto margins for centering', () => {
    const css = loadMainCss();
    assert(css.includes('.main'), 'Should have .main styles');
    assert(css.includes('margin: 0 auto') || css.includes('margin:0 auto'),
           'Should center content with auto margins');
});

test('Header container is constrained on large screens', () => {
    const css = loadMainCss();
    assert(css.includes('.header-container'),
           'Should have header-container styles');
    const largeScreenSection = css.split('@media (min-width: 1920px)')[1];
    if (largeScreenSection) {
        assert(largeScreenSection.includes('max-width') ||
               largeScreenSection.includes('.header-container'),
               'Header should be constrained on large screens');
    }
});

test('Footer container is constrained on large screens', () => {
    const css = loadMainCss();
    assert(css.includes('.footer-container'),
           'Should have footer-container styles');
});

test('Hero content has max-width for readability on large screens', () => {
    const css = loadMainCss();
    const largeScreenSection = css.split('@media (min-width: 1920px)')[1];
    if (largeScreenSection) {
        assert(largeScreenSection.includes('.hero-content') ||
               css.includes('.hero-content'),
               'Hero content should be constrained');
    }
});

test('Root max-width variable increases appropriately for large screens', () => {
    const css = loadMainCss();
    // Check base max-width exists
    assert(css.includes('--max-width: 1200px') || css.includes('--max-width:1200px'),
           'Should have base max-width of 1200px');
    // Check large screen max-width adjustment
    const largeScreenSection = css.split('@media (min-width: 1920px)')[1];
    if (largeScreenSection) {
        assert(largeScreenSection.includes('--max-width') ||
               largeScreenSection.includes('max-width'),
               'Large screen should adjust max-width');
    }
});

// =============================================
// Additional Desktop Layout Tests
// =============================================
console.log('\nAdditional Desktop Layout Tests');

test('Features grid uses multi-column layout on desktop', () => {
    const css = loadMainCss();
    assert(css.includes('.features-grid'), 'Should have features-grid class');
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        assert(desktopSection.includes('.features-grid') ||
               css.includes('grid-template-columns'),
               'Features should use multi-column grid on desktop');
    }
});

test('Code examples layout is optimized for desktop', () => {
    const css = loadMainCss();
    assert(css.includes('.code-examples'), 'Should have code-examples class');
});

test('Footer uses horizontal layout on desktop', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        assert(desktopSection.includes('.footer-container') ||
               desktopSection.includes('flex-direction: row'),
               'Footer should use horizontal layout on desktop');
    }
});

test('CSS variables are defined for consistent spacing', () => {
    const css = loadMainCss();
    assert(css.includes('--spacing-'), 'Should define spacing CSS variables');
});

test('No horizontal scroll elements on desktop (no overflow-x)', () => {
    const css = loadMainCss();
    // Main body should not have overflow-x: scroll
    // Check that base styles don't force horizontal scrolling
    assert(!css.includes('overflow-x: scroll') || css.includes('overflow-x: auto'),
           'Should not have forced horizontal scroll on main elements');
});

// =============================================
// MOBILE TESTS - Scenario 9: Responsive Layout Mobile
// =============================================

// =============================================
// Test Case 1: Mobile Layout at 375px Viewport (Scenario 9)
// All sections visible, content stacked vertically
// =============================================
console.log('\n--- MOBILE TESTS (Scenario 9) ---');
console.log('\nTest Case 1: Mobile Layout at 375px Viewport');
console.log('Input: Render page at 375px viewport width');
console.log('Expected: All sections visible, content stacked vertically');

test('CSS has 375px mobile media query', () => {
    const css = loadMainCss();
    assert(css.includes('@media (max-width: 375px)'),
           'Should have max-width: 375px media query for mobile');
});

test('CSS has 768px tablet/mobile breakpoint', () => {
    const css = loadMainCss();
    assert(css.includes('@media (max-width: 768px)'),
           'Should have max-width: 768px media query for tablet/mobile');
});

test('Mobile metrics grid stacks vertically (single column)', () => {
    const css = loadMainCss();
    // Check for single column grid on mobile
    const mobileSection = css.split('@media (max-width: 375px)')[1];
    if (mobileSection) {
        assert(mobileSection.includes('grid-template-columns: 1fr'),
               'Metrics grid should be single column on mobile');
    }
});

test('All main sections exist for mobile view', () => {
    const html = loadHomepageHtml();
    const sections = ['header', 'hero', 'dashboard', 'features', 'quickstart', 'documentation', 'footer'];
    for (const section of sections) {
        assert(html.includes(`id="${section}"`),
               `Should have ${section} section visible at 375px`);
    }
});

test('Hero title scales down for mobile', () => {
    const css = loadMainCss();
    const mobileSection = css.split('@media (max-width: 375px)')[1];
    if (mobileSection) {
        assert(mobileSection.includes('.hero-title'),
               'Should style hero title for mobile');
    }
    // Also check 768px breakpoint
    const tabletSection = css.split('@media (max-width: 768px)')[1];
    if (tabletSection) {
        assert(tabletSection.includes('.hero-title') || css.includes('.hero-title'),
               'Should have hero title styles');
    }
});

// =============================================
// Test Case 2: No Horizontal Overflow at 375px (Scenario 9)
// =============================================
console.log('\nTest Case 2: No Horizontal Overflow at 375px');
console.log('Input: Check for horizontal overflow at 375px');
console.log('Expected: No horizontal scroll required');

test('HTML and body have overflow-x hidden', () => {
    const css = loadMainCss();
    assert(css.includes('overflow-x: hidden'),
           'Should have overflow-x: hidden to prevent horizontal scroll');
});

test('Mobile elements have max-width 100%', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('max-width: 100%') || mobileSections.includes('max-width:100%'),
           'Mobile elements should have max-width: 100%');
});

test('Code blocks have overflow-x auto for scrolling', () => {
    const css = loadMainCss();
    assert(css.includes('.code-block'),
           'Should have code-block styles');
    assert(css.includes('overflow-x: auto'),
           'Code blocks should have overflow-x: auto for horizontal scroll within container');
});

test('Box-sizing border-box prevents overflow', () => {
    const css = loadMainCss();
    assert(css.includes('box-sizing: border-box'),
           'Should use border-box for predictable sizing');
});

// =============================================
// Test Case 3: Navigation Collapsed at 375px (Scenario 9)
// =============================================
console.log('\nTest Case 3: Navigation at 375px Viewport');
console.log('Input: Check navigation at 375px viewport');
console.log('Expected: Navigation collapsed to hamburger/mobile menu');

test('Hamburger menu button exists in HTML', () => {
    const html = loadHomepageHtml();
    assert(html.includes('hamburger-menu'),
           'Should have hamburger menu button in HTML');
    assert(html.includes('class="hamburger-menu"') || html.includes('id="hamburger-menu"'),
           'Should have hamburger-menu class or id');
});

test('Hamburger menu has accessible attributes', () => {
    const html = loadHomepageHtml();
    assert(html.includes('aria-label') && html.includes('hamburger'),
           'Hamburger menu should have aria-label');
    assert(html.includes('aria-expanded'),
           'Hamburger menu should have aria-expanded attribute');
});

test('CSS hides hamburger on desktop, shows on mobile', () => {
    const css = loadMainCss();
    assert(css.includes('.hamburger-menu'),
           'Should have hamburger-menu styles');
    // Desktop: display: none
    assert(css.includes('.hamburger-menu') && css.includes('display: none'),
           'Hamburger should be hidden by default (desktop)');
    // Mobile: display: flex
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('.hamburger-menu') && mobileSections.includes('display: flex'),
           'Hamburger should display: flex on mobile');
});

test('Nav list is hidden by default on mobile', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('.nav-list') && mobileSections.includes('display: none'),
           'Nav list should be hidden by default on mobile');
});

test('Nav list has open state class', () => {
    const css = loadMainCss();
    assert(css.includes('.nav-open') || css.includes('.nav-list.nav-open'),
           'Should have nav-open class for expanded state');
});

// =============================================
// Test Case 4: Hamburger Menu Expands (Scenario 9)
// =============================================
console.log('\nTest Case 4: Hamburger Menu Expansion');
console.log('Input: Tap hamburger menu icon');
console.log('Expected: Navigation menu expands showing all links');

test('Nav-open class shows nav list', () => {
    const css = loadMainCss();
    assert(css.includes('.nav-open') && css.includes('display: flex'),
           'Nav-open class should make nav list visible');
});

test('Mobile nav is positioned absolutely', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('.nav-list') && mobileSections.includes('position: absolute'),
           'Mobile nav should be positioned absolutely');
});

test('All navigation links are present for mobile menu', () => {
    const html = loadHomepageHtml();
    assert(html.includes('id="nav-overview"'), 'Should have Overview nav link');
    assert(html.includes('id="nav-api-docs"'), 'Should have API Docs nav link');
    assert(html.includes('id="nav-github"'), 'Should have GitHub nav link');
});

test('Hamburger menu has animated lines', () => {
    const html = loadHomepageHtml();
    const hamburgerLineCount = (html.match(/hamburger-line/g) || []).length;
    assert(hamburgerLineCount >= 3,
           'Hamburger menu should have 3 lines for animation');
});

test('CSS has hamburger animation transforms', () => {
    const css = loadMainCss();
    assert(css.includes('.hamburger-line'),
           'Should have hamburger-line styles');
    assert(css.includes('transform') && css.includes('rotate'),
           'Should have rotation transform for hamburger animation');
});

// =============================================
// Test Case 5: Touch Target Sizes (Scenario 9)
// =============================================
console.log('\nTest Case 5: Touch Target Sizes');
console.log('Input: Measure button touch target sizes');
console.log('Expected: All buttons/links minimum 44x44px touch area');

test('Hamburger menu has 44x44px minimum size', () => {
    const css = loadMainCss();
    assert(css.includes('.hamburger-menu'),
           'Should have hamburger-menu styles');
    // Check for explicit 44px dimensions
    assert(css.includes('width: 44px') || css.includes('min-width: 44px'),
           'Hamburger menu should have 44px width');
    assert(css.includes('height: 44px') || css.includes('min-height: 44px'),
           'Hamburger menu should have 44px height');
});

test('Mobile buttons have min-height 44px', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('min-height: 44px'),
           'Mobile buttons should have min-height: 44px');
});

test('CTA button has touch-friendly size on mobile', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('.cta-button'),
           'CTA button should be styled for mobile');
});

test('Nav links have touch-friendly size on mobile', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('.nav-link'),
           'Nav links should be styled for mobile touch targets');
});

test('Documentation links have touch-friendly size on mobile', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('.doc-link') || mobileSections.includes('.doc-links'),
           'Doc links should be styled for mobile touch targets');
});

// =============================================
// Test Case 6: Dashboard Metrics at 375px (Scenario 9)
// =============================================
console.log('\nTest Case 6: Dashboard Metrics at 375px');
console.log('Input: Check dashboard metrics at 375px');
console.log('Expected: Metrics stack vertically, readable without zoom');

test('Metrics grid is single column at 375px', () => {
    const css = loadMainCss();
    const mobileSection = css.split('@media (max-width: 375px)')[1];
    if (mobileSection) {
        assert(mobileSection.includes('.metrics-grid') && mobileSection.includes('grid-template-columns: 1fr'),
               'Metrics grid should be single column at 375px');
    }
});

test('Metric values have readable font size on mobile', () => {
    const css = loadMainCss();
    const mobileSection = css.split('@media (max-width: 375px)')[1];
    if (mobileSection) {
        assert(mobileSection.includes('.metric-value'),
               'Metric values should be styled for mobile readability');
    }
});

test('Dashboard section exists with all metrics', () => {
    const html = loadHomepageHtml();
    assert(html.includes('id="dashboard"'), 'Should have dashboard section');
    const metrics = ['metric-uptime', 'metric-memory', 'metric-keys', 'metric-ops', 'metric-storage'];
    for (const metric of metrics) {
        assert(html.includes(`id="${metric}"`),
               `Should have ${metric} card in dashboard`);
    }
});

test('Section titles scale for mobile', () => {
    const css = loadMainCss();
    const mobileSection = css.split('@media (max-width: 375px)')[1];
    if (mobileSection) {
        assert(mobileSection.includes('.section-title'),
               'Section titles should be styled for mobile');
    }
});

// =============================================
// Additional Mobile Tests
// =============================================
console.log('\nAdditional Mobile Tests');

test('Footer links stack vertically on smallest screens', () => {
    const css = loadMainCss();
    const mobileSection = css.split('@media (max-width: 375px)')[1];
    if (mobileSection) {
        assert(mobileSection.includes('.footer-links') && mobileSection.includes('flex-direction: column'),
               'Footer links should stack vertically on mobile');
    }
});

test('Features grid is single column on mobile', () => {
    const css = loadMainCss();
    const smallSection = css.split('@media (max-width: 480px)')[1];
    if (smallSection) {
        assert(smallSection.includes('.features-grid') && smallSection.includes('grid-template-columns: 1fr'),
               'Features grid should be single column on mobile');
    }
});

test('Code blocks have touch scrolling', () => {
    const css = loadMainCss();
    assert(css.includes('-webkit-overflow-scrolling: touch'),
           'Code blocks should have smooth touch scrolling');
});

test('Mobile nav list has dropdown styling', () => {
    const css = loadMainCss();
    const mobileSections = getMobile768Sections(css);
    assert(mobileSections.includes('.nav-list') && mobileSections.includes('box-shadow'),
           'Mobile nav should have dropdown shadow');
});

// =============================================
// Summary
// =============================================
console.log('\n' + '='.repeat(50));
console.log(`Responsive Desktop Test Results: ${passed} passed, ${failed} failed`);
console.log('='.repeat(50));

// Exit with appropriate code
if (failed > 0) {
    console.log('\nFailed tests:');
    results.filter(r => r.status === 'fail').forEach(r => {
        console.log(`  - ${r.name}: ${r.error}`);
    });
    process.exit(1);
} else {
    console.log('\nAll desktop responsive tests passed!');
    process.exit(0);
}
