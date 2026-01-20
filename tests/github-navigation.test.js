/**
 * GitHub Navigation and Project Status Tests
 * Tests for verifying GitHub repository links and CI status badges (REQ-5, REQ-6)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load the HTML file
const htmlPath = path.join(__dirname, '..', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

// Parse with JSDOM
const dom = new JSDOM(html);
const document = dom.window.document;

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

function test(name, fn) {
    try {
        fn();
        passed++;
        results.push({ name, status: 'pass' });
        console.log(`\x1b[32m✓\x1b[0m ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`\x1b[31m✗\x1b[0m ${name}`);
        console.log(`  Error: ${error.message}`);
    }
}

function assert(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

console.log('\n=== GitHub Navigation and Project Status Tests ===\n');

// Test Case 1: Check for GitHub link presence
// Type: unit
// Expected: At least one link to GitHub repository is present on the page
test('TC1: At least one link to GitHub repository is present on the page', () => {
    const allLinks = document.querySelectorAll('a[href*="github.com"]');
    assert(allLinks.length > 0, 'Page should contain at least one GitHub link');

    // Verify that we have multiple GitHub links (hero button, footer, etc.)
    const githubUrls = Array.from(allLinks).map(link => link.getAttribute('href'));
    console.log(`  Found ${githubUrls.length} GitHub links on the page`);

    // Check that at least one link is to the main repository
    const mainRepoLinks = Array.from(allLinks).filter(link =>
        link.getAttribute('href').includes('github.com/yetone/mirdb')
    );
    assert(mainRepoLinks.length > 0, 'At least one link should point to the mirdb repository');
});

// Test Case 2: Verify GitHub link URL
// Type: unit
// Expected: GitHub link href contains 'github.com' and points to the mirdb repository
test('TC2: GitHub link href contains github.com and points to the mirdb repository', () => {
    const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
    assert(githubLinks.length > 0, 'Page should contain at least one link to github.com/yetone/mirdb');

    // Check that the main "View on GitHub" button has the correct URL
    const viewOnGithubBtn = document.querySelector('a.btn-secondary[href*="github.com"]');
    assert(viewOnGithubBtn, 'View on GitHub button should exist');

    const href = viewOnGithubBtn.getAttribute('href');
    assert(href === 'https://github.com/yetone/mirdb',
        `GitHub button should link to "https://github.com/yetone/mirdb", got "${href}"`);

    // Verify footer GitHub link also exists and is correct
    const footerGithubLink = document.querySelector('footer a[href*="github.com/yetone/mirdb"]');
    assert(footerGithubLink, 'Footer should contain a GitHub link');

    const footerHref = footerGithubLink.getAttribute('href');
    assert(footerHref === 'https://github.com/yetone/mirdb',
        `Footer GitHub link should be "https://github.com/yetone/mirdb", got "${footerHref}"`);
});

// Test Case 3: Check for CI status badge (DOM validation)
// Type: unit (DOM-based check for badge elements)
// Expected: CircleCI or similar CI status badge image is displayed
test('TC3: CI status badge image is present in the DOM', () => {
    const badgesContainer = document.querySelector('.badges');
    assert(badgesContainer, 'Badges container should exist with class "badges"');

    const badgeLink = badgesContainer.querySelector('a');
    assert(badgeLink, 'Badge should be wrapped in a link');

    const badgeImg = badgesContainer.querySelector('img');
    assert(badgeImg, 'Badge image should exist');

    const imgSrc = badgeImg.getAttribute('src');
    assert(imgSrc, 'Badge image should have a src attribute');

    // The badge image URL should be present (it's a CI status badge)
    const altText = badgeImg.getAttribute('alt');
    assert(altText, 'Badge image should have alt text for accessibility');
    assert(altText.toLowerCase().includes('circleci') || altText.toLowerCase().includes('build') || altText.toLowerCase().includes('status'),
        `Badge alt text should mention CI status, got "${altText}"`);
});

// Test Case 4: Verify badge links to CI dashboard (DOM validation)
// Type: unit (DOM-based check for link attributes)
// Expected: Clicking CI badge navigates to CircleCI build status page
test('TC4: CI badge links to CircleCI build status page', () => {
    const badgesContainer = document.querySelector('.badges');
    assert(badgesContainer, 'Badges container should exist');

    const badgeLink = badgesContainer.querySelector('a');
    assert(badgeLink, 'Badge should be wrapped in a link element');

    const href = badgeLink.getAttribute('href');
    assert(href, 'Badge link should have an href attribute');
    assert(href.includes('circleci.com'),
        `Badge link should point to CircleCI, got "${href}"`);
    assert(href.includes('yetone/mirdb'),
        `Badge link should include the repository path, got "${href}"`);

    // Verify external link security attributes
    const target = badgeLink.getAttribute('target');
    assert(target === '_blank', 'Badge link should open in new tab (target="_blank")');

    const rel = badgeLink.getAttribute('rel');
    assert(rel && rel.includes('noopener'), 'Badge link should have rel="noopener" for security');
});

// Additional test: Verify all GitHub links have proper security attributes
test('TC5: All GitHub links have proper security attributes', () => {
    const externalLinks = document.querySelectorAll('a[href*="github.com"]');

    externalLinks.forEach((link, index) => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        assert(target === '_blank',
            `Link ${index + 1} should have target="_blank", got "${target}"`);
        assert(rel && rel.includes('noopener'),
            `Link ${index + 1} should have rel containing "noopener", got "${rel}"`);
    });

    console.log(`  Verified ${externalLinks.length} external GitHub links have proper security attributes`);
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
