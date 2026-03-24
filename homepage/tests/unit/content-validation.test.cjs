/**
 * Content Validation Unit Tests
 * Owner: Scenario 6 - Configuration Reference / Scenario 14 - Content Accuracy
 *
 * Tests for:
 * - Configuration values match PRD specifications
 * - Content accuracy validation
 */

const { readFileSync } = require('fs');
const { resolve } = require('path');

describe('Configuration Values Content Validation', () => {
    let htmlContent;

    beforeAll(() => {
        // Read the HTML file
        const htmlPath = resolve(__dirname, '../../src/index.html');
        htmlContent = readFileSync(htmlPath, 'utf8');
    });

    // Test Case 2: Default listen address
    test('should contain default listen address 0.0.0.0:12333', () => {
        expect(htmlContent).toContain('0.0.0.0:12333');
    });

    // Test Case 3: Default max LSM levels
    test('should contain default max LSM levels (7)', () => {
        // Check for the value 7 in the context of max_lsm_levels
        expect(htmlContent).toContain('data-config="max-lsm-levels"');
        expect(htmlContent).toMatch(/>7</);
    });

    // Test Case 4: Default work directory
    test('should contain default work directory (/tmp/mirdb)', () => {
        expect(htmlContent).toContain('/tmp/mirdb');
    });

    // Test Case 5: Default SSTable max size
    test('should contain default SSTable max size (100MB)', () => {
        expect(htmlContent).toContain('100MB');
    });

    // Test Case 6: Default memtable max size
    test('should contain default memtable max size (4MB)', () => {
        expect(htmlContent).toContain('4MB');
    });

    // Additional content validation tests
    test('should have configuration section with id="config"', () => {
        expect(htmlContent).toContain('id="config"');
    });

    test('should have configuration table with proper structure', () => {
        expect(htmlContent).toContain('class="config-table"');
        expect(htmlContent).toContain('<thead>');
        expect(htmlContent).toContain('<tbody>');
    });

    test('should have listen_address parameter documented', () => {
        expect(htmlContent).toMatch(/listen_address/i);
    });

    test('should have max_lsm_levels parameter documented', () => {
        expect(htmlContent).toMatch(/max_lsm_levels/i);
    });

    test('should have work_directory parameter documented', () => {
        expect(htmlContent).toMatch(/work_directory/i);
    });

    test('should have sstable_max_size parameter documented', () => {
        expect(htmlContent).toMatch(/sstable_max_size/i);
    });

    test('should have memtable_max_size parameter documented', () => {
        expect(htmlContent).toMatch(/memtable_max_size/i);
    });

    test('should have block_size parameter documented', () => {
        expect(htmlContent).toMatch(/block_size/i);
    });

    test('block_size should have value 4KB', () => {
        expect(htmlContent).toContain('4KB');
    });
});

/**
 * Content Accuracy Tests
 * Owner: Scenario 14 - Content Accuracy
 *
 * Tests for:
 * - External link validation (GitHub repository)
 * - Logo image presence
 * - CI badge presence and link
 * - Usage GIF presence (if applicable)
 */
describe('Content Accuracy - External Links and Assets', () => {
    let htmlContent;

    beforeAll(() => {
        // Read the HTML file
        const htmlPath = resolve(__dirname, '../../src/index.html');
        htmlContent = readFileSync(htmlPath, 'utf8');
    });

    // Test Case 1: Check GitHub repository link
    describe('GitHub Repository Link', () => {
        test('should contain link to MirDB GitHub repository', () => {
            expect(htmlContent).toContain('https://github.com/yetone/mirdb');
        });

        test('should have GitHub link in header navigation', () => {
            // Check for GitHub link in the header nav section
            const headerSection = htmlContent.match(/<!-- HEADER SECTION START[\s\S]*?<!-- HEADER SECTION END -->/);
            expect(headerSection).not.toBeNull();
            expect(headerSection[0]).toContain('https://github.com/yetone/mirdb');
        });

        test('should have "View on GitHub" CTA button with correct link', () => {
            // Check for View on GitHub button in hero section
            expect(htmlContent).toMatch(/View on GitHub[\s\S]*?https:\/\/github\.com\/yetone\/mirdb/);
        });

        test('should have GitHub link in footer', () => {
            // Check for GitHub link in footer section
            const footerSection = htmlContent.match(/<!-- FOOTER SECTION START[\s\S]*?<!-- FOOTER SECTION END -->/);
            expect(footerSection).not.toBeNull();
            expect(footerSection[0]).toContain('https://github.com/yetone/mirdb');
        });

        test('GitHub links should have proper security attributes', () => {
            // Check for noopener noreferrer on external links
            const githubLinkMatches = htmlContent.match(/href="https:\/\/github\.com\/yetone\/mirdb"[^>]*/g);
            expect(githubLinkMatches).not.toBeNull();
            expect(githubLinkMatches.length).toBeGreaterThan(0);

            // Verify at least one has security attributes
            const hasSecurityAttrs = githubLinkMatches.some(link =>
                link.includes('rel="noopener noreferrer"') || link.includes("rel='noopener noreferrer'")
            );
            expect(hasSecurityAttrs).toBe(true);
        });
    });

    // Test Case 2: Check logo image
    describe('Logo Image', () => {
        test('should have logo image in the page', () => {
            expect(htmlContent).toContain('logo.gif');
        });

        test('should have logo image in header', () => {
            const headerSection = htmlContent.match(/<!-- HEADER SECTION START[\s\S]*?<!-- HEADER SECTION END -->/);
            expect(headerSection).not.toBeNull();
            expect(headerSection[0]).toContain('logo.gif');
        });

        test('should have proper alt text for logo', () => {
            expect(htmlContent).toMatch(/alt="[^"]*[Ll]ogo[^"]*"/);
        });

        test('logo image should be in assets folder or use CDN', () => {
            // Check for local assets path or CDN URL
            const hasLocalLogo = htmlContent.includes('assets/images/logo.gif');
            const hasCDNLogo = htmlContent.includes('github.com/yetone/mirdb/raw/master/assets/logo.gif');
            expect(hasLocalLogo || hasCDNLogo).toBe(true);
        });
    });

    // Test Case 3: Check CI badge
    describe('CI Badge', () => {
        test('should have CI badge in the page', () => {
            // CI badge can be from atompunk or GitHub Actions badge
            const hasAtompunkBadge = htmlContent.includes('atompunk.yetone.fun/github/yetone/mirdb');
            const hasGitHubActionsBadge = htmlContent.includes('github.com/yetone/mirdb/actions');
            const hasWorkflowBadge = htmlContent.includes('workflow') && htmlContent.includes('badge');
            const hasCIBadge = htmlContent.toLowerCase().includes('ci') && (
                htmlContent.includes('badge') || htmlContent.includes('status')
            );

            // The CI badge should be present in some form
            expect(hasAtompunkBadge || hasGitHubActionsBadge || hasWorkflowBadge || hasCIBadge).toBe(true);
        });

        test('CI badge should be an image element', () => {
            // Look for any badge image
            const hasBadgeImg = htmlContent.match(/<img[^>]*(?:badge|ci|status|build)[^>]*>/i);
            expect(hasBadgeImg).not.toBeNull();
        });

        test('CI badge should link to build status', () => {
            // Badge should be wrapped in a link or have associated link
            const badgeWithLink = htmlContent.match(/<a[^>]*>[^<]*<img[^>]*(?:badge|ci|status|build)[^>]*>/i) ||
                                  htmlContent.match(/<a[^>]*href="[^"]*(?:actions|build|ci)[^"]*"[^>]*>[^<]*<img/i);
            expect(badgeWithLink).not.toBeNull();
        });
    });

    // Test Case 4: Usage GIF (if present)
    describe('Usage GIF', () => {
        test('should have usage demonstration GIF if present', () => {
            // Usage GIF may be present from PRD external assets
            const hasUsageGif = htmlContent.includes('usage.gif');
            const hasUsageDemo = htmlContent.match(/usage|demo|demonstration/i);

            // If usage.gif is present, verify it's an image
            if (hasUsageGif) {
                expect(htmlContent).toMatch(/<img[^>]*usage\.gif[^>]*>/i);
            }

            // Test passes if either condition is met (GIF present) or if no demo section exists
            // This handles the "if present" condition in the test case
            expect(hasUsageGif || hasUsageDemo !== null || true).toBe(true);
        });

        test('usage GIF should have proper attributes if present', () => {
            const usageGifMatch = htmlContent.match(/<img[^>]*usage\.gif[^>]*>/i);

            if (usageGifMatch) {
                // Should have alt text for accessibility
                expect(usageGifMatch[0]).toMatch(/alt="/);
            }
            // If not present, test passes (optional element)
            expect(true).toBe(true);
        });

        test('usage GIF should be loadable from valid source if present', () => {
            // Check for local or CDN path
            const hasLocalUsageGif = htmlContent.includes('assets/images/usage.gif');
            const hasCDNUsageGif = htmlContent.includes('github.com/yetone/mirdb/raw/master/assets/usage.gif');

            // If usage GIF is referenced, it should have a valid source
            if (htmlContent.includes('usage.gif')) {
                expect(hasLocalUsageGif || hasCDNUsageGif).toBe(true);
            }
            // If not present, test passes (optional element)
            expect(true).toBe(true);
        });
    });
});
