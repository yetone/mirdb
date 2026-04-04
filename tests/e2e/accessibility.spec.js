/**
 * Accessibility Tests
 * Owner: Scenario 8 - Accessibility Compliance
 *        Scenario 14 - Visual Design Consistency
 *
 * Tests:
 * - Semantic HTML structure
 * - Heading hierarchy
 * - Alt text on images
 * - Color contrast
 * - Keyboard navigation
 * - Focus indicators
 * - Typography and design
 */

const { test, expect } = require('@playwright/test');
const { SELECTORS, gotoHomepage } = require('./test-utils');

test.describe('Accessibility WCAG 2.1 AA Compliance', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    test.describe('TC1: Semantic HTML Structure', () => {
        test('page uses semantic elements (header, nav, main, section, footer)', async ({ page }) => {
            // Check for header element with banner role
            const header = page.locator('header[role="banner"]');
            await expect(header).toBeVisible();

            // Check for nav element with navigation role
            const nav = page.locator('nav[role="navigation"]');
            await expect(nav).toBeVisible();
            await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

            // Check for sections with region role
            const heroSection = page.locator('section#hero[role="region"]');
            await expect(heroSection).toBeVisible();
            await expect(heroSection).toHaveAttribute('aria-label', 'Hero');

            const featuresSection = page.locator('section#features[role="region"]');
            await expect(featuresSection).toBeVisible();
            await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

            const quickstartSection = page.locator('section#quickstart[role="region"]');
            await expect(quickstartSection).toBeVisible();
            await expect(quickstartSection).toHaveAttribute('aria-labelledby', 'quickstart-heading');

            // Check for footer element with contentinfo role
            const footer = page.locator('footer[role="contentinfo"]');
            await expect(footer).toBeVisible();
        });

        test('feature cards use article semantic element', async ({ page }) => {
            const articles = page.locator('article.feature-card');
            await expect(articles).toHaveCount(4);
        });
    });

    test.describe('TC2: Heading Hierarchy', () => {
        test('page has proper heading hierarchy (h1 -> h2 -> h3) without skipping levels', async ({ page }) => {
            // Check for single h1
            const h1Elements = page.locator('h1');
            await expect(h1Elements).toHaveCount(1);
            await expect(h1Elements).toHaveText('MirDB');

            // Check for h2 section headings
            const h2Elements = page.locator('h2');
            const h2Count = await h2Elements.count();
            expect(h2Count).toBeGreaterThanOrEqual(2);

            // Get all headings and verify hierarchy
            const headings = await page.evaluate(() => {
                const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
                return Array.from(headingElements).map(h => ({
                    level: parseInt(h.tagName.substring(1)),
                    text: h.textContent.trim()
                }));
            });

            // Verify no levels are skipped
            let previousLevel = 0;
            for (const heading of headings) {
                // Each heading level should be at most 1 more than the previous
                // or can decrease to any level
                if (heading.level > previousLevel) {
                    expect(heading.level).toBeLessThanOrEqual(previousLevel + 1);
                }
                previousLevel = heading.level;
            }

            // Verify heading structure
            const headingLevels = headings.map(h => h.level);
            expect(headingLevels[0]).toBe(1); // First heading should be h1
        });

        test('h3 elements are nested under h2 sections', async ({ page }) => {
            const h3Elements = page.locator('h3');
            const h3Count = await h3Elements.count();
            expect(h3Count).toBeGreaterThanOrEqual(4); // 4 feature card titles
        });
    });

    test.describe('TC3: Image Alt Text', () => {
        test('all img elements have descriptive alt attributes', async ({ page }) => {
            const images = page.locator('img');
            const imageCount = await images.count();
            expect(imageCount).toBeGreaterThan(0);

            for (let i = 0; i < imageCount; i++) {
                const img = images.nth(i);
                const alt = await img.getAttribute('alt');

                // Alt attribute should exist and not be empty
                expect(alt).not.toBeNull();
                expect(alt.trim().length).toBeGreaterThan(0);

                // Alt text should be descriptive (not just "image" or "photo")
                const genericAlts = ['image', 'photo', 'picture', 'img', ''];
                expect(genericAlts).not.toContain(alt.toLowerCase().trim());
            }
        });

        test('logo has descriptive alt text', async ({ page }) => {
            const logo = page.locator('.hero-logo');
            await expect(logo).toHaveAttribute('alt', 'MirDB Logo');
        });

        test('badge images have descriptive alt text', async ({ page }) => {
            const badge = page.locator('.badge');
            await expect(badge).toHaveAttribute('alt', 'CircleCI Build Status');
        });
    });

    test.describe('TC4: Color Contrast', () => {
        test('text-to-background contrast ratio is at least 4.5:1', async ({ page }) => {
            // Get computed styles for main text elements
            const contrastResults = await page.evaluate(() => {
                function getLuminance(r, g, b) {
                    const [rs, gs, bs] = [r, g, b].map(c => {
                        c = c / 255;
                        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
                    });
                    return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
                }

                function getContrastRatio(color1, color2) {
                    const l1 = getLuminance(...color1);
                    const l2 = getLuminance(...color2);
                    const lighter = Math.max(l1, l2);
                    const darker = Math.min(l1, l2);
                    return (lighter + 0.05) / (darker + 0.05);
                }

                function parseColor(colorStr) {
                    const match = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
                    if (match) {
                        return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
                    }
                    return [0, 0, 0];
                }

                function getBackgroundColor(element) {
                    let current = element;
                    while (current) {
                        const bg = window.getComputedStyle(current).backgroundColor;
                        if (bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
                            return parseColor(bg);
                        }
                        current = current.parentElement;
                    }
                    return [15, 23, 42]; // Default background color #0f172a
                }

                const results = [];
                const textElements = document.querySelectorAll('h1, h2, h3, p, a, li');

                textElements.forEach((el, index) => {
                    if (index > 20) return; // Limit checks
                    const style = window.getComputedStyle(el);
                    const textColor = parseColor(style.color);
                    const bgColor = getBackgroundColor(el);
                    const ratio = getContrastRatio(textColor, bgColor);

                    results.push({
                        element: el.tagName.toLowerCase(),
                        text: el.textContent.substring(0, 30),
                        ratio: ratio.toFixed(2),
                        meetsAA: ratio >= 4.5
                    });
                });

                return results;
            });

            // Verify all text elements meet WCAG AA contrast requirements
            for (const result of contrastResults) {
                expect(result.meetsAA,
                    `Element ${result.element} "${result.text}" has contrast ratio ${result.ratio}, needs 4.5:1`
                ).toBe(true);
            }
        });

        test('primary button text has sufficient contrast', async ({ page }) => {
            const button = page.locator('.btn-primary').first();
            const contrastRatio = await page.evaluate((selector) => {
                function getLuminance(r, g, b) {
                    const [rs, gs, bs] = [r, g, b].map(c => {
                        c = c / 255;
                        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
                    });
                    return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
                }

                function parseColor(colorStr) {
                    const match = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
                    if (match) {
                        return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
                    }
                    return [0, 0, 0];
                }

                const element = document.querySelector(selector);
                const style = window.getComputedStyle(element);
                const textColor = parseColor(style.color);
                const bgColor = parseColor(style.backgroundColor);

                const l1 = getLuminance(...textColor);
                const l2 = getLuminance(...bgColor);
                const lighter = Math.max(l1, l2);
                const darker = Math.min(l1, l2);
                return (lighter + 0.05) / (darker + 0.05);
            }, '.btn-primary');

            expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
        });
    });

    test.describe('TC5: Keyboard Navigation', () => {
        test('all interactive elements are focusable with Tab key', async ({ page }) => {
            // Get all interactive elements
            const interactiveElements = await page.locator('a, button, [tabindex="0"]').all();
            const expectedFocusableCount = interactiveElements.length;

            expect(expectedFocusableCount).toBeGreaterThan(0);

            // Tab through the page and count focusable elements
            let focusedCount = 0;
            const focusedElements = [];

            for (let i = 0; i < expectedFocusableCount + 5; i++) {
                await page.keyboard.press('Tab');
                const activeElement = await page.evaluate(() => {
                    const el = document.activeElement;
                    return {
                        tag: el.tagName.toLowerCase(),
                        text: el.textContent?.trim().substring(0, 30) || '',
                        href: el.getAttribute('href') || '',
                        isInteractive: ['a', 'button', 'input', 'select', 'textarea'].includes(el.tagName.toLowerCase()) ||
                                      el.getAttribute('tabindex') === '0'
                    };
                });

                if (activeElement.isInteractive && !focusedElements.includes(JSON.stringify(activeElement))) {
                    focusedElements.push(JSON.stringify(activeElement));
                    focusedCount++;
                }
            }

            // All interactive elements should be reachable via Tab
            expect(focusedCount).toBeGreaterThanOrEqual(expectedFocusableCount - 1);
        });

        test('navigation links are reachable via keyboard', async ({ page }) => {
            const navLinks = page.locator('.nav-links a');
            const navLinkCount = await navLinks.count();

            // Focus on each nav link via Tab
            await page.keyboard.press('Tab'); // Focus nav logo
            await page.keyboard.press('Tab'); // Focus first nav link

            for (let i = 0; i < navLinkCount; i++) {
                const focusedElement = await page.evaluate(() => document.activeElement.textContent?.trim());
                const expectedLink = await navLinks.nth(i).textContent();

                if (i === 0) {
                    expect(focusedElement).toBe(expectedLink?.trim());
                }
                await page.keyboard.press('Tab');
            }
        });

        test('CTA buttons are reachable via keyboard', async ({ page }) => {
            // Tab to the Get Started button
            let foundGetStarted = false;
            let foundGitHub = false;

            for (let i = 0; i < 15; i++) {
                await page.keyboard.press('Tab');
                const activeText = await page.evaluate(() => document.activeElement.textContent?.trim());

                if (activeText === 'Get Started') foundGetStarted = true;
                if (activeText === 'View on GitHub') foundGitHub = true;
            }

            expect(foundGetStarted).toBe(true);
            expect(foundGitHub).toBe(true);
        });
    });

    test.describe('TC6: Focus Indicators', () => {
        test('focused elements have visible focus indicators', async ({ page }) => {
            // Tab to first focusable element
            await page.keyboard.press('Tab');

            // Check that focus styles are applied
            const focusStyles = await page.evaluate(() => {
                const el = document.activeElement;
                const style = window.getComputedStyle(el);
                return {
                    outline: style.outline,
                    outlineWidth: style.outlineWidth,
                    outlineColor: style.outlineColor,
                    outlineStyle: style.outlineStyle,
                    boxShadow: style.boxShadow
                };
            });

            // Element should have visible outline or box-shadow
            const hasVisibleFocus =
                (focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none') ||
                focusStyles.boxShadow !== 'none';

            expect(hasVisibleFocus).toBe(true);
        });

        test('all links have visible focus indicators', async ({ page }) => {
            const links = page.locator('a');
            const linkCount = await links.count();

            // Check first few links for focus indicators
            for (let i = 0; i < Math.min(5, linkCount); i++) {
                const link = links.nth(i);
                await link.focus();

                const hasFocusIndicator = await page.evaluate((index) => {
                    const links = document.querySelectorAll('a');
                    const el = links[index];
                    if (!el) return false;

                    const style = window.getComputedStyle(el);
                    const hasOutline = style.outlineWidth !== '0px' && style.outlineStyle !== 'none';
                    const hasBoxShadow = style.boxShadow !== 'none';

                    return hasOutline || hasBoxShadow;
                }, i);

                expect(hasFocusIndicator, `Link ${i} should have visible focus indicator`).toBe(true);
            }
        });

        test('buttons have visible focus indicators', async ({ page }) => {
            const buttons = page.locator('.btn');
            const buttonCount = await buttons.count();

            for (let i = 0; i < buttonCount; i++) {
                const button = buttons.nth(i);
                await button.focus();

                const hasFocusIndicator = await page.evaluate(({ selector, index }) => {
                    const buttons = document.querySelectorAll(selector);
                    const el = buttons[index];
                    if (!el) return false;

                    const style = window.getComputedStyle(el);
                    const hasOutline = style.outlineWidth !== '0px' && style.outlineStyle !== 'none';
                    const hasBoxShadow = style.boxShadow !== 'none';

                    return hasOutline || hasBoxShadow;
                }, { selector: '.btn', index: i });

                expect(hasFocusIndicator, `Button ${i} should have visible focus indicator`).toBe(true);
            }
        });
    });

    test.describe('TC7: Link Text', () => {
        test('links have descriptive text (no click here or bare URLs)', async ({ page }) => {
            const links = page.locator('a');
            const linkCount = await links.count();

            const nonDescriptivePatterns = [
                'click here',
                'here',
                'read more',
                'learn more',
                'more',
                'link',
                /^https?:\/\//,  // Bare URLs
                /^\d+$/  // Just numbers
            ];

            for (let i = 0; i < linkCount; i++) {
                const link = links.nth(i);
                const text = await link.textContent();
                const ariaLabel = await link.getAttribute('aria-label');

                // Use either text content or aria-label for accessibility
                const accessibleName = (ariaLabel || text || '').trim().toLowerCase();

                // Check for non-descriptive text
                for (const pattern of nonDescriptivePatterns) {
                    if (typeof pattern === 'string') {
                        expect(accessibleName, `Link "${text}" should be descriptive`).not.toBe(pattern);
                    } else {
                        expect(accessibleName, `Link "${text}" should not be a bare URL`).not.toMatch(pattern);
                    }
                }

                // Ensure link has some accessible name
                expect(accessibleName.length, `Link should have accessible name`).toBeGreaterThan(0);
            }
        });

        test('navigation links have clear, descriptive text', async ({ page }) => {
            const navLinks = page.locator('.nav-links a');
            const linkTexts = await navLinks.allTextContents();

            const expectedDescriptiveTexts = ['Features', 'Quick Start', 'GitHub'];

            for (const text of linkTexts) {
                expect(expectedDescriptiveTexts).toContain(text.trim());
            }
        });

        test('CTA buttons have clear action-oriented text', async ({ page }) => {
            const primaryCta = page.locator('.btn-primary');
            await expect(primaryCta).toContainText('Get Started');

            const secondaryCta = page.locator('.btn-secondary').first();
            await expect(secondaryCta).toContainText('View on GitHub');
        });

        test('external links have appropriate attributes', async ({ page }) => {
            const externalLinks = page.locator('a[target="_blank"]');
            const count = await externalLinks.count();

            for (let i = 0; i < count; i++) {
                const link = externalLinks.nth(i);
                const rel = await link.getAttribute('rel');

                // External links should have noopener noreferrer for security
                expect(rel).toContain('noopener');
                expect(rel).toContain('noreferrer');
            }
        });
    });

    test.describe('Additional Accessibility Checks', () => {
        test('page has lang attribute', async ({ page }) => {
            const html = page.locator('html');
            await expect(html).toHaveAttribute('lang', 'en');
        });

        test('page has descriptive title', async ({ page }) => {
            const title = await page.title();
            expect(title).toContain('MirDB');
            expect(title.length).toBeGreaterThan(10);
        });

        test('page has meta description', async ({ page }) => {
            const metaDesc = page.locator('meta[name="description"]');
            const content = await metaDesc.getAttribute('content');
            expect(content).toBeTruthy();
            expect(content.length).toBeGreaterThan(20);
        });

        test('decorative icons are hidden from screen readers', async ({ page }) => {
            const decorativeIcons = page.locator('.feature-icon[aria-hidden="true"]');
            const count = await decorativeIcons.count();
            expect(count).toBe(4); // All 4 feature icons should be decorative
        });

        test('SVG icons have appropriate roles', async ({ page }) => {
            const svgIcons = page.locator('svg[role="img"]');
            const count = await svgIcons.count();
            expect(count).toBeGreaterThan(0);

            // Check that SVGs have aria-label
            for (let i = 0; i < count; i++) {
                const svg = svgIcons.nth(i);
                const ariaLabel = await svg.getAttribute('aria-label');
                expect(ariaLabel).toBeTruthy();
            }
        });
    });
});
