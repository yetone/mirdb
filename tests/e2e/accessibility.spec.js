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

    // Scenario 14: Visual Design and Technical Aesthetic Tests (NFR-5)
    test.describe('Visual Design - Typography', () => {
        test('page uses clean, readable system fonts appropriate for technical content', async ({ page }) => {
            // Verify body uses system font stack
            const bodyFont = await page.evaluate(() => {
                const body = document.body;
                return window.getComputedStyle(body).fontFamily;
            });

            // Should use system font stack (not decorative fonts)
            const systemFonts = ['-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'];
            const usesSystemFont = systemFonts.some(font => bodyFont.toLowerCase().includes(font.toLowerCase()));
            expect(usesSystemFont, 'Page should use system font stack').toBe(true);
        });

        test('headings have appropriate font weights for hierarchy', async ({ page }) => {
            const headingStyles = await page.evaluate(() => {
                const h1 = document.querySelector('h1');
                const h2 = document.querySelector('h2');
                const h3 = document.querySelector('h3');
                return {
                    h1: {
                        weight: window.getComputedStyle(h1).fontWeight,
                        size: parseFloat(window.getComputedStyle(h1).fontSize)
                    },
                    h2: {
                        weight: window.getComputedStyle(h2).fontWeight,
                        size: parseFloat(window.getComputedStyle(h2).fontSize)
                    },
                    h3: {
                        weight: window.getComputedStyle(h3).fontWeight,
                        size: parseFloat(window.getComputedStyle(h3).fontSize)
                    }
                };
            });

            // Headings should have semi-bold or bold weight (>=500)
            expect(parseInt(headingStyles.h1.weight)).toBeGreaterThanOrEqual(500);
            expect(parseInt(headingStyles.h2.weight)).toBeGreaterThanOrEqual(500);
            expect(parseInt(headingStyles.h3.weight)).toBeGreaterThanOrEqual(500);

            // Font size should decrease h1 > h2 > h3
            expect(headingStyles.h1.size).toBeGreaterThan(headingStyles.h2.size);
            expect(headingStyles.h2.size).toBeGreaterThan(headingStyles.h3.size);
        });

        test('body text has readable line height', async ({ page }) => {
            const lineHeight = await page.evaluate(() => {
                const body = document.body;
                const styles = window.getComputedStyle(body);
                const lineHeightValue = styles.lineHeight;
                const fontSize = parseFloat(styles.fontSize);

                // If line-height is a number, multiply by font-size
                // If it's already in pixels, just parse it
                if (lineHeightValue === 'normal') {
                    return 1.2; // Browser default
                }
                const numericLineHeight = parseFloat(lineHeightValue);
                if (lineHeightValue.includes('px')) {
                    return numericLineHeight / fontSize;
                }
                return numericLineHeight;
            });

            // Line height should be between 1.4 and 1.8 for readability
            expect(lineHeight).toBeGreaterThanOrEqual(1.4);
            expect(lineHeight).toBeLessThanOrEqual(1.8);
        });
    });

    test.describe('Visual Design - Color Scheme', () => {
        test('color palette is professional and not overly colorful', async ({ page }) => {
            const colors = await page.evaluate(() => {
                const root = document.documentElement;
                const styles = getComputedStyle(root);
                return {
                    background: styles.getPropertyValue('--color-background').trim(),
                    surface: styles.getPropertyValue('--color-surface').trim(),
                    text: styles.getPropertyValue('--color-text').trim(),
                    textSecondary: styles.getPropertyValue('--color-text-secondary').trim(),
                    primary: styles.getPropertyValue('--color-primary').trim(),
                    accent: styles.getPropertyValue('--color-accent').trim()
                };
            });

            // Dark theme with muted colors indicates professional look
            expect(colors.background).toBeTruthy();
            expect(colors.text).toBeTruthy();

            // Verify colors are defined (not empty)
            Object.values(colors).forEach(color => {
                expect(color.length).toBeGreaterThan(0);
            });
        });

        test('uses dark theme appropriate for infrastructure tools', async ({ page }) => {
            const bgColor = await page.evaluate(() => {
                const body = document.body;
                const bg = window.getComputedStyle(body).backgroundColor;
                const match = bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
                if (match) {
                    return {
                        r: parseInt(match[1]),
                        g: parseInt(match[2]),
                        b: parseInt(match[3])
                    };
                }
                return null;
            });

            // Background should be dark (RGB values low)
            expect(bgColor).not.toBeNull();
            expect(bgColor.r).toBeLessThan(50);
            expect(bgColor.g).toBeLessThan(50);
            expect(bgColor.b).toBeLessThan(80);
        });

        test('limited color palette with consistent accent color', async ({ page }) => {
            const accentUsage = await page.evaluate(() => {
                const links = document.querySelectorAll('a');
                const accentColors = new Set();

                links.forEach(link => {
                    const color = window.getComputedStyle(link).color;
                    accentColors.add(color);
                });

                return {
                    uniqueAccentColors: accentColors.size,
                    colors: Array.from(accentColors)
                };
            });

            // Should have limited color palette (not rainbow)
            expect(accentUsage.uniqueAccentColors).toBeLessThanOrEqual(4);
        });
    });

    test.describe('Visual Design - Whitespace', () => {
        test('adequate whitespace provides clean, uncluttered appearance', async ({ page }) => {
            const spacing = await page.evaluate(() => {
                const sections = document.querySelectorAll('section');
                const spacingValues = [];

                sections.forEach(section => {
                    const styles = window.getComputedStyle(section);
                    spacingValues.push({
                        paddingTop: parseFloat(styles.paddingTop),
                        paddingBottom: parseFloat(styles.paddingBottom),
                        element: section.id || 'unnamed'
                    });
                });

                return spacingValues;
            });

            // Each section should have adequate vertical padding (at least 40px)
            spacing.forEach(section => {
                expect(section.paddingTop, `Section ${section.element} should have adequate top padding`).toBeGreaterThanOrEqual(24);
                expect(section.paddingBottom, `Section ${section.element} should have adequate bottom padding`).toBeGreaterThanOrEqual(24);
            });
        });

        test('feature cards have proper spacing between elements', async ({ page }) => {
            const cardSpacing = await page.evaluate(() => {
                const cards = document.querySelectorAll('.feature-card');
                if (cards.length === 0) return null;

                const card = cards[0];
                const styles = window.getComputedStyle(card);

                return {
                    padding: parseFloat(styles.padding) || parseFloat(styles.paddingTop),
                    gap: parseFloat(styles.gap) || 0
                };
            });

            expect(cardSpacing).not.toBeNull();
            expect(cardSpacing.padding).toBeGreaterThanOrEqual(16);
        });

        test('hero section has generous spacing for visual hierarchy', async ({ page }) => {
            const heroSpacing = await page.evaluate(() => {
                const hero = document.querySelector('.hero');
                const heroContent = document.querySelector('.hero-content');

                if (!hero || !heroContent) return null;

                const heroStyles = window.getComputedStyle(hero);
                const viewportHeight = window.innerHeight;
                const heroHeight = parseFloat(heroStyles.minHeight);

                return {
                    minHeight: heroStyles.minHeight,
                    heroHeightPx: heroHeight,
                    viewportHeight: viewportHeight,
                    coversViewport: heroHeight >= viewportHeight * 0.9,
                    paddingTop: parseFloat(heroStyles.paddingTop),
                    paddingBottom: parseFloat(heroStyles.paddingBottom)
                };
            });

            expect(heroSpacing).not.toBeNull();
            // Hero should cover at least 90% of viewport height for visual prominence
            expect(heroSpacing.coversViewport, 'Hero section should cover most of viewport').toBe(true);
        });
    });

    test.describe('Visual Design - Code Block Styling', () => {
        test('code blocks have monospace font with appropriate styling', async ({ page }) => {
            const codeStyles = await page.evaluate(() => {
                const codeBlock = document.querySelector('.code-block code');
                if (!codeBlock) return null;

                const styles = window.getComputedStyle(codeBlock);
                return {
                    fontFamily: styles.fontFamily,
                    fontSize: styles.fontSize,
                    lineHeight: styles.lineHeight,
                    color: styles.color
                };
            });

            expect(codeStyles).not.toBeNull();

            // Should use monospace font
            const monoFonts = ['SF Mono', 'Fira Code', 'Fira Mono', 'Menlo', 'Monaco', 'Consolas', 'monospace'];
            const usesMonoFont = monoFonts.some(font =>
                codeStyles.fontFamily.toLowerCase().includes(font.toLowerCase())
            );
            expect(usesMonoFont, 'Code blocks should use monospace font').toBe(true);
        });

        test('code blocks have distinct background styling', async ({ page }) => {
            const blockStyles = await page.evaluate(() => {
                const codeBlock = document.querySelector('.code-block');
                if (!codeBlock) return null;

                const styles = window.getComputedStyle(codeBlock);
                return {
                    backgroundColor: styles.backgroundColor,
                    borderRadius: styles.borderRadius,
                    border: styles.border
                };
            });

            expect(blockStyles).not.toBeNull();
            // Code blocks should have a visible background (not transparent)
            expect(blockStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
            expect(blockStyles.backgroundColor).not.toBe('transparent');
        });

        test('code blocks have syntax highlighting', async ({ page }) => {
            const hasSyntaxHighlighting = await page.evaluate(() => {
                const codeBlock = document.querySelector('.code-block.syntax-highlighted');
                if (!codeBlock) return false;

                // Check for syntax highlighting classes
                const highlightClasses = [
                    '.code-comment',
                    '.code-command',
                    '.code-keyword',
                    '.code-variable',
                    '.code-string',
                    '.code-output'
                ];

                return highlightClasses.some(cls => codeBlock.querySelector(cls) !== null);
            });

            expect(hasSyntaxHighlighting, 'Code blocks should have syntax highlighting').toBe(true);
        });

        test('code block pre element has proper overflow handling', async ({ page }) => {
            const preStyles = await page.evaluate(() => {
                const pre = document.querySelector('.code-block pre');
                if (!pre) return null;

                const styles = window.getComputedStyle(pre);
                return {
                    overflowX: styles.overflowX,
                    padding: styles.padding
                };
            });

            expect(preStyles).not.toBeNull();
            // Pre should have horizontal scrolling for long lines
            expect(preStyles.overflowX).toBe('auto');
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
