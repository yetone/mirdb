/**
 * Accessibility E2E Tests
 * Owner: Scenario 13 - Keyboard Navigation, Scenario 16 - Progressive Enhancement
 *
 * End-to-end tests for accessibility:
 * - Tab navigation through all interactive elements
 * - Focus indicators visible
 * - Enter key activation
 * - Tests with JavaScript disabled
 * - Content visible without JS
 * - Links work without JS
 */

import { test, expect } from '@playwright/test';

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Tab through all interactive elements - all links, buttons, and copy buttons are keyboard focusable', async ({ page }) => {
    // Start focus at the beginning of the page
    await page.keyboard.press('Tab');

    // Track all focusable elements we encounter
    const focusedElements = [];
    let maxIterations = 50; // Safety limit

    while (maxIterations > 0) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          type: el.getAttribute('type'),
          textContent: el.textContent?.trim().substring(0, 50),
          ariaLabel: el.getAttribute('aria-label'),
        };
      });

      if (!activeElement) break;

      // Check if we've cycled back to the first element
      if (
        focusedElements.length > 0 &&
        activeElement.className === focusedElements[0].className &&
        activeElement.href === focusedElements[0].href &&
        activeElement.textContent === focusedElements[0].textContent
      ) {
        break;
      }

      focusedElements.push(activeElement);
      await page.keyboard.press('Tab');
      maxIterations--;
    }

    // Verify we found interactive elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify navigation links are focusable
    const navLinks = focusedElements.filter(
      (el) => el.tagName === 'a' && el.href && el.href.startsWith('#')
    );
    expect(navLinks.length).toBeGreaterThan(0);

    // Verify Get Started button (CTA link) is focusable
    const ctaLink = focusedElements.find(
      (el) =>
        el.tagName === 'a' &&
        (el.className?.includes('hero-cta') || el.textContent?.includes('Get Started'))
    );
    expect(ctaLink).toBeDefined();

    // Verify copy buttons are focusable
    const copyButtons = focusedElements.filter(
      (el) =>
        el.tagName === 'button' &&
        (el.className?.includes('copy') || el.ariaLabel?.includes('Copy'))
    );
    expect(copyButtons.length).toBeGreaterThan(0);

    // Verify external links are focusable
    const externalLinks = focusedElements.filter(
      (el) =>
        el.tagName === 'a' &&
        (el.href?.includes('github') || el.textContent?.includes('GitHub'))
    );
    expect(externalLinks.length).toBeGreaterThan(0);
  });

  test('TC2: Check focus indicator on Get Started button - button shows visible focus ring when focused', async ({ page }) => {
    // Find and focus the Get Started button
    const ctaButton = page.locator('.hero-cta');
    await ctaButton.focus();

    // Check that the button has a visible focus indicator
    const outlineStyle = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineColor: styles.outlineColor,
        outlineOffset: styles.outlineOffset,
        boxShadow: styles.boxShadow,
      };
    });

    // Verify focus indicator is visible (either outline or box-shadow)
    const hasOutline =
      outlineStyle.outlineStyle !== 'none' &&
      outlineStyle.outlineWidth !== '0px' &&
      outlineStyle.outline !== 'none' &&
      outlineStyle.outline !== '' &&
      !outlineStyle.outline.includes('0px');

    const hasBoxShadow =
      outlineStyle.boxShadow && outlineStyle.boxShadow !== 'none';

    expect(hasOutline || hasBoxShadow).toBe(true);
  });

  test('TC3: Check focus indicator on links - links show visible focus indicator when focused', async ({ page }) => {
    // Test navigation links
    const navLinks = page.locator('.nav__links a');
    const navLinkCount = await navLinks.count();

    expect(navLinkCount).toBeGreaterThan(0);

    // Focus the first navigation link
    const firstNavLink = navLinks.first();
    await firstNavLink.focus();

    // Check for visible focus indicator on nav link
    const navLinkFocusStyle = await firstNavLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineOffset: styles.outlineOffset,
        boxShadow: styles.boxShadow,
      };
    });

    const navHasOutline =
      navLinkFocusStyle.outlineStyle !== 'none' &&
      navLinkFocusStyle.outlineWidth !== '0px';

    const navHasBoxShadow =
      navLinkFocusStyle.boxShadow && navLinkFocusStyle.boxShadow !== 'none';

    expect(navHasOutline || navHasBoxShadow).toBe(true);

    // Test footer links
    const footerLinks = page.locator('.footer__link');
    const footerLinkCount = await footerLinks.count();

    if (footerLinkCount > 0) {
      const firstFooterLink = footerLinks.first();
      await firstFooterLink.focus();

      const footerLinkFocusStyle = await firstFooterLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineOffset: styles.outlineOffset,
          boxShadow: styles.boxShadow,
        };
      });

      const footerHasOutline =
        footerLinkFocusStyle.outlineStyle !== 'none' &&
        footerLinkFocusStyle.outlineWidth !== '0px';

      const footerHasBoxShadow =
        footerLinkFocusStyle.boxShadow &&
        footerLinkFocusStyle.boxShadow !== 'none';

      expect(footerHasOutline || footerHasBoxShadow).toBe(true);
    }
  });

  test('TC4: Press Enter on focused link - link is activated and navigates appropriately', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Focus and activate the Quick Start link via keyboard
    const quickStartLink = page.locator('.nav__links a[href="#quickstart"]');
    await quickStartLink.focus();

    // Verify the link is focused
    const isFocused = await quickStartLink.evaluate(
      (el) => document.activeElement === el
    );
    expect(isFocused).toBe(true);

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify the page has navigated (scrolled or URL changed)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    const currentHash = await page.evaluate(() => window.location.hash);

    // Either the page scrolled or the hash changed
    const didNavigate =
      finalScrollY > initialScrollY || currentHash === '#quickstart';
    expect(didNavigate).toBe(true);

    // Verify quickstart section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC5: Press Enter on focused copy button - copy action is triggered via keyboard', async ({ page }) => {
    // Find the first copy button
    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Focus the copy button
    await copyButton.focus();

    // Verify the button is focused
    const isFocused = await copyButton.evaluate(
      (el) => document.activeElement === el
    );
    expect(isFocused).toBe(true);

    // Get the initial button text
    const initialText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(initialText).toBe('Copy');

    // Press Enter to activate the copy action
    await page.keyboard.press('Enter');

    // Wait for the copy action to complete and show feedback
    await page.waitForTimeout(500);

    // Verify the button shows copied feedback
    const copiedText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(copiedText).toBe('Copied!');

    // Verify the button has the copied class
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);
  });

  test('All interactive elements have minimum touch target size of 44x44px', async ({ page }) => {
    // Test CTA button
    const ctaButton = page.locator('.hero-cta');
    const ctaBoundingBox = await ctaButton.boundingBox();
    expect(ctaBoundingBox.width).toBeGreaterThanOrEqual(44);
    expect(ctaBoundingBox.height).toBeGreaterThanOrEqual(44);

    // Test copy buttons
    const copyButtons = page.locator('.code-block__copy');
    const copyButtonCount = await copyButtons.count();

    for (let i = 0; i < copyButtonCount; i++) {
      const button = copyButtons.nth(i);
      const boundingBox = await button.boundingBox();
      expect(boundingBox.width).toBeGreaterThanOrEqual(44);
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);
    }
  });

  test('Focus order follows logical document order', async ({ page }) => {
    // Tab through elements and verify they follow logical order
    const focusOrder = [];
    let maxIterations = 50;

    await page.keyboard.press('Tab');

    while (maxIterations > 0) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        // Get element position in document
        const rect = el.getBoundingClientRect();
        return {
          top: rect.top + window.scrollY,
          left: rect.left,
          tagName: el.tagName.toLowerCase(),
          className: el.className,
        };
      });

      if (!activeElement) break;

      // Check for cycle back to start
      if (
        focusOrder.length > 0 &&
        activeElement.className === focusOrder[0].className &&
        activeElement.top === focusOrder[0].top
      ) {
        break;
      }

      focusOrder.push(activeElement);
      await page.keyboard.press('Tab');
      maxIterations--;
    }

    // Verify elements are generally in top-to-bottom order
    // (allowing for some variation in horizontal positioning)
    let outOfOrderCount = 0;
    for (let i = 1; i < focusOrder.length; i++) {
      // Allow elements to be within 200px of each other vertically
      // (for side-by-side elements)
      if (focusOrder[i].top < focusOrder[i - 1].top - 200) {
        outOfOrderCount++;
      }
    }

    // Allow for some out-of-order elements (e.g., modal close buttons)
    // but most should follow document order
    const outOfOrderPercentage = outOfOrderCount / focusOrder.length;
    expect(outOfOrderPercentage).toBeLessThan(0.2);
  });

  test('Shift+Tab navigates backwards through focusable elements', async ({ page }) => {
    // Tab forward a few times
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    // Get the current focused element
    const thirdElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.className : null;
    });

    // Tab forward once more
    await page.keyboard.press('Tab');

    const fourthElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.className : null;
    });

    // Shift+Tab backward
    await page.keyboard.press('Shift+Tab');

    // Should be back at the third element
    const backToThird = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.className : null;
    });

    expect(backToThird).toBe(thirdElement);
  });

  test('Space key activates buttons', async ({ page }) => {
    // Find the first copy button
    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Focus the copy button
    await copyButton.focus();

    // Get the initial button text
    const initialText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(initialText).toBe('Copy');

    // Press Space to activate the button
    await page.keyboard.press('Space');

    // Wait for the copy action to complete
    await page.waitForTimeout(500);

    // Verify the button shows copied feedback
    const copiedText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(copiedText).toBe('Copied!');
  });
});

/**
 * Progressive Enhancement Tests
 * Owner: Scenario 16 - Progressive Enhancement
 *
 * Tests to verify that core content renders and is functional
 * without JavaScript execution. This ensures the site follows
 * progressive enhancement principles.
 */
test.describe('Progressive Enhancement - No JavaScript', () => {
  test.use({ javaScriptEnabled: false });

  test('TC1: Page loads and displays all text content without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main structural elements are present
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('main')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Verify all sections are present and visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#roadmap')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();
  });

  test('TC2: Hero headline, tagline, and description are visible without JS', async ({ page }) => {
    await page.goto('/');

    // Verify hero section content
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify headline is visible and contains expected text
    const headline = page.locator('.hero-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('MirDB');
    await expect(headline).toContainText('Persistent Key-Value Store');
    await expect(headline).toContainText('Memcached Protocol');

    // Verify tagline is visible
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Simple, fast, and durable');
    await expect(tagline).toContainText('Rust');

    // Verify logo is visible
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Verify tech badges are visible
    const techStack = page.locator('.hero-tech-stack');
    await expect(techStack).toBeVisible();
    await expect(page.locator('.tech-badge--rust')).toBeVisible();
    await expect(page.locator('.tech-badge--tokio')).toBeVisible();

    // Verify Get Started CTA is visible
    const cta = page.locator('.hero-cta');
    await expect(cta).toBeVisible();
    await expect(cta).toContainText('Get Started');
  });

  test('TC3: Code blocks are readable without JS (syntax highlighting may be basic)', async ({ page }) => {
    await page.goto('/');

    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify code blocks are present and visible
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify each code block has readable content
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify the code content is visible
      const codeContent = codeBlock.locator('pre code');
      await expect(codeContent).toBeVisible();

      // Verify the code has actual text content
      const textContent = await codeContent.textContent();
      expect(textContent.length).toBeGreaterThan(0);
    }

    // Verify specific code examples are present
    // Installation command
    await expect(page.locator('code').filter({ hasText: 'cargo install mirdb' })).toBeVisible();

    // Server start command (use .first() since there are multiple mirdb-server references)
    await expect(page.locator('code').filter({ hasText: 'mirdb-server' }).first()).toBeVisible();

    // Telnet connection command
    await expect(page.locator('code').filter({ hasText: 'telnet localhost 11211' })).toBeVisible();

    // Memcached operations (use .first() since there are multiple matches)
    await expect(page.locator('code').filter({ hasText: 'set mykey' }).first()).toBeVisible();
    await expect(page.locator('code').filter({ hasText: 'get mykey' }).first()).toBeVisible();
  });

  test('TC4: Features section displays all feature items without JS', async ({ page }) => {
    await page.goto('/');

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features title
    const featuresTitle = page.locator('#features-title');
    await expect(featuresTitle).toBeVisible();
    await expect(featuresTitle).toContainText('Features');

    // Verify all feature items are visible
    const featureItems = page.locator('.features__item');
    const featureCount = await featureItems.count();
    expect(featureCount).toBeGreaterThanOrEqual(6);

    // Verify specific features are listed
    const expectedFeatures = [
      'Tokio Async Runtime',
      'Skip-List Memtable',
      'Write-Ahead Log',
      'Minor Compaction',
      'Major Compaction',
      'Memcached Protocol'
    ];

    for (const featureName of expectedFeatures) {
      const featureTitle = page.locator('.features__item-title').filter({ hasText: featureName });
      await expect(featureTitle).toBeVisible();
    }

    // Verify feature descriptions are visible
    const featureDescriptions = page.locator('.features__item-description');
    const descriptionCount = await featureDescriptions.count();
    expect(descriptionCount).toBeGreaterThanOrEqual(6);

    for (let i = 0; i < descriptionCount; i++) {
      const description = featureDescriptions.nth(i);
      await expect(description).toBeVisible();
      const textContent = await description.textContent();
      expect(textContent.length).toBeGreaterThan(0);
    }
  });

  test('TC5: GitHub link works and navigates to GitHub repository without JS', async ({ page }) => {
    await page.goto('/');

    // Find GitHub link in navigation
    const navGitHubLink = page.locator('.nav__links a[href*="github"]');
    await expect(navGitHubLink).toBeVisible();

    // Verify the link has correct href
    const navHref = await navGitHubLink.getAttribute('href');
    expect(navHref).toBe('https://github.com/yetone/mirdb');

    // Verify link has proper attributes for external link
    await expect(navGitHubLink).toHaveAttribute('target', '_blank');
    await expect(navGitHubLink).toHaveAttribute('rel', 'noopener');

    // Find GitHub link in footer
    const footerGitHubLink = page.locator('[data-testid="github-link"]');
    await expect(footerGitHubLink).toBeVisible();

    // Verify footer link has correct href
    const footerHref = await footerGitHubLink.getAttribute('href');
    expect(footerHref).toBe('https://github.com/yetone/mirdb');

    // Test that clicking the link would navigate (check href attribute)
    // Note: Actual navigation to external sites is not tested in E2E tests
    // We verify the link is properly configured for navigation
    await expect(footerGitHubLink).toHaveAttribute('target', '_blank');
    await expect(footerGitHubLink).toHaveAttribute('rel', 'noopener');
  });

  test('TC6: Copy buttons visible but code is still manually selectable without JS', async ({ page }) => {
    await page.goto('/');

    // Verify copy buttons are present (they may not work without JS)
    const copyButtons = page.locator('.code-block__copy');
    const copyButtonCount = await copyButtons.count();
    expect(copyButtonCount).toBeGreaterThan(0);

    // Verify code blocks are still visible and selectable
    const codeElements = page.locator('pre code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBeGreaterThan(0);

    for (let i = 0; i < codeCount; i++) {
      const codeElement = codeElements.nth(i);
      await expect(codeElement).toBeVisible();

      // Verify the code element has text content that can be selected
      const textContent = await codeElement.textContent();
      expect(textContent.length).toBeGreaterThan(0);

      // Verify the code element is not hidden or has display:none
      const isHidden = await codeElement.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.display === 'none' || styles.visibility === 'hidden';
      });
      expect(isHidden).toBe(false);

      // Verify user-select is not disabled on code elements
      const userSelect = await codeElement.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.userSelect;
      });
      // user-select should not be 'none' - it should allow text selection
      expect(userSelect).not.toBe('none');
    }
  });

  test('Navigation links work without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify navigation is visible
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Verify navigation links are present
    const navLinks = page.locator('.nav__links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify internal anchor links have proper href attributes
    const quickstartLink = page.locator('.nav__links a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();
    await expect(quickstartLink).toHaveAttribute('href', '#quickstart');

    const featuresLink = page.locator('.nav__links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');

    const roadmapLink = page.locator('.nav__links a[href="#roadmap"]');
    await expect(roadmapLink).toBeVisible();
    await expect(roadmapLink).toHaveAttribute('href', '#roadmap');

    const configLink = page.locator('.nav__links a[href="#configuration"]');
    await expect(configLink).toBeVisible();
    await expect(configLink).toHaveAttribute('href', '#configuration');
  });

  test('Internal anchor links navigate to correct sections without JS', async ({ page }) => {
    // Navigate directly to a section via anchor
    await page.goto('/#features');

    // Verify we're at the features section (URL has hash)
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');

    // The features section should be the target
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  test('Roadmap section is visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify roadmap section is present and visible
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Verify roadmap title
    const roadmapTitle = page.locator('#roadmap-title');
    await expect(roadmapTitle).toBeVisible();
    await expect(roadmapTitle).toContainText('Roadmap');

    // Verify roadmap items are visible
    const roadmapItems = page.locator('.roadmap__item');
    const itemCount = await roadmapItems.count();
    expect(itemCount).toBeGreaterThan(0);

    // Verify "Coming Soon" badges are visible
    const comingSoonBadges = page.locator('.roadmap__badge');
    const badgeCount = await comingSoonBadges.count();
    expect(badgeCount).toBeGreaterThan(0);

    // Verify specific roadmap items
    await expect(page.locator('.roadmap__item-title').filter({ hasText: 'Raft Consensus' })).toBeVisible();
  });

  test('Configuration section is visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify configuration section is present and visible
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify configuration title
    const configTitle = page.locator('#configuration-title');
    await expect(configTitle).toBeVisible();
    await expect(configTitle).toContainText('Configuration');

    // Verify configuration options are visible
    const configOptions = page.locator('.configuration__option');
    const optionCount = await configOptions.count();
    expect(optionCount).toBeGreaterThan(0);

    // Verify specific configuration options
    await expect(page.locator('.configuration__option-title').filter({ hasText: 'Port Configuration' })).toBeVisible();
    await expect(page.locator('.configuration__option-title').filter({ hasText: 'Data Directory' })).toBeVisible();
    await expect(page.locator('.configuration__option-title').filter({ hasText: 'WAL Settings' })).toBeVisible();
  });

  test('Footer content is visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer links are present
    const footerLinks = page.locator('.footer__links');
    await expect(footerLinks).toBeVisible();

    // Verify GitHub link
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub');

    // Verify License link
    const licenseLink = page.locator('[data-testid="license-link"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toContainText('License');

    // Verify CircleCI badge is present (image may not load without JS)
    const circleCIBadge = page.locator('[data-testid="circleci-badge"]');
    await expect(circleCIBadge).toBeVisible();

    // Verify license text
    const licenseText = page.locator('.footer__license');
    await expect(licenseText).toBeVisible();
    await expect(licenseText).toContainText('MIT License');
  });

  test('Get Started CTA link works without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Find the Get Started CTA
    const ctaLink = page.locator('.hero-cta');
    await expect(ctaLink).toBeVisible();

    // Verify it has the correct href to quickstart section
    await expect(ctaLink).toHaveAttribute('href', '#quickstart');

    // Click the CTA link (navigation will work via browser native behavior)
    await ctaLink.click();

    // Verify URL has changed to include the anchor
    const currentUrl = page.url();
    expect(currentUrl).toContain('#quickstart');
  });

  test('All images have alt text for accessibility without JS', async ({ page }) => {
    await page.goto('/');

    // Find all images
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify each image has alt text
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const altText = await img.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
    }
  });

  test('Language labels on code blocks are visible without JS', async ({ page }) => {
    await page.goto('/');

    // Find all code block language labels
    const languageLabels = page.locator('.code-block__language');
    const labelCount = await languageLabels.count();
    expect(labelCount).toBeGreaterThan(0);

    // Verify each label is visible and has content
    for (let i = 0; i < labelCount; i++) {
      const label = languageLabels.nth(i);
      await expect(label).toBeVisible();
      const textContent = await label.textContent();
      expect(textContent.length).toBeGreaterThan(0);
    }
  });
});
