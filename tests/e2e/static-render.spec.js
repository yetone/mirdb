/**
 * Static Rendering E2E Tests
 * Owner: Scenario 14 - Static Rendering Without JavaScript
 *
 * Tests:
 * - All content renders without JavaScript enabled
 * - Anchor navigation works via standard browser behavior
 * - Code blocks are readable without JS syntax highlighting
 * - Navigation links accessible without JS-based mobile menu
 *
 * Validates NFR-4: Homepage shall be renderable as static HTML without requiring JavaScript execution
 */

const { test, expect } = require('@playwright/test');

// Configure all tests in this file to run with JavaScript disabled
test.use({ javaScriptEnabled: false });

test.describe('Static Rendering Without JavaScript', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC-1: All section content renders correctly with JavaScript disabled', async ({ page }) => {
    // Verify hero section renders
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const heroH1 = heroSection.locator('h1');
    await expect(heroH1).toBeVisible();
    await expect(heroH1).toHaveText('MirDB');

    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A Persistent Key-Value Store');

    // Verify hero logo image renders
    const heroLogo = heroSection.locator('img.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify CTA buttons render
    const ctaButtons = heroSection.locator('.hero-cta .btn');
    await expect(ctaButtons).toHaveCount(2);

    // Verify features section renders
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featuresH2 = featuresSection.locator('h2');
    await expect(featuresH2).toHaveText('Key Features');

    // Verify all 5 feature cards render
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);

    // Verify architecture section renders
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const archH2 = architectureSection.locator('h2');
    await expect(archH2).toHaveText('Architecture');

    // Verify architecture diagram (SVG) renders
    const archDiagram = architectureSection.locator('.architecture-diagram');
    await expect(archDiagram).toBeVisible();

    // Verify architecture component descriptions render
    const archComponents = architectureSection.locator('.arch-component');
    const archCount = await archComponents.count();
    expect(archCount).toBeGreaterThanOrEqual(3);

    // Verify quick start section renders
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    const quickstartH2 = quickstartSection.locator('h2');
    await expect(quickstartH2).toHaveText('Quick Start');

    // Verify commands section renders
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const commandsH2 = commandsSection.locator('h2');
    await expect(commandsH2).toHaveText('Supported Commands');

    // Verify performance section renders
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();

    const performanceH2 = performanceSection.locator('h2');
    await expect(performanceH2).toHaveText('Performance');

    // Verify roadmap section renders
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    const roadmapH2 = roadmapSection.locator('h2');
    await expect(roadmapH2).toHaveText('Roadmap');

    // Verify footer renders
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();
  });

  test('TC-2: Anchor links navigate to correct sections via standard browser behavior', async ({ page }) => {
    // Test navigation from header links to each section
    const navLinks = [
      { linkText: 'Features', targetId: '#features' },
      { linkText: 'Architecture', targetId: '#architecture' },
      { linkText: 'Quick Start', targetId: '#quickstart' },
      { linkText: 'Commands', targetId: '#commands' },
      { linkText: 'Performance', targetId: '#performance' },
      { linkText: 'Roadmap', targetId: '#roadmap' },
    ];

    for (const { linkText, targetId } of navLinks) {
      // Find and click the navigation link
      const navLink = page.locator(`.nav-links a:has-text("${linkText}")`);
      await expect(navLink).toBeVisible();

      // Verify the href attribute points to the correct anchor
      const href = await navLink.getAttribute('href');
      expect(href).toBe(targetId);

      // Click the link - this should work without JS via standard browser anchor behavior
      await navLink.click();

      // Verify URL hash changed to target
      await expect(page).toHaveURL(new RegExp(`${targetId.replace('#', '')}$`));

      // Verify the target section is now in view (or at least exists)
      const targetSection = page.locator(targetId);
      await expect(targetSection).toBeVisible();
    }

    // Test "Get Started" CTA button navigation
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const getStartedBtn = page.locator('#hero .hero-cta a:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    const getStartedHref = await getStartedBtn.getAttribute('href');
    expect(getStartedHref).toBe('#quickstart');

    await getStartedBtn.click();
    await expect(page).toHaveURL(/#quickstart$/);
  });

  test('TC-3: Code blocks are readable without syntax highlighting JavaScript', async ({ page }) => {
    // Navigate to quick start section which has code blocks
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Find all code containers
    const codeContainers = quickstartSection.locator('.code-container');
    const count = await codeContainers.count();
    expect(count).toBeGreaterThanOrEqual(3); // At least 3 code examples in quickstart

    // Verify each code block has readable content
    for (let i = 0; i < count; i++) {
      const container = codeContainers.nth(i);
      const preCode = container.locator('pre code');
      await expect(preCode).toBeVisible();

      // Get the text content and verify it's not empty
      const codeText = await preCode.textContent();
      expect(codeText.trim().length).toBeGreaterThan(0);
    }

    // Verify specific code content is readable
    // Check for git clone command
    const buildCode = quickstartSection.locator('.code-container pre code').first();
    const buildText = await buildCode.textContent();
    expect(buildText).toContain('git clone');
    expect(buildText).toContain('cargo build');

    // Check commands section also has readable code blocks
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const commandCodeBlocks = commandsSection.locator('pre code');
    const commandCount = await commandCodeBlocks.count();
    expect(commandCount).toBeGreaterThan(0);

    // Verify SET command syntax is readable
    const setCommandBlock = commandsSection.locator('.command-item[data-command="set"] pre code').first();
    const setCommandText = await setCommandBlock.textContent();
    expect(setCommandText).toContain('set');
    expect(setCommandText).toContain('<key>');

    // Verify code blocks have proper styling (monospace font)
    const codeStyles = await page.evaluate(() => {
      const codeElement = document.querySelector('#quickstart pre code');
      if (!codeElement) return null;
      const styles = window.getComputedStyle(codeElement);
      return {
        fontFamily: styles.fontFamily,
        display: window.getComputedStyle(codeElement.parentElement).display,
      };
    });

    expect(codeStyles).not.toBeNull();
    // Font family should include monospace
    expect(codeStyles.fontFamily.toLowerCase()).toMatch(/mono|consolas|courier/);
  });

  test('TC-4: Navigation links accessible even if JS-based menu does not function', async ({ page }) => {
    // Test at mobile viewport where hamburger menu would normally be used
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify the nav-links container exists in the DOM
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeAttached();

    // Get all navigation links
    const navLinkItems = navLinks.locator('a');
    const linkCount = await navLinkItems.count();
    expect(linkCount).toBeGreaterThanOrEqual(6); // Features, Architecture, Quickstart, Commands, Performance, Roadmap

    // Verify each navigation link has proper href attribute (accessible for fallback)
    const expectedLinks = ['#features', '#architecture', '#quickstart', '#commands', '#performance', '#roadmap'];

    for (const expectedHref of expectedLinks) {
      const link = navLinks.locator(`a[href="${expectedHref}"]`);
      await expect(link).toBeAttached();
    }

    // Verify the hamburger menu button exists (for JS enhancement)
    const hamburger = page.locator('.hamburger');
    await expect(hamburger).toBeAttached();

    // Verify footer navigation is always accessible as fallback
    const footerNav = page.locator('footer .footer-links');
    await expect(footerNav).toBeVisible();

    const footerLinks = footerNav.locator('a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThanOrEqual(3);

    // Test at tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Navigation should still be in DOM and functional via CSS
    await expect(navLinks).toBeAttached();

    // Test at desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // At desktop, nav links should be visible
    await expect(navLinks).toBeVisible();

    // Verify navigation links can be clicked and navigate correctly
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    await expect(page).toHaveURL(/#features$/);
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  test('All major page sections have proper ID attributes for anchor linking', async ({ page }) => {
    // Verify all sections have IDs that match navigation hrefs
    const sections = [
      { id: 'hero', shouldExist: true },
      { id: 'features', shouldExist: true },
      { id: 'architecture', shouldExist: true },
      { id: 'quickstart', shouldExist: true },
      { id: 'commands', shouldExist: true },
      { id: 'performance', shouldExist: true },
      { id: 'roadmap', shouldExist: true },
    ];

    for (const { id, shouldExist } of sections) {
      const section = page.locator(`#${id}`);
      if (shouldExist) {
        await expect(section).toBeAttached();
        await expect(section).toBeVisible();
      }
    }
  });

  test('Page content is not hidden or dependent on JavaScript for visibility', async ({ page }) => {
    // Verify no elements have JS-dependent display states that hide content
    const hiddenByJs = await page.evaluate(() => {
      const allElements = document.querySelectorAll('section, article, div.container, main');
      const hiddenElements = [];

      allElements.forEach((el) => {
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden') {
          // Check if this is a legitimate hidden element (like mobile menu in closed state)
          if (!el.classList.contains('mobile-menu') && !el.classList.contains('hamburger-menu')) {
            hiddenElements.push({
              tag: el.tagName,
              id: el.id,
              class: el.className,
            });
          }
        }
      });

      return hiddenElements;
    });

    // Should have no unexpectedly hidden major content sections
    const unexpectedHidden = hiddenByJs.filter(
      (el) => el.id && ['hero', 'features', 'architecture', 'quickstart', 'commands', 'performance', 'roadmap'].includes(el.id)
    );

    expect(unexpectedHidden).toHaveLength(0);
  });

  test('External links have proper attributes and are accessible without JS', async ({ page }) => {
    // Verify GitHub link in hero section
    const heroGithubLink = page.locator('#hero .hero-cta a[href*="github.com"]');
    await expect(heroGithubLink).toBeVisible();

    const heroGithubHref = await heroGithubLink.getAttribute('href');
    expect(heroGithubHref).toContain('github.com/yetone/mirdb');

    // Verify target="_blank" and rel="noopener" for security
    const target = await heroGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await heroGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify navigation GitHub button
    const navGithubBtn = page.locator('.navbar a[href*="github.com"]');
    await expect(navGithubBtn).toBeVisible();

    // Verify footer links are accessible
    const footerLinks = page.locator('footer .footer-links a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();

      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    }
  });

  test('Images render correctly without JavaScript', async ({ page }) => {
    // Verify hero logo image
    const heroLogo = page.locator('#hero img.hero-logo');
    await expect(heroLogo).toBeVisible();

    const heroLogoSrc = await heroLogo.getAttribute('src');
    expect(heroLogoSrc).toContain('logo.gif');

    const heroLogoAlt = await heroLogo.getAttribute('alt');
    expect(heroLogoAlt).toBeTruthy();

    // Verify navbar logo image
    const navLogo = page.locator('.navbar img.logo-img');
    await expect(navLogo).toBeVisible();

    const navLogoSrc = await navLogo.getAttribute('src');
    expect(navLogoSrc).toContain('logo.gif');

    // Verify SVG architecture diagram renders
    const archDiagram = page.locator('#architecture .architecture-diagram');
    await expect(archDiagram).toBeVisible();

    // Verify SVG has accessible title and desc
    const svgTitle = archDiagram.locator('title');
    await expect(svgTitle).toBeAttached();

    const svgDesc = archDiagram.locator('desc');
    await expect(svgDesc).toBeAttached();
  });

  test('Tables render correctly without JavaScript', async ({ page }) => {
    // Verify performance configuration table
    const configTable = page.locator('#performance .config-table');
    await expect(configTable).toBeVisible();

    // Verify table structure
    const thead = configTable.locator('thead');
    await expect(thead).toBeVisible();

    const tbody = configTable.locator('tbody');
    await expect(tbody).toBeVisible();

    // Verify table has rows
    const rows = tbody.locator('tr');
    const rowCount = await rows.count();
    expect(rowCount).toBeGreaterThanOrEqual(5); // At least 5 configuration parameters

    // Verify table content is readable
    const firstRowCells = rows.first().locator('td');
    const cellCount = await firstRowCells.count();
    expect(cellCount).toBe(3); // Parameter, Value, Description
  });
});
