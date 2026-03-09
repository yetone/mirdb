/**
 * Navigation E2E Tests
 * Owner: Scenario 8 - Navigation and Links
 *
 * Tests:
 * - Nav links to all sections
 * - GitHub button present
 * - Anchor navigation works
 * - Section IDs match nav hrefs
 * - Footer links present
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC-1: Navigation bar contains links to Features, Architecture, Quick Start, and Roadmap sections', async ({ page }) => {
    // Query for navigation bar
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Query for nav links list
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify Features link is present
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    // Verify Architecture link is present
    const architectureLink = navLinks.locator('a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();
    await expect(architectureLink).toHaveText('Architecture');

    // Verify Quick Start link is present
    const quickStartLink = navLinks.locator('a[href="#quickstart"]');
    await expect(quickStartLink).toBeVisible();
    await expect(quickStartLink).toHaveText('Quick Start');

    // Verify Roadmap link is present
    const roadmapLink = navLinks.locator('a[href="#roadmap"]');
    await expect(roadmapLink).toBeVisible();
    await expect(roadmapLink).toHaveText('Roadmap');

    // Verify Commands link is also present (additional section)
    const commandsLink = navLinks.locator('a[href="#commands"]');
    await expect(commandsLink).toBeVisible();

    // Verify Performance link is also present (additional section)
    const performanceLink = navLinks.locator('a[href="#performance"]');
    await expect(performanceLink).toBeVisible();
  });

  test('TC-2: GitHub button present in navigation with target="_blank" attribute', async ({ page }) => {
    // Query for GitHub button in navigation
    const navbar = page.locator('.navbar');
    const githubButton = navbar.locator('a.btn[href*="github.com"]');

    await expect(githubButton).toBeVisible();

    // Verify the href points to MirDB repository
    const href = await githubButton.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify target="_blank" attribute is present
    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel="noopener" for security
    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify button text
    await expect(githubButton).toContainText('GitHub');
  });

  test('TC-3: Click Features nav link scrolls to features section and updates URL hash', async ({ page }) => {
    // Click the Features nav link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify URL hash is updated to #features
    const url = page.url();
    expect(url).toContain('#features');

    // Verify the features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify the section heading is visible
    const featuresHeading = featuresSection.locator('h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toContainText('Key Features');
  });

  test('TC-4: All anchor IDs in nav hrefs match section IDs', async ({ page }) => {
    // Get all navigation links with anchor hrefs
    const navAnchors = await page.locator('.nav-links a[href^="#"]').all();

    expect(navAnchors.length).toBeGreaterThan(0);

    // For each nav link, verify the corresponding section exists
    for (const anchor of navAnchors) {
      const href = await anchor.getAttribute('href');
      const sectionId = href.replace('#', '');

      // Verify a section with matching ID exists
      const section = page.locator(`section#${sectionId}, #${sectionId}`);
      await expect(section).toBeAttached();

      // Verify the section is a valid page element
      const tagName = await section.evaluate(el => el.tagName.toLowerCase());
      expect(['section', 'div', 'article']).toContain(tagName);
    }

    // Specifically verify critical sections have correct IDs
    const requiredSections = ['features', 'architecture', 'quickstart', 'roadmap'];
    for (const sectionId of requiredSections) {
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeAttached();

      // Verify section has an id attribute matching the nav link
      const idAttr = await section.getAttribute('id');
      expect(idAttr).toBe(sectionId);
    }
  });

  test('TC-5: Footer contains links to documentation, license, and external resources', async ({ page }) => {
    // Query for footer
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Query for footer links navigation
    const footerLinks = footer.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Verify Documentation link is present
    const docsLink = footerLinks.locator('a[href*="docs.rs"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveText('Documentation');
    const docsTarget = await docsLink.getAttribute('target');
    expect(docsTarget).toBe('_blank');

    // Verify GitHub link is present
    const githubLink = footerLinks.locator('a[href*="github.com/yetone/mirdb"]:not([href*="issues"]):not([href*="LICENSE"])');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('GitHub');
    const githubTarget = await githubLink.getAttribute('target');
    expect(githubTarget).toBe('_blank');

    // Verify License link is present
    const licenseLink = footerLinks.locator('a[href*="LICENSE"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toHaveText('License');
    const licenseTarget = await licenseLink.getAttribute('target');
    expect(licenseTarget).toBe('_blank');

    // Verify Issues link is present (external resource for community)
    const issuesLink = footerLinks.locator('a[href*="issues"]');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveText('Issues');
    const issuesTarget = await issuesLink.getAttribute('target');
    expect(issuesTarget).toBe('_blank');

    // Verify all external links have rel="noopener" for security
    const externalLinks = await footerLinks.locator('a[target="_blank"]').all();
    for (const link of externalLinks) {
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  test('Navigation anchor links provide smooth scrolling', async ({ page }) => {
    // Verify CSS smooth scroll behavior is applied
    const scrollBehavior = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');

    // Test scrolling to architecture section
    const archLink = page.locator('.nav-links a[href="#architecture"]');
    await archLink.click();
    await page.waitForTimeout(500);

    const archSection = page.locator('#architecture');
    await expect(archSection).toBeInViewport();

    // Test scrolling to quickstart section
    const quickstartLink = page.locator('.nav-links a[href="#quickstart"]');
    await quickstartLink.click();
    await page.waitForTimeout(500);

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Navigation bar is fixed at top of page', async ({ page }) => {
    const navbar = page.locator('.navbar');

    // Verify navbar is visible
    await expect(navbar).toBeVisible();

    // Get navbar position style
    const position = await navbar.evaluate(el => {
      return getComputedStyle(el).position;
    });
    expect(position).toBe('fixed');

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(200);

    // Verify navbar is still visible after scrolling
    await expect(navbar).toBeVisible();
    await expect(navbar).toBeInViewport();
  });

  test('Footer has appropriate aria-label for accessibility', async ({ page }) => {
    const footerNav = page.locator('footer .footer-links');

    // Verify footer navigation has aria-label
    const ariaLabel = await footerNav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('footer');
  });

  test('Logo in navigation links to home', async ({ page }) => {
    const logo = page.locator('.navbar .logo');
    await expect(logo).toBeVisible();

    // Verify logo has href
    const href = await logo.getAttribute('href');
    expect(href).toBe('#');

    // Verify logo contains MirDB text
    const logoText = logo.locator('.logo-text');
    await expect(logoText).toHaveText('MirDB');

    // Verify logo contains image
    const logoImg = logo.locator('img.logo-img');
    await expect(logoImg).toBeVisible();
    const imgSrc = await logoImg.getAttribute('src');
    expect(imgSrc).toContain('logo.gif');
  });
});
