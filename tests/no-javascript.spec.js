const { test, expect } = require('@playwright/test');

/**
 * NFR-7: Page should work without JavaScript for core content display
 *
 * These tests verify that the MirDB landing page displays core content
 * correctly when JavaScript is disabled. Interactive features like
 * copy-to-clipboard may not work, but all content should be visible.
 */
test.describe('Core Content Without JavaScript', () => {
  // Disable JavaScript for all tests in this suite
  test.use({ javaScriptEnabled: false });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Hero section with product name and tagline is visible', async ({ page }) => {
    // Test Case 1: Load page with JavaScript disabled
    // Expected: Hero section with product name and tagline is visible

    // Verify hero section exists and is visible
    const heroSection = page.locator('[data-testid="hero-header"]');
    await expect(heroSection).toBeVisible();

    // Verify product name (MirDB) is visible
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');

    // Verify CTA buttons are visible
    const getStartedButton = heroSection.locator('a:has-text("Get Started")');
    await expect(getStartedButton).toBeVisible();

    const githubButton = heroSection.locator('a:has-text("View on GitHub")');
    await expect(githubButton).toBeVisible();
  });

  test('Key features are visible and readable', async ({ page }) => {
    // Test Case 2: Check features section with JS disabled
    // Expected: Key features are visible and readable

    // Verify features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify section title
    const sectionTitle = featuresSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Why MirDB');

    // Verify features grid is visible
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify Memcached Compatible feature card
    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedFeature).toBeVisible();
    const memcachedTitle = memcachedFeature.locator('h3');
    await expect(memcachedTitle).toHaveText('Memcached Compatible');
    const memcachedDesc = memcachedFeature.locator('p');
    await expect(memcachedDesc).toBeVisible();

    // Verify Data Persistence feature card
    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    await expect(persistenceFeature).toBeVisible();
    const persistenceTitle = persistenceFeature.locator('h3');
    await expect(persistenceTitle).toHaveText('Data Persistence');
    const persistenceDesc = persistenceFeature.locator('p');
    await expect(persistenceDesc).toBeVisible();

    // Verify High Performance feature card
    const performanceFeature = page.locator('[data-testid="feature-performance"]');
    await expect(performanceFeature).toBeVisible();
    const performanceTitle = performanceFeature.locator('h3');
    await expect(performanceTitle).toHaveText('High Performance');
    const performanceDesc = performanceFeature.locator('p');
    await expect(performanceDesc).toBeVisible();
  });

  test('Code examples are visible (copy button may not work)', async ({ page }) => {
    // Test Case 3: Check code blocks with JS disabled
    // Expected: Code examples are visible (copy button may not work)

    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify installation code block is visible
    const installCodeBlock = page.locator('[data-testid="install-code-block"]');
    await expect(installCodeBlock).toBeVisible();
    const installCode = installCodeBlock.locator('code');
    await expect(installCode).toBeVisible();
    await expect(installCode).toContainText('cargo build --release');

    // Verify run code block is visible
    const runCodeBlock = page.locator('[data-testid="run-code-block"]');
    await expect(runCodeBlock).toBeVisible();
    const runCode = runCodeBlock.locator('code');
    await expect(runCode).toBeVisible();
    await expect(runCode).toContainText('./mirdb -c');

    // Verify telnet code block is visible with basic operations
    const telnetCodeBlock = page.locator('[data-testid="telnet-code-block"]');
    await expect(telnetCodeBlock).toBeVisible();
    const telnetCode = telnetCodeBlock.locator('code');
    await expect(telnetCode).toBeVisible();
    await expect(telnetCode).toContainText('telnet localhost 12333');
    await expect(telnetCode).toContainText('set mykey');
    await expect(telnetCode).toContainText('get mykey');
    await expect(telnetCode).toContainText('delete mykey');

    // Verify configuration code block is visible
    const configCodeBlock = page.locator('[data-testid="config-toml-block"]');
    await expect(configCodeBlock).toBeVisible();
    const configCode = configCodeBlock.locator('code');
    await expect(configCode).toBeVisible();
    await expect(configCode).toContainText('addr = "0.0.0.0:12333"');

    // Note: Copy buttons are present but won't function without JS
    const copyButtons = page.locator('.copy-button');
    const count = await copyButtons.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Navigation links function using anchor tags', async ({ page }) => {
    // Test Case 4: Test navigation links with JS disabled
    // Expected: Navigation links function using anchor tags

    // Verify navigation header is visible
    const navHeader = page.locator('[data-testid="navigation-header"]');
    await expect(navHeader).toBeVisible();

    // Verify nav links are present and contain correct href
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();

    // Test Features navigation link
    const featuresLink = page.locator('[data-testid="nav-link-features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');

    // Test Architecture navigation link
    const architectureLink = page.locator('[data-testid="nav-link-architecture"]');
    await expect(architectureLink).toBeVisible();
    await expect(architectureLink).toHaveAttribute('href', '#architecture');

    // Test Commands navigation link
    const commandsLink = page.locator('[data-testid="nav-link-commands"]');
    await expect(commandsLink).toBeVisible();
    await expect(commandsLink).toHaveAttribute('href', '#commands');

    // Test Quick Start navigation link
    const quickstartLink = page.locator('[data-testid="nav-link-quickstart"]');
    await expect(quickstartLink).toBeVisible();
    await expect(quickstartLink).toHaveAttribute('href', '#quickstart');

    // Test Configuration navigation link
    const configLink = page.locator('[data-testid="nav-link-configuration"]');
    await expect(configLink).toBeVisible();
    await expect(configLink).toHaveAttribute('href', '#configuration');

    // Test external GitHub link
    const githubLink = page.locator('[data-testid="nav-link-github"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Test external Docs link
    const docsLink = page.locator('[data-testid="nav-link-docs"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');

    // Test clicking a navigation link (anchor navigation works without JS)
    await featuresLink.click();

    // Verify the URL hash changed
    await expect(page).toHaveURL(/#features$/);

    // Verify the features section is in view (scroll should work without JS)
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Architecture section is visible without JavaScript', async ({ page }) => {
    // Additional verification for architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify architecture diagram is visible
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Verify SVG diagram is present
    const svgDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(svgDiagram).toBeVisible();

    // Verify architecture explanation is visible
    const explanation = page.locator('[data-testid="architecture-explanation"]');
    await expect(explanation).toBeVisible();

    // Verify architecture benefits list is visible
    const benefits = page.locator('[data-testid="architecture-benefits"]');
    await expect(benefits).toBeVisible();
  });

  test('Commands section is visible without JavaScript', async ({ page }) => {
    // Verify commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify commands grid is visible
    const commandsGrid = page.locator('[data-testid="commands-grid"]');
    await expect(commandsGrid).toBeVisible();

    // Verify individual command cards are visible
    const commands = ['set', 'get', 'add', 'replace', 'append', 'prepend', 'delete'];
    for (const cmd of commands) {
      const cmdCard = page.locator(`[data-testid="command-${cmd}"]`);
      await expect(cmdCard).toBeVisible();
    }

    // Verify documentation link is visible
    const docsLink = page.locator('[data-testid="commands-docs-link"]');
    await expect(docsLink).toBeVisible();
  });

  test('Footer is visible without JavaScript', async ({ page }) => {
    // Verify footer section
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer description
    const footerDesc = page.locator('[data-testid="footer-description"]');
    await expect(footerDesc).toBeVisible();
    await expect(footerDesc).toContainText('MirDB');

    // Verify footer links
    const footerLinks = page.locator('[data-testid="footer-links"]');
    await expect(footerLinks).toBeVisible();

    // Verify GitHub link
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify Contributing link
    const contributingLink = page.locator('[data-testid="footer-contributing-link"]');
    await expect(contributingLink).toBeVisible();

    // Verify License link
    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();
  });

  test('Main content area is visible without JavaScript', async ({ page }) => {
    // Verify main content wrapper is visible
    const mainContent = page.locator('[data-testid="main-content"]');
    await expect(mainContent).toBeVisible();

    // Verify all major sections within main are visible
    const sections = ['#features', '#architecture', '#commands', '#quickstart', '#configuration'];
    for (const section of sections) {
      const sectionElement = page.locator(section);
      await expect(sectionElement).toBeVisible();
    }
  });
});
