/**
 * No-JavaScript Fallback E2E Tests
 * Owner: Scenario 12 - No-JavaScript Basic Display
 *
 * Tests for:
 * - Progressive enhancement verification
 * - Content visibility without JavaScript
 * - Navigation functionality without JavaScript
 * - Code block display without syntax highlighting
 */
import { test, expect } from '@playwright/test';
import { SELECTORS, CONTENT, FEATURES_DATA } from '../fixtures/test-data';

test.describe('No-JavaScript Basic Display', () => {
  test.use({ javaScriptEnabled: false });

  test('TC1: Hero section content is visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify hero section is visible
    const heroSection = page.locator(SELECTORS.hero);
    await expect(heroSection).toBeVisible();

    // Verify MirDB logo/title area is visible
    const heroLogo = page.locator(SELECTORS.heroLogo);
    await expect(heroLogo).toBeVisible();

    // Verify hero title is visible with product name
    const heroTitle = page.locator(SELECTORS.heroTitle);
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText(CONTENT.productName);

    // Verify tagline is visible
    const heroTagline = page.locator(SELECTORS.heroTagline);
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText(CONTENT.tagline);

    // Verify CTA buttons are visible (links should work without JS)
    const ctaGetStarted = page.locator(SELECTORS.ctaGetStarted);
    await expect(ctaGetStarted).toBeVisible();
    await expect(ctaGetStarted).toContainText(CONTENT.ctaGetStartedText);

    const ctaGitHub = page.locator(SELECTORS.ctaGitHub);
    await expect(ctaGitHub).toBeVisible();
    await expect(ctaGitHub).toContainText(CONTENT.ctaGitHubText);

    // Verify the hero description is visible
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();
  });

  test('TC2: Features section content is visible and readable without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify features section is visible
    const featuresSection = page.locator(SELECTORS.features);
    await expect(featuresSection).toBeVisible();

    // Verify features title is visible
    const featuresTitle = page.locator(SELECTORS.featuresTitle);
    await expect(featuresTitle).toBeVisible();
    await expect(featuresTitle).toContainText(CONTENT.featuresSectionTitle);

    // Verify features grid is visible
    const featuresGrid = page.locator(SELECTORS.featuresGrid);
    await expect(featuresGrid).toBeVisible();

    // Verify all 6 feature cards are visible and readable
    const featureCards = page.locator(SELECTORS.featureCard);
    await expect(featureCards).toHaveCount(FEATURES_DATA.length);

    // Verify each feature card has visible content
    for (const feature of FEATURES_DATA) {
      // Scope to features section to avoid matching status section items
      const card = page.locator(`${SELECTORS.features} [data-feature="${feature.id}"]`);
      await expect(card).toBeVisible();

      // Verify feature title is visible
      const cardTitle = card.locator(SELECTORS.featureTitle);
      await expect(cardTitle).toBeVisible();
      await expect(cardTitle).toContainText(feature.title);

      // Verify feature description is visible
      const cardDescription = card.locator(SELECTORS.featureDescription);
      await expect(cardDescription).toBeVisible();
    }

    // Verify feature icons are present (SVG elements)
    const featureIcons = page.locator(`${SELECTORS.featureCard} ${SELECTORS.featureIcon}`);
    await expect(featureIcons).toHaveCount(FEATURES_DATA.length);
  });

  test('TC3: Code examples are visible without JavaScript (may lack syntax highlighting)', async ({ page }) => {
    await page.goto('/');

    // Navigate to quick-start section to verify code blocks
    const quickStartSection = page.locator(SELECTORS.quickStart);
    await expect(quickStartSection).toBeVisible();

    // Verify all code blocks are visible
    const codeBlocks = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeBlock}`);
    await expect(codeBlocks).toHaveCount(3); // Installation, Server Start, Usage

    // Verify code content is visible in each block
    for (let i = 0; i < 3; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify pre/code elements are visible
      const codeContent = codeBlock.locator(SELECTORS.codeContent);
      await expect(codeContent).toBeVisible();

      // Verify code text is not empty
      const codeText = await codeContent.textContent();
      expect(codeText).toBeTruthy();
      expect(codeText!.length).toBeGreaterThan(0);
    }

    // Verify code header labels are visible
    const codeLanguages = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeLanguage}`);
    await expect(codeLanguages).toHaveCount(3);

    // Verify specific code content is readable
    // Check installation command is visible
    const installCodeBlock = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeContent}`).first();
    await expect(installCodeBlock).toContainText('cargo install mirdb');

    // Check protocol section code examples are also visible
    const protocolSection = page.locator(SELECTORS.protocol);
    await expect(protocolSection).toBeVisible();

    const syntaxExamples = page.locator(`${SELECTORS.protocol} ${SELECTORS.syntaxExample}`);
    await expect(syntaxExamples).toHaveCount(3); // SET, GET, DELETE examples

    // Verify each syntax example has visible code
    for (let i = 0; i < 3; i++) {
      const syntaxExample = syntaxExamples.nth(i);
      await expect(syntaxExample).toBeVisible();

      const codeBlock = syntaxExample.locator('pre.code-block');
      await expect(codeBlock).toBeVisible();
    }
  });

  test('TC4: Anchor navigation links work without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify header navigation is visible
    const header = page.locator(SELECTORS.header);
    await expect(header).toBeVisible();

    const nav = page.locator(SELECTORS.nav);
    await expect(nav).toBeVisible();

    // Get all navigation links
    const navLinks = page.locator(SELECTORS.navLink);
    await expect(navLinks).toHaveCount(5); // Features, Quick Start, Architecture, Protocol, Status

    // Test each navigation link has correct href (anchor links work without JS)
    const expectedAnchors = ['#features', '#quick-start', '#architecture', '#protocol', '#status'];

    for (let i = 0; i < expectedAnchors.length; i++) {
      const link = navLinks.nth(i);
      await expect(link).toHaveAttribute('href', expectedAnchors[i]);
    }

    // Test clicking Features link navigates to features section
    await navLinks.filter({ hasText: 'Features' }).click();
    await expect(page).toHaveURL(/#features$/);

    // Verify the features section exists at the anchor target
    const featuresSection = page.locator(SELECTORS.features);
    await expect(featuresSection).toBeVisible();

    // Test Quick Start link
    await page.goto('/');
    await navLinks.filter({ hasText: 'Quick Start' }).click();
    await expect(page).toHaveURL(/#quick-start$/);

    const quickStartSection = page.locator(SELECTORS.quickStart);
    await expect(quickStartSection).toBeVisible();

    // Test Get Started button (CTA) navigates to quick-start
    await page.goto('/');
    const ctaGetStarted = page.locator(SELECTORS.ctaGetStarted);
    await expect(ctaGetStarted).toHaveAttribute('href', '#quick-start');

    await ctaGetStarted.click();
    await expect(page).toHaveURL(/#quick-start$/);

    // Test Architecture link
    await page.goto('/');
    await navLinks.filter({ hasText: 'Architecture' }).click();
    await expect(page).toHaveURL(/#architecture$/);

    // Test Protocol link
    await page.goto('/');
    await navLinks.filter({ hasText: 'Protocol' }).click();
    await expect(page).toHaveURL(/#protocol$/);

    // Test Status link
    await page.goto('/');
    await navLinks.filter({ hasText: 'Status' }).click();
    await expect(page).toHaveURL(/#status$/);
  });

  test('All main content sections are visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify all main sections are present and visible
    const sections = [
      { selector: SELECTORS.hero, name: 'Hero' },
      { selector: SELECTORS.features, name: 'Features' },
      { selector: SELECTORS.quickStart, name: 'Quick Start' },
      { selector: SELECTORS.architecture, name: 'Architecture' },
      { selector: SELECTORS.protocol, name: 'Protocol' },
      { selector: SELECTORS.status, name: 'Status' },
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element, `${section.name} section should be visible`).toBeVisible();
    }

    // Verify header and footer are visible
    await expect(page.locator(SELECTORS.header)).toBeVisible();
    await expect(page.locator(SELECTORS.footer)).toBeVisible();
  });

  test('Architecture diagram content is visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Architecture section should be visible
    const architectureSection = page.locator(SELECTORS.architecture);
    await expect(architectureSection).toBeVisible();

    // Mermaid diagram container should be visible (content may not render without JS)
    const mermaidContainer = page.locator(SELECTORS.mermaidDiagram);
    await expect(mermaidContainer).toBeVisible();

    // Component cards should be visible
    const componentCards = page.locator(SELECTORS.componentCard);
    await expect(componentCards).toHaveCount(4); // WAL, Memtable, Immutable, SSTable

    // Verify component titles and descriptions are readable
    const componentTitles = page.locator(SELECTORS.componentTitle);
    await expect(componentTitles).toHaveCount(4);

    const componentDescriptions = page.locator(SELECTORS.componentDescription);
    await expect(componentDescriptions).toHaveCount(4);
  });

  test('Project status section displays implemented and planned features without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Status section should be visible
    const statusSection = page.locator(SELECTORS.status);
    await expect(statusSection).toBeVisible();

    // Implemented features list should be visible
    const implementedList = page.locator('#implemented-features');
    await expect(implementedList).toBeVisible();

    // Should have 8 implemented features
    const implementedItems = page.locator('#implemented-features .status-item');
    await expect(implementedItems).toHaveCount(8);

    // Planned features list should be visible
    const plannedList = page.locator('#planned-features');
    await expect(plannedList).toBeVisible();

    // Should have 3 planned features
    const plannedItems = page.locator('#planned-features .status-item');
    await expect(plannedItems).toHaveCount(3);
  });

  test('Footer links are functional without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Footer should be visible
    const footer = page.locator(SELECTORS.footer);
    await expect(footer).toBeVisible();

    // Footer links should be visible and have correct attributes
    const githubLink = page.locator(SELECTORS.footerGithubLink);
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/akiozihao/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');

    const issuesLink = page.locator(SELECTORS.footerIssuesLink);
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', 'https://github.com/akiozihao/mirdb/issues');

    const contributingLink = page.locator(SELECTORS.footerContributingLink);
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveAttribute('href', 'https://github.com/akiozihao/mirdb/blob/main/CONTRIBUTING.md');

    // Copyright text should be visible
    const copyright = page.locator(SELECTORS.footerCopyright);
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MIT License');
  });
});
