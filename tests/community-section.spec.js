// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Community and Contribution Section
 * Scenario: Verify community/contribution section with links to issues and contribution guidelines
 * Related Requirements: REQ-10, US-5
 */

test.describe('Community and Contribution Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for GitHub repository link
   * Expected: Prominent link to GitHub repository is visible on the page
   */
  test('TC1: GitHub repository link is prominently visible', async ({ page }) => {
    // Verify the community section exists
    const communitySection = page.locator('#community');
    await expect(communitySection).toBeVisible();

    // Verify the GitHub repository card exists
    const githubCard = page.locator('[data-testid="github-repo-link"]');
    await expect(githubCard).toBeVisible();

    // Verify the GitHub link within the card
    const githubLink = githubCard.locator('a.btn');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('View on GitHub');

    // Verify the link points to the correct GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has noopener for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 2: Check for contribution guidelines link
   * Expected: Link to contribution guidelines or CONTRIBUTING.md is present
   */
  test('TC2: Contribution guidelines link is present', async ({ page }) => {
    // Verify the contributing card exists
    const contributingCard = page.locator('[data-testid="contributing-link"]');
    await expect(contributingCard).toBeVisible();

    // Verify the card has appropriate title
    const cardTitle = contributingCard.locator('.community-title');
    await expect(cardTitle).toHaveText('Contribution Guidelines');

    // Verify the link to CONTRIBUTING.md
    const contributingLink = contributingCard.locator('a.btn');
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveText('Read Guidelines');

    // Verify the link points to CONTRIBUTING.md
    const href = await contributingLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('CONTRIBUTING');

    // Verify it opens in a new tab
    const target = await contributingLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has noopener for security
    const rel = await contributingLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 3: Check for issues link
   * Expected: Link to GitHub issues for reporting bugs or suggesting features
   */
  test('TC3: GitHub issues link is present', async ({ page }) => {
    // Verify the issues card exists
    const issuesCard = page.locator('[data-testid="issues-link"]');
    await expect(issuesCard).toBeVisible();

    // Verify the card has appropriate title
    const cardTitle = issuesCard.locator('.community-title');
    await expect(cardTitle).toHaveText('Report Issues');

    // Verify the description mentions bug reporting
    const description = issuesCard.locator('.community-description');
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('bug');
    expect(descText.toLowerCase()).toContain('feature');

    // Verify the link to GitHub issues
    const issuesLink = issuesCard.locator('a.btn');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveText('Open an Issue');

    // Verify the link points to GitHub issues
    const href = await issuesLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('/issues');

    // Verify it opens in a new tab
    const target = await issuesLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has noopener for security
    const rel = await issuesLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 4: Check for project structure information
   * Expected: Brief information about project structure or link to detailed docs
   */
  test('TC4: Project structure information is present', async ({ page }) => {
    // Verify the project structure card exists
    const structureCard = page.locator('[data-testid="project-structure-link"]');
    await expect(structureCard).toBeVisible();

    // Verify the card has appropriate title
    const cardTitle = structureCard.locator('.community-title');
    await expect(cardTitle).toHaveText('Project Structure');

    // Verify the description mentions project structure details
    const description = structureCard.locator('.community-description');
    const descText = await description.textContent();
    // Should mention workspace organization or crates
    expect(descText.toLowerCase()).toContain('crate');

    // Verify the link to project structure documentation
    const structureLink = structureCard.locator('a.btn');
    await expect(structureLink).toBeVisible();
    await expect(structureLink).toHaveText('View Structure');

    // Verify the link points to project structure section
    const href = await structureLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('project-structure');

    // Verify it opens in a new tab
    const target = await structureLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has noopener for security
    const rel = await structureLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Additional test: Verify the community section is properly styled and accessible
   */
  test('Community section has proper structure and accessibility', async ({ page }) => {
    // Verify the section has a proper heading
    const sectionTitle = page.locator('#community .section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Community & Contributing');

    // Verify the introduction paragraph exists
    const intro = page.locator('#community .community-intro');
    await expect(intro).toBeVisible();
    const introText = await intro.textContent();
    expect(introText.toLowerCase()).toContain('open-source');
    expect(introText.toLowerCase()).toContain('contribution');

    // Verify all four cards are present in the grid
    const communityCards = page.locator('#community .community-card');
    await expect(communityCards).toHaveCount(4);

    // Verify each card has an icon (with aria-hidden for accessibility)
    const icons = page.locator('#community .community-icon[aria-hidden="true"]');
    await expect(icons).toHaveCount(4);
  });

  /**
   * Test navigation to community section
   */
  test('Navigation link to community section works', async ({ page }) => {
    // Find the community nav link
    const navLink = page.locator('.nav-links a[href="#community"]');
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveText('Community');

    // Click the nav link
    await navLink.click();

    // Verify we've scrolled to the community section
    const communitySection = page.locator('#community');
    await expect(communitySection).toBeInViewport();
  });
});
