import { test, expect } from '@playwright/test';

test.describe('Architecture Section - Scenario 5', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for architecture diagram image or SVG', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check for visual diagram element (SVG in this case)
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify SVG element is present
    const svgDiagram = diagramContainer.locator('svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify SVG has accessible role and label
    await expect(svgDiagram).toHaveAttribute('role', 'img');
    await expect(svgDiagram).toHaveAttribute('aria-label', /LSM Tree Data Flow/i);
  });

  test('TC2: Check diagram mentions WAL', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check diagram or surrounding text mentions WAL or Write-Ahead Log
    const walInDiagram = architectureSection.locator('svg text', { hasText: /WAL/i });
    await expect(walInDiagram.first()).toBeVisible();

    // Also check for full "Write-Ahead Log" text in SVG or explanation
    const writeAheadLogText = architectureSection.getByText(/Write-Ahead Log/i);
    await expect(writeAheadLogText.first()).toBeVisible();
  });

  test('TC3: Check diagram mentions Memtable', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check diagram mentions Memtable
    const memtableInDiagram = architectureSection.locator('svg text', { hasText: /Memtable/i });
    await expect(memtableInDiagram.first()).toBeVisible();

    // Also check for Memtable in the explanation text
    const memtableExplanation = architectureSection.locator('[data-testid="architecture-explanation"]');
    await expect(memtableExplanation.getByText(/Memtable/i).first()).toBeVisible();
  });

  test('TC4: Check diagram mentions SSTable levels', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check diagram mentions SSTable
    const sstableInDiagram = architectureSection.locator('svg text', { hasText: /SSTable/i });
    await expect(sstableInDiagram.first()).toBeVisible();

    // Check for levels mention (Level 0, Level 1+) in diagram
    const levelInDiagram = architectureSection.locator('svg text', { hasText: /Level/i });
    await expect(levelInDiagram.first()).toBeVisible();

    // Check for compaction mention in diagram or explanation
    const compactionText = architectureSection.getByText(/compaction/i);
    await expect(compactionText.first()).toBeVisible();
  });

  test('TC5: Check for LSM tree explanation text', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check for LSM tree mention
    const lsmTreeText = architectureSection.getByText(/LSM/i);
    await expect(lsmTreeText.first()).toBeVisible();

    // Check for benefits explanation
    const explanationSection = architectureSection.locator('[data-testid="architecture-explanation"]');
    await expect(explanationSection).toBeVisible();

    // Verify explanation mentions benefits
    const benefitsHeading = explanationSection.getByText(/Benefits/i);
    await expect(benefitsHeading).toBeVisible();

    // Check for specific benefit mentions
    const writeThroughputBenefit = explanationSection.getByText(/write throughput/i);
    await expect(writeThroughputBenefit).toBeVisible();

    // Verify "How It Works" section exists
    const howItWorksHeading = explanationSection.getByText(/How It Works/i);
    await expect(howItWorksHeading).toBeVisible();
  });
});
