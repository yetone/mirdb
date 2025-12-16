// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Architecture Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test.describe('Test Case 1: Architecture section exists', () => {
        test('should display the architecture section on the page', async ({ page }) => {
            const architectureSection = page.locator('[data-testid="architecture-section"]');
            await expect(architectureSection).toBeVisible();
        });

        test('should have proper heading', async ({ page }) => {
            const heading = page.locator('[data-testid="architecture-heading"]');
            await expect(heading).toBeVisible();
            await expect(heading).toHaveText('Architecture');
        });

        test('should have a description explaining the architecture', async ({ page }) => {
            const description = page.locator('[data-testid="architecture-description"]');
            await expect(description).toBeVisible();
            await expect(description).toContainText('LSM-tree');
            await expect(description).toContainText('Log-Structured Merge-tree');
        });

        test('should be accessible via navigation link', async ({ page }) => {
            // Check desktop navigation has architecture link
            const navLink = page.locator('.nav-links a[href="#architecture"]');
            await expect(navLink).toBeVisible();
            await expect(navLink).toHaveText('Architecture');
        });

        test('should scroll to architecture section when nav link is clicked', async ({ page }) => {
            const navLink = page.locator('.nav-links a[href="#architecture"]');
            await navLink.click();

            // Wait for scroll animation
            await page.waitForTimeout(500);

            const architectureSection = page.locator('[data-testid="architecture-section"]');
            await expect(architectureSection).toBeInViewport();
        });
    });

    test.describe('Test Case 2: Visual diagram is present', () => {
        test('should display the architecture diagram container', async ({ page }) => {
            const diagram = page.locator('[data-testid="architecture-diagram"]');
            await expect(diagram).toBeVisible();
        });

        test('should contain an SVG diagram', async ({ page }) => {
            const svg = page.locator('[data-testid="architecture-svg"]');
            await expect(svg).toBeVisible();
        });

        test('should have proper ARIA role for accessibility', async ({ page }) => {
            const diagram = page.locator('[data-testid="architecture-diagram"]');
            await expect(diagram).toHaveAttribute('role', 'img');
        });

        test('should have ARIA label describing the diagram', async ({ page }) => {
            const diagram = page.locator('[data-testid="architecture-diagram"]');
            const ariaLabel = await diagram.getAttribute('aria-label');
            expect(ariaLabel).toContain('LSM-tree');
            expect(ariaLabel).toContain('architecture');
        });

        test('should display WAL component in diagram', async ({ page }) => {
            const walBox = page.locator('[data-testid="wal-box"]');
            await expect(walBox).toBeVisible();

            const walText = page.locator('[data-testid="wal-text"]');
            await expect(walText).toBeVisible();
        });

        test('should display Memtable component in diagram', async ({ page }) => {
            const memtableBox = page.locator('[data-testid="memtable-box"]');
            await expect(memtableBox).toBeVisible();

            const memtableText = page.locator('[data-testid="memtable-text"]');
            await expect(memtableText).toBeVisible();
        });

        test('should display Immutable Memtable component in diagram', async ({ page }) => {
            const immutableBox = page.locator('[data-testid="immutable-memtable-box"]');
            await expect(immutableBox).toBeVisible();

            const immutableText = page.locator('[data-testid="immutable-text"]');
            await expect(immutableText).toBeVisible();
        });

        test('should display SSTable levels in diagram', async ({ page }) => {
            const sstableSection = page.locator('[data-testid="sstable-section"]');
            await expect(sstableSection).toBeVisible();

            const level0 = page.locator('[data-testid="level-0-box"]');
            const level1 = page.locator('[data-testid="level-1-box"]');
            const level2 = page.locator('[data-testid="level-2-box"]');

            await expect(level0).toBeVisible();
            await expect(level1).toBeVisible();
            await expect(level2).toBeVisible();
        });

        test('should show compaction flow labels', async ({ page }) => {
            const minorCompaction = page.locator('[data-testid="minor-compaction-label"]');
            const majorCompaction = page.locator('[data-testid="major-compaction-label"]');

            await expect(minorCompaction).toBeVisible();
            await expect(majorCompaction).toBeVisible();
        });

        test('should show write and read paths', async ({ page }) => {
            const writeLabel = page.locator('[data-testid="write-label"]');
            const readLabel = page.locator('[data-testid="read-label"]');

            await expect(writeLabel).toBeVisible();
            await expect(readLabel).toBeVisible();
        });
    });

    test.describe('Test Case 3: LSM-tree explanation', () => {
        test('should display the explanation section', async ({ page }) => {
            const explanation = page.locator('[data-testid="architecture-explanation"]');
            await expect(explanation).toBeVisible();
        });

        test('should have How It Works heading', async ({ page }) => {
            const title = page.locator('.explanation-title');
            await expect(title).toBeVisible();
            await expect(title).toHaveText('How It Works');
        });

        test('should explain WAL (Write-Ahead Log)', async ({ page }) => {
            const walItem = page.locator('[data-testid="explanation-item-wal"]');
            await expect(walItem).toBeVisible();
            await expect(walItem).toContainText('Write-Ahead Log');
            await expect(walItem).toContainText('durability');
        });

        test('should explain Memtable', async ({ page }) => {
            const memtableItem = page.locator('[data-testid="explanation-item-memtable"]');
            await expect(memtableItem).toBeVisible();
            await expect(memtableItem).toContainText('Memtable');
            await expect(memtableItem).toContainText('skip list');
        });

        test('should explain Compaction', async ({ page }) => {
            const compactionItem = page.locator('[data-testid="explanation-item-compaction"]');
            await expect(compactionItem).toBeVisible();
            await expect(compactionItem).toContainText('Compaction');
            await expect(compactionItem).toContainText('Minor compaction');
            await expect(compactionItem).toContainText('Major compaction');
        });

        test('should explain SSTable Levels', async ({ page }) => {
            const sstableItem = page.locator('[data-testid="explanation-item-sstable"]');
            await expect(sstableItem).toBeVisible();
            await expect(sstableItem).toContainText('SSTable');
            await expect(sstableItem).toContainText('levels');
        });

        test('should mention Cuckoo filter', async ({ page }) => {
            const sstableItem = page.locator('[data-testid="explanation-item-sstable"]');
            await expect(sstableItem).toContainText('Cuckoo filter');
        });
    });

    test.describe('Test Case 4: Diagram responsiveness', () => {
        test('should display correctly on desktop (1280px)', async ({ page }) => {
            await page.setViewportSize({ width: 1280, height: 800 });

            const architectureSection = page.locator('[data-testid="architecture-section"]');
            await expect(architectureSection).toBeVisible();

            const diagram = page.locator('[data-testid="architecture-diagram"]');
            await expect(diagram).toBeVisible();

            const explanation = page.locator('[data-testid="architecture-explanation"]');
            await expect(explanation).toBeVisible();

            // On desktop, diagram and explanation should be side by side
            const diagramBox = await diagram.boundingBox();
            const explanationBox = await explanation.boundingBox();

            expect(diagramBox).not.toBeNull();
            expect(explanationBox).not.toBeNull();

            // Diagram should be on the left of explanation on desktop
            expect(diagramBox.x).toBeLessThan(explanationBox.x);
        });

        test('should display correctly on tablet (768px)', async ({ page }) => {
            await page.setViewportSize({ width: 768, height: 1024 });

            const architectureSection = page.locator('[data-testid="architecture-section"]');
            await expect(architectureSection).toBeVisible();

            const diagram = page.locator('[data-testid="architecture-diagram"]');
            await expect(diagram).toBeVisible();

            const svg = page.locator('[data-testid="architecture-svg"]');
            await expect(svg).toBeVisible();
        });

        test('should display correctly on mobile (375px)', async ({ page }) => {
            await page.setViewportSize({ width: 375, height: 667 });

            const architectureSection = page.locator('[data-testid="architecture-section"]');
            await expect(architectureSection).toBeVisible();

            const diagram = page.locator('[data-testid="architecture-diagram"]');
            await expect(diagram).toBeVisible();

            const explanation = page.locator('[data-testid="architecture-explanation"]');
            await expect(explanation).toBeVisible();

            // On mobile, diagram and explanation should be stacked
            const diagramBox = await diagram.boundingBox();
            const explanationBox = await explanation.boundingBox();

            expect(diagramBox).not.toBeNull();
            expect(explanationBox).not.toBeNull();

            // Explanation should be below diagram on mobile
            expect(explanationBox.y).toBeGreaterThan(diagramBox.y);
        });

        test('should not cause horizontal scroll on mobile', async ({ page }) => {
            await page.setViewportSize({ width: 375, height: 667 });

            // Check that the page doesn't have horizontal overflow
            const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
            const windowWidth = await page.evaluate(() => window.innerWidth);

            // Allow for a small margin of error (e.g., scrollbar width)
            expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 20);
        });

        test('should allow horizontal scroll on diagram container for small screens', async ({ page }) => {
            await page.setViewportSize({ width: 375, height: 667 });

            const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
            const overflowX = await diagramContainer.evaluate(el =>
                window.getComputedStyle(el).overflowX
            );

            expect(overflowX).toBe('auto');
        });

        test('should maintain SVG readability at different sizes', async ({ page }) => {
            // Test at desktop size
            await page.setViewportSize({ width: 1280, height: 800 });

            const svg = page.locator('[data-testid="architecture-svg"]');
            await expect(svg).toBeVisible();

            const svgBox = await svg.boundingBox();
            expect(svgBox).not.toBeNull();
            expect(svgBox.width).toBeGreaterThan(300);
            expect(svgBox.height).toBeGreaterThan(200);
        });

        test('should have mobile navigation link for architecture', async ({ page }) => {
            await page.setViewportSize({ width: 375, height: 667 });

            // Open mobile menu
            const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
            await menuToggle.click();

            // Check mobile nav has architecture link
            const mobileArchLink = page.locator('.mobile-nav-links a[href="#architecture"]');
            await expect(mobileArchLink).toBeVisible();
        });
    });
});
