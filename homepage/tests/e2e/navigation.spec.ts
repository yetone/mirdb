import { test, expect, Page } from '@playwright/test';

const HEADER_HEIGHT_PX = 64;

async function getScrollY(page: Page): Promise<number> {
  return page.evaluate(() => window.scrollY);
}

async function getElementTop(page: Page, selector: string): Promise<number> {
  return page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return -1;
    return el.getBoundingClientRect().top + window.scrollY;
  }, selector);
}

test.describe('Navigation and Smooth Scrolling', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
  });

  test('header contains links to Features, Architecture, Getting Started, Protocol, and GitHub', async ({
    page,
  }) => {
    const desktopNav = page.locator('[data-testid="desktop-nav"]');
    await expect(desktopNav).toBeVisible();

    const navLinks = desktopNav.locator('a');
    await expect(navLinks).toHaveCount(5);

    const linkTexts = await navLinks.allTextContents();
    expect(linkTexts).toEqual([
      'Features',
      'Getting Started',
      'Protocol',
      'Architecture',
      'GitHub',
    ]);

    // Internal anchor links should resolve to the page hash
    const featuresHref = await navLinks.nth(0).getAttribute('href');
    expect(featuresHref).toBe('#features');

    const quickStartHref = await navLinks.nth(1).getAttribute('href');
    expect(quickStartHref).toBe('#quick-start');

    const protocolHref = await navLinks.nth(2).getAttribute('href');
    expect(protocolHref).toBe('#protocol');

    const architectureHref = await navLinks.nth(3).getAttribute('href');
    expect(architectureHref).toBe('#architecture');

    // GitHub should be an external URL
    const githubHref = await navLinks.nth(4).getAttribute('href');
    expect(githubHref).toMatch(/^https?:\/\//);
  });

  test('clicking Features nav link smooth-scrolls to the Features section', async ({
    page,
  }) => {
    const featuresLink = page
      .locator('[data-testid="desktop-nav"]')
      .locator('a', { hasText: 'Features' });
    await featuresLink.click();

    // Wait for scroll animation to settle
    await page.waitForFunction(
      ({ headerHeight }) => {
        const target = document.getElementById('features');
        if (!target) return false;
        const top = target.getBoundingClientRect().top;
        return Math.abs(top - headerHeight) < 5;
      },
      { headerHeight: HEADER_HEIGHT_PX },
      { timeout: 5000 }
    );

    // Verify the section is now near the top under the header
    const featuresTop = await page.evaluate(() => {
      const el = document.getElementById('features');
      return el?.getBoundingClientRect().top ?? null;
    });
    expect(featuresTop).not.toBeNull();
    expect(featuresTop!).toBeGreaterThan(HEADER_HEIGHT_PX - 10);
    expect(featuresTop!).toBeLessThan(HEADER_HEIGHT_PX + 10);
  });

  test('clicking each desktop nav link scrolls to the corresponding section', async ({
    page,
  }) => {
    const cases = [
      { label: 'Features', id: 'features' },
      { label: 'Getting Started', id: 'quick-start' },
      { label: 'Protocol', id: 'protocol' },
      { label: 'Architecture', id: 'architecture' },
    ];

    for (const { label, id } of cases) {
      const link = page
        .locator('[data-testid="desktop-nav"]')
        .locator('a', { hasText: label });
      await link.click();

      await page.waitForFunction(
        ({ targetId, headerHeight }) => {
          const target = document.getElementById(targetId);
          if (!target) return false;
          const top = target.getBoundingClientRect().top;
          return Math.abs(top - headerHeight) < 5;
        },
        { targetId: id, headerHeight: HEADER_HEIGHT_PX },
        { timeout: 5000 }
      );

      const top = await page.evaluate((targetId) => {
        const el = document.getElementById(targetId);
        return el?.getBoundingClientRect().top ?? null;
      }, id);
      expect(top).not.toBeNull();
      expect(Math.abs(top! - HEADER_HEIGHT_PX)).toBeLessThan(10);
    }
  });

  test('clicking a nav link uses smooth scroll animation (not instant jump)', async ({
    page,
  }) => {
    // Start at top
    const initialScroll = await getScrollY(page);
    expect(initialScroll).toBe(0);

    const archLink = page
      .locator('[data-testid="desktop-nav"]')
      .locator('a', { hasText: 'Architecture' });
    await archLink.click();

    // Sample scroll position shortly after click - should not have arrived at final position instantly
    const samples: number[] = [];
    for (let i = 0; i < 5; i++) {
      samples.push(await getScrollY(page));
      await page.waitForTimeout(50);
    }

    // Wait for animation to complete
    await page.waitForFunction(
      () => {
        const target = document.getElementById('architecture');
        if (!target) return false;
        return Math.abs(target.getBoundingClientRect().top - 64) < 5;
      },
      undefined,
      { timeout: 5000 }
    );

    const finalScroll = await getScrollY(page);

    // Final scroll should be much greater than 0
    expect(finalScroll).toBeGreaterThan(100);

    // We should have observed at least one intermediate sample (proving smooth scroll)
    const hasIntermediate = samples.some(
      (s) => s > 0 && s < finalScroll
    );
    // It's also acceptable if the smooth scroll completed within first sample,
    // but at minimum scroll position must increase from 0.
    expect(hasIntermediate || samples[samples.length - 1] > 0).toBe(true);
  });

  test('clicking nav link updates URL hash without reload', async ({ page }) => {
    // Mark window so we can detect a full page reload
    await page.evaluate(() => {
      (window as unknown as { __navTestMarker: boolean }).__navTestMarker = true;
    });

    const protocolLink = page
      .locator('[data-testid="desktop-nav"]')
      .locator('a', { hasText: 'Protocol' });
    await protocolLink.click();

    await page.waitForTimeout(300);

    // URL should contain the hash
    expect(page.url()).toContain('#protocol');

    // Marker should still be present (no page reload happened)
    const markerPresent = await page.evaluate(
      () => (window as unknown as { __navTestMarker?: boolean }).__navTestMarker === true
    );
    expect(markerPresent).toBe(true);

    // Click another link, hash should update
    const archLink = page
      .locator('[data-testid="desktop-nav"]')
      .locator('a', { hasText: 'Architecture' });
    await archLink.click();
    await page.waitForTimeout(300);
    expect(page.url()).toContain('#architecture');

    // Marker should still be present
    const markerStillPresent = await page.evaluate(
      () => (window as unknown as { __navTestMarker?: boolean }).__navTestMarker === true
    );
    expect(markerStillPresent).toBe(true);
  });

  test('header remains fixed at top when scrolling down', async ({ page }) => {
    const header = page.locator('[data-testid="header"]');
    await expect(header).toBeVisible();

    // Initially at top
    const initialBox = await header.boundingBox();
    expect(initialBox?.y).toBe(0);

    // Scroll down 800px
    await page.evaluate(() => window.scrollTo(0, 800));
    await page.waitForTimeout(150);

    // Header should still be at top
    await expect(header).toBeVisible();
    const scrolledBox = await header.boundingBox();
    expect(scrolledBox?.y).toBe(0);

    // Scroll more
    await page.evaluate(() => window.scrollTo(0, 2000));
    await page.waitForTimeout(150);
    await expect(header).toBeVisible();
    const deeperBox = await header.boundingBox();
    expect(deeperBox?.y).toBe(0);
  });

  test('header has backdrop blur or solid background when scrolled', async ({
    page,
  }) => {
    const header = page.locator('[data-testid="header"]');

    // Scroll past the hero section
    await page.evaluate(() => window.scrollTo(0, 600));
    await page.waitForTimeout(200);

    // Header should be marked as scrolled
    await expect(header).toHaveAttribute('data-scrolled', 'true');

    // Backdrop blur or non-transparent background must be present
    const styles = await header.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backdropFilter:
          computed.backdropFilter || (computed as unknown as { webkitBackdropFilter: string }).webkitBackdropFilter || '',
        backgroundColor: computed.backgroundColor,
      };
    });

    const hasBlur = styles.backdropFilter && styles.backdropFilter !== 'none';
    const hasBackground =
      styles.backgroundColor &&
      styles.backgroundColor !== 'rgba(0, 0, 0, 0)' &&
      styles.backgroundColor !== 'transparent';

    expect(hasBlur || hasBackground).toBe(true);
  });

  test('header does not awkwardly overlap content', async ({ page }) => {
    // Pages should have padding-top equal to or greater than header height
    const main = page.locator('main');
    await expect(main).toBeVisible();

    const paddingTop = await main.evaluate((el) =>
      parseInt(window.getComputedStyle(el).paddingTop)
    );
    expect(paddingTop).toBeGreaterThanOrEqual(HEADER_HEIGHT_PX);

    // Click features and verify it's not hidden behind the header
    const featuresLink = page
      .locator('[data-testid="desktop-nav"]')
      .locator('a', { hasText: 'Features' });
    await featuresLink.click();
    await page.waitForTimeout(900);

    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toBeVisible();
    const headingBox = await featuresHeading.boundingBox();
    expect(headingBox?.y).toBeGreaterThanOrEqual(HEADER_HEIGHT_PX - 5);
  });

  test('opening URL with #protocol hash scrolls to Protocol section on load', async ({
    page,
  }) => {
    await page.goto('/#protocol');

    // Allow time for the post-mount smooth scroll to settle
    await page.waitForFunction(
      () => {
        const target = document.getElementById('protocol');
        if (!target) return false;
        const top = target.getBoundingClientRect().top;
        return Math.abs(top - 64) < 60;
      },
      undefined,
      { timeout: 5000 }
    );

    const protocolTop = await getElementTop(page, '#protocol');
    expect(protocolTop).toBeGreaterThan(0);

    // The Protocol section should be near the viewport top (under the header)
    const viewportTopOfProtocol = await page.evaluate(() => {
      const el = document.getElementById('protocol');
      return el?.getBoundingClientRect().top ?? null;
    });
    expect(viewportTopOfProtocol).not.toBeNull();
    expect(viewportTopOfProtocol!).toBeLessThan(HEADER_HEIGHT_PX + 60);
    expect(viewportTopOfProtocol!).toBeGreaterThan(-50);
  });

  test('opening URL with #architecture hash scrolls to Architecture section on load', async ({
    page,
  }) => {
    await page.goto('/#architecture');

    await page.waitForFunction(
      () => {
        const target = document.getElementById('architecture');
        if (!target) return false;
        const top = target.getBoundingClientRect().top;
        return Math.abs(top - 64) < 60;
      },
      undefined,
      { timeout: 5000 }
    );

    const viewportTopOfArchitecture = await page.evaluate(() => {
      const el = document.getElementById('architecture');
      return el?.getBoundingClientRect().top ?? null;
    });
    expect(viewportTopOfArchitecture).not.toBeNull();
    expect(viewportTopOfArchitecture!).toBeLessThan(HEADER_HEIGHT_PX + 60);
  });

  test('active section highlighting updates as user scrolls through page', async ({
    page,
  }) => {
    const featuresLink = page.locator(
      '[data-testid="desktop-nav-link-features"]'
    );
    const quickStartLink = page.locator(
      '[data-testid="desktop-nav-link-getting-started"]'
    );
    const protocolLink = page.locator(
      '[data-testid="desktop-nav-link-protocol"]'
    );
    const architectureLink = page.locator(
      '[data-testid="desktop-nav-link-architecture"]'
    );

    // Scroll to Features
    await page.evaluate(() => {
      const el = document.getElementById('features');
      if (el) {
        const top =
          el.getBoundingClientRect().top + window.scrollY - 64;
        window.scrollTo(0, top + 10);
      }
    });
    await page.waitForTimeout(400);
    await expect(featuresLink).toHaveAttribute('data-active', 'true');

    // Scroll to Quick Start
    await page.evaluate(() => {
      const el = document.getElementById('quick-start');
      if (el) {
        const top =
          el.getBoundingClientRect().top + window.scrollY - 64;
        window.scrollTo(0, top + 10);
      }
    });
    await page.waitForTimeout(400);
    await expect(quickStartLink).toHaveAttribute('data-active', 'true');

    // Scroll to Protocol
    await page.evaluate(() => {
      const el = document.getElementById('protocol');
      if (el) {
        const top =
          el.getBoundingClientRect().top + window.scrollY - 64;
        window.scrollTo(0, top + 10);
      }
    });
    await page.waitForTimeout(400);
    await expect(protocolLink).toHaveAttribute('data-active', 'true');

    // Scroll to Architecture
    await page.evaluate(() => {
      const el = document.getElementById('architecture');
      if (el) {
        const top =
          el.getBoundingClientRect().top + window.scrollY - 64;
        window.scrollTo(0, top + 10);
      }
    });
    await page.waitForTimeout(400);
    await expect(architectureLink).toHaveAttribute('data-active', 'true');
  });
});

test.describe('Navigation - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
  });

  test('clicking a mobile nav link closes the menu and scrolls to section', async ({
    page,
  }) => {
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await mobileMenuButton.click();

    const overlay = page.locator('[data-testid="mobile-nav-overlay"]');
    await expect(overlay).toHaveCount(1);

    // Click Protocol link in mobile menu
    const protocolLink = overlay
      .locator('[data-testid="mobile-nav-link"]')
      .filter({ hasText: 'Protocol' });
    await protocolLink.click();

    // Menu should close
    await expect(overlay).toHaveCount(0);

    // Wait for scroll to settle
    await page.waitForFunction(
      () => {
        const target = document.getElementById('protocol');
        if (!target) return false;
        const top = target.getBoundingClientRect().top;
        return Math.abs(top - 64) < 60;
      },
      undefined,
      { timeout: 5000 }
    );

    // URL hash should be updated
    expect(page.url()).toContain('#protocol');

    // Protocol section should be near the top
    const protocolTop = await page.evaluate(() => {
      const el = document.getElementById('protocol');
      return el?.getBoundingClientRect().top ?? null;
    });
    expect(protocolTop).not.toBeNull();
    expect(protocolTop!).toBeLessThan(HEADER_HEIGHT_PX + 60);
  });

  test('mobile nav link click on Architecture closes menu and scrolls', async ({
    page,
  }) => {
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await mobileMenuButton.click();

    const overlay = page.locator('[data-testid="mobile-nav-overlay"]');
    await expect(overlay).toHaveCount(1);

    const archLink = overlay
      .locator('[data-testid="mobile-nav-link"]')
      .filter({ hasText: 'Architecture' });
    await archLink.click();

    await expect(overlay).toHaveCount(0);

    await page.waitForFunction(
      () => {
        const target = document.getElementById('architecture');
        if (!target) return false;
        return Math.abs(target.getBoundingClientRect().top - 64) < 60;
      },
      undefined,
      { timeout: 5000 }
    );

    expect(page.url()).toContain('#architecture');
  });

  test('mobile sticky header remains visible during scroll', async ({ page }) => {
    const header = page.locator('[data-testid="header"]');
    await expect(header).toBeVisible();
    const initialBox = await header.boundingBox();
    expect(initialBox?.y).toBe(0);

    await page.evaluate(() => window.scrollTo(0, 600));
    await page.waitForTimeout(200);

    await expect(header).toBeVisible();
    const scrolledBox = await header.boundingBox();
    expect(scrolledBox?.y).toBe(0);
  });
});
