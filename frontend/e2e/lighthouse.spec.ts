import { test, expect, chromium, Browser, Page, BrowserContext } from '@playwright/test';
import { playAudit } from 'playwright-lighthouse';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Lighthouse port for remote debugging
const LIGHTHOUSE_PORT = 9222;

// Score thresholds as per NFR-2 requirement (90+)
const SCORE_THRESHOLD = 90;

test.describe('Lighthouse Performance Audit', () => {
  let browser: Browser;
  let context: BrowserContext;
  let page: Page;

  test.beforeAll(async () => {
    // Launch browser with remote debugging port for Lighthouse
    browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });
    context = await browser.newContext();
    page = await context.newPage();
  });

  test.afterAll(async () => {
    await context?.close();
    await browser?.close();
  });

  test('homepage achieves performance score >= 90', async () => {
    await page.goto('http://localhost:4173/', { waitUntil: 'networkidle' });

    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: {
        performance: SCORE_THRESHOLD,
      },
      reports: {
        formats: { json: true },
        name: 'lighthouse-performance',
        directory: path.join(__dirname, '../lighthouse-reports'),
      },
    });

    const performanceScore = result.lhr.categories.performance.score * 100;
    console.log(`Performance Score: ${performanceScore}`);

    expect(performanceScore).toBeGreaterThanOrEqual(SCORE_THRESHOLD);
  });

  test('homepage achieves accessibility score >= 90', async () => {
    await page.goto('http://localhost:4173/', { waitUntil: 'networkidle' });

    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: {
        accessibility: SCORE_THRESHOLD,
      },
      reports: {
        formats: { json: true },
        name: 'lighthouse-accessibility',
        directory: path.join(__dirname, '../lighthouse-reports'),
      },
    });

    const accessibilityScore = result.lhr.categories.accessibility.score * 100;
    console.log(`Accessibility Score: ${accessibilityScore}`);

    expect(accessibilityScore).toBeGreaterThanOrEqual(SCORE_THRESHOLD);
  });

  test('homepage achieves best-practices score >= 90', async () => {
    await page.goto('http://localhost:4173/', { waitUntil: 'networkidle' });

    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: {
        'best-practices': SCORE_THRESHOLD,
      },
      reports: {
        formats: { json: true },
        name: 'lighthouse-best-practices',
        directory: path.join(__dirname, '../lighthouse-reports'),
      },
    });

    const bestPracticesScore = result.lhr.categories['best-practices'].score * 100;
    console.log(`Best Practices Score: ${bestPracticesScore}`);

    expect(bestPracticesScore).toBeGreaterThanOrEqual(SCORE_THRESHOLD);
  });

  test('homepage achieves SEO score >= 90', async () => {
    await page.goto('http://localhost:4173/', { waitUntil: 'networkidle' });

    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: {
        seo: SCORE_THRESHOLD,
      },
      reports: {
        formats: { json: true },
        name: 'lighthouse-seo',
        directory: path.join(__dirname, '../lighthouse-reports'),
      },
    });

    const seoScore = result.lhr.categories.seo.score * 100;
    console.log(`SEO Score: ${seoScore}`);

    expect(seoScore).toBeGreaterThanOrEqual(SCORE_THRESHOLD);
  });
});
