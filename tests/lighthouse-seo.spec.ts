import { test, expect } from '@playwright/test';
import lighthouse, { type RunnerResult } from 'lighthouse';
import * as chromeLauncher from 'chrome-launcher';

// Path to Playwright's bundled Chromium
const CHROME_PATH = '/home/something/.cache/ms-playwright/chromium-1200/chrome-linux64/chrome';

test.describe('Lighthouse SEO Audit', () => {
  test('should have Lighthouse SEO score of 90 or higher', async () => {
    test.setTimeout(120000);

    const chrome = await chromeLauncher.launch({
      chromePath: CHROME_PATH,
      chromeFlags: ['--headless', '--no-sandbox', '--disable-gpu']
    });

    try {
      const options = {
        logLevel: 'error' as const,
        output: 'json' as const,
        onlyCategories: ['seo'],
        port: chrome.port,
      };

      const runnerResult = await lighthouse('http://localhost:3000/', options) as RunnerResult;

      expect(runnerResult).toBeTruthy();
      expect(runnerResult.lhr).toBeTruthy();
      expect(runnerResult.lhr.categories).toBeTruthy();
      expect(runnerResult.lhr.categories.seo).toBeTruthy();

      const seoScore = runnerResult.lhr.categories.seo.score;
      expect(seoScore).not.toBeNull();

      const seoScorePercent = (seoScore as number) * 100;
      console.log(`Lighthouse SEO Score: ${seoScorePercent}`);

      expect(seoScorePercent).toBeGreaterThanOrEqual(90);
    } finally {
      await chrome.kill();
    }
  });
});
