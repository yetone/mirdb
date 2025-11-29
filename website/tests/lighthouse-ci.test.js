import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Check if Lighthouse CLI is available
function isLighthouseAvailable() {
  try {
    execSync('npx lighthouse --version', { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

function startTestServer() {
  const { spawn } = await import('child_process');
  const server = spawn('python3', ['-m', 'http.server', '4000', '--directory', path.join(__dirname, '..', 'dist')]);

  // Give server time to start
  return new Promise((resolve, reject) => {
    server.stdout.on('data', (data) => {
      if (data.toString().includes('Serving HTTP')) {
        resolve(server);
      }
    });
    server.stderr.on('data', (data) => {
      // Ignore errors for now
    });
    setTimeout(() => resolve(server), 2000); // Timeout after 2 seconds
  });
}

test.describe('Lighthouse Performance', () => {
  test.skip(!isLighthouseAvailable(), 'Lighthouse CLI not available');

  test('should run Lighthouse audit and score above 90', async () => {
    // Start a test server
    const server = await startTestServer();

    try {
      // Run Lighthouse audit
      const cmd = `npx lighthouse http://localhost:4000 --output=json --output-path=stdout --quiet --chrome-flags="--headless"`;
      const output = execSync(cmd, { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 });

      const results = JSON.parse(output);

      // Extract scores
      const performance = results.categories.performance.score * 100;
      const accessibility = results.categories.accessibility.score * 100;
      const bestPractices = results.categories['best-practices'].score * 100;
      const seo = results.categories.seo.score * 100;

      console.log('\n=== Lighthouse Scores ===');
      console.log(`Performance: ${performance.toFixed(0)}`);
      console.log(`Accessibility: ${accessibility.toFixed(0)}`);
      console.log(`Best Practices: ${bestPractices.toFixed(0)}`);
      console.log(`SEO: ${seo.toFixed(0)}`);

      // Verify scores meet requirements
      expect(performance).toBeGreaterThanOrEqual(90);
      expect(accessibility).toBeGreaterThanOrEqual(90);
      expect(bestPractices).toBeGreaterThanOrEqual(90);
      expect(seo).toBeGreaterThanOrEqual(90);

      // Check Core Web Vitals
      const metrics = results.audits;
      const fcp = metrics['first-contentful-paint'].numericValue; // in ms
      const lcp = metrics['largest-contentful-paint'].numericValue; // in ms

      console.log('\n=== Core Web Vitals ===');
      console.log(`First Contentful Paint: ${(fcp / 1000).toFixed(2)}s`);
      console.log(`Largest Contentful Paint: ${(lcp / 1000).toFixed(2)}s`);

      // Should load quickly
      expect(fcp).toBeLessThan(3000); // Under 3 seconds
      expect(lcp).toBeLessThan(3000); // Under 3 seconds

    } finally {
      // Kill the test server
      server.kill();
    }
  });
});
