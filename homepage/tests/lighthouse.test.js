/**
 * Lighthouse Performance Tests - E2E
 *
 * This test suite verifies:
 * - Page loads within 3 seconds on 3G connection
 * - Lighthouse performance score is greater than 80
 *
 * These tests run the performance-e2e.test.js script which uses Playwright
 * and Lighthouse CLI, then validates the results.
 */

const { execSync, spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// Increase timeout for performance tests
jest.setTimeout(300000);

describe('Performance - Lighthouse Audit', () => {
  let testResults = null;
  const resultsPath = path.join(__dirname, 'performance-e2e-results.json');

  beforeAll(async () => {
    // Clean up any existing results
    if (fs.existsSync(resultsPath)) {
      fs.unlinkSync(resultsPath);
    }

    // Run the E2E performance tests
    const e2eTestPath = path.join(__dirname, 'performance-e2e-runner.js');

    console.log('Running E2E performance tests...');

    try {
      const output = execSync(`node "${e2eTestPath}"`, {
        timeout: 240000,
        encoding: 'utf-8',
        stdio: ['pipe', 'pipe', 'pipe']
      });
      console.log(output);
    } catch (err) {
      // Test script may exit with non-zero if tests fail
      // but we still want to read the results
      if (err.stdout) console.log(err.stdout);
      if (err.stderr) console.error(err.stderr);
    }

    // Read the results
    if (fs.existsSync(resultsPath)) {
      testResults = JSON.parse(fs.readFileSync(resultsPath, 'utf-8'));
    }
  });

  afterAll(() => {
    // Cleanup results file
    if (fs.existsSync(resultsPath)) {
      fs.unlinkSync(resultsPath);
    }
  });

  // Test Case 1: Load page on 3G throttled connection
  describe('Test Case 1: 3G Connection Load Time', () => {
    test('should load page interactively within 3 seconds on 3G throttled connection', () => {
      expect(testResults).not.toBeNull();

      const loadTimeTest = testResults.tests.find(t => t.name === '3G Load Time');
      expect(loadTimeTest).toBeDefined();

      if (loadTimeTest.status === 'pass') {
        console.log(`DOM Content Loaded in: ${loadTimeTest.time?.toFixed(2)} seconds (3G throttled)`);
      }

      expect(loadTimeTest.status).toBe('pass');
    });

    test('should have reasonable First Contentful Paint on 3G', () => {
      expect(testResults).not.toBeNull();

      const fcpTest = testResults.tests.find(t => t.name === 'FCP on 3G');
      expect(fcpTest).toBeDefined();

      // Allow skipped test if FCP metric was not available
      if (fcpTest.status === 'skipped') {
        console.log('FCP test skipped - metric not available');
        return;
      }

      if (fcpTest.status === 'pass') {
        console.log(`First Contentful Paint: ${fcpTest.time?.toFixed(2)} seconds`);
      }

      expect(fcpTest.status).toBe('pass');
    });
  });

  // Test Case 2: Run Lighthouse performance audit
  describe('Test Case 2: Lighthouse Performance Score', () => {
    test('should have Lighthouse performance score greater than 80', () => {
      expect(testResults).not.toBeNull();

      const lighthouseTest = testResults.tests.find(t => t.name === 'Lighthouse Score');
      expect(lighthouseTest).toBeDefined();

      if (lighthouseTest.status === 'pass') {
        console.log(`Lighthouse Performance Score: ${lighthouseTest.score?.toFixed(0)}`);
      } else if (lighthouseTest.error) {
        console.log(`Lighthouse test error: ${lighthouseTest.error}`);
      }

      expect(lighthouseTest.status).toBe('pass');
    });
  });
});
