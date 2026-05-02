/**
 * Project status and badges validation tests.
 * Owner: Scenario 4 - Project Status and Badges
 *
 * Validates CI/CD status badges and feature completion tracking.
 * Checks for accurate representation of completed vs planned features.
 */

const { setupHomePage } = require('../../utils/setup');

describe('Project Status and Badges', () => {
  let page;

  beforeAll(async () => {
    page = await setupHomePage();
  });

  afterAll(async () => {
    await page.close();
  });

  describe('Status Badges', () => {
    test('CircleCI badge displays current build status', async () => {
      const circleCiBadge = await page.$('img[alt="CircleCI"]');
      expect(circleCiBadge).toBeTruthy();

      // Verify badge points to correct repository
      const src = await circleCiBadge.getAttribute('src');
      expect(src).toContain('circleci.com/gh/');

      // Verify badge is functioning (simplified - would need network check in real test)
      const response = await page.request.get(src);
      expect(response.status()).toBe(200);
    });
  });

  describe('Feature Status Tracking', () => {
    test('Completed features list is accurate', async () => {
      const completedFeatures = [
        'tokio networking',
        'memtable',
        'minor compaction',
        'major compaction'
      ];

      for (const feature of completedFeatures) {
        const featureElement = await page.$(`.completed-features:has-text("${feature}")`);
        expect(featureElement).toBeTruthy();
      }
    });

    test('Planned features include Raft consensus', async () => {
      const raftElement = await page.$('.planned-features:has-text("Raft consensus")');
      expect(raftElement).toBeTruthy();
    });
  });
});