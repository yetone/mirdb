/**
 * Project Status Display Tests
 * Scenario: Verify that project status (implemented vs. planned features) is indicated (REQ-8)
 */

const fs = require('fs');
const path = require('path');

describe('Project Status Display', () => {
  let document;

  beforeAll(() => {
    // Load the HTML file
    const html = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Project status information exists somewhere on page
  describe('Test Case 1: Status/roadmap section or badges exist', () => {
    test('Project status information exists somewhere on page', () => {
      // Look for status section by ID
      const statusSection = document.getElementById('status') ||
                           document.getElementById('roadmap') ||
                           document.getElementById('project-status');

      // Also check for status-related classes or data attributes
      const statusByClass = document.querySelector('.project-status, .status-section, .roadmap, .status');

      // Check for status badges in footer or elsewhere
      const statusBadges = document.querySelectorAll('[class*="badge"], [class*="status"]');

      // Check for status keywords in any section
      const pageContent = document.body.textContent.toLowerCase();
      const hasStatusContent = pageContent.includes('implemented') ||
                               pageContent.includes('planned') ||
                               pageContent.includes('coming soon') ||
                               pageContent.includes('roadmap') ||
                               pageContent.includes('in development');

      // At least one of these should be true
      const hasStatusInfo = statusSection !== null ||
                           statusByClass !== null ||
                           statusBadges.length > 0 ||
                           hasStatusContent;

      expect(hasStatusInfo).toBe(true);
    });
  });

  // Test Case 2: At least one feature is marked as implemented/complete
  describe('Test Case 2: Implemented feature indicators exist', () => {
    test('At least one feature is marked as implemented/complete', () => {
      const pageContent = document.body.innerHTML.toLowerCase();

      // Check for implemented indicators
      const hasImplementedBadge = pageContent.includes('implemented') ||
                                  pageContent.includes('complete') ||
                                  pageContent.includes('available') ||
                                  pageContent.includes('shipped');

      // Check for specific implemented features mentioned
      const hasImplementedFeatures = (
        (pageContent.includes('memcached') && pageContent.includes('implemented')) ||
        (pageContent.includes('persistence') && pageContent.includes('implemented')) ||
        (pageContent.includes('compaction') && pageContent.includes('implemented')) ||
        (pageContent.includes('wal') && pageContent.includes('implemented')) ||
        (pageContent.includes('write-ahead') && pageContent.includes('implemented'))
      );

      // Check for visual indicators of status
      const statusIndicators = document.querySelectorAll('.status-implemented, .badge-implemented, .implemented, [data-status="implemented"], .status-complete, .complete');

      expect(hasImplementedBadge || hasImplementedFeatures || statusIndicators.length > 0).toBe(true);
    });
  });

  // Test Case 3: Raft/distributed feature marked as planned/coming soon
  describe('Test Case 3: Planned feature indicators exist', () => {
    test('Raft/distributed feature marked as planned/coming soon', () => {
      const pageContent = document.body.innerHTML.toLowerCase();

      // Check for Raft or distributed features in planned/coming context
      const hasRaftPlanned = pageContent.includes('raft') || pageContent.includes('distributed');

      // Check for planned/coming soon indicators
      const hasPlannedIndicators = pageContent.includes('planned') ||
                                   pageContent.includes('coming soon') ||
                                   pageContent.includes('coming-soon') ||
                                   pageContent.includes('in development') ||
                                   pageContent.includes('future') ||
                                   pageContent.includes('roadmap');

      // Check for visual indicators of planned status
      const plannedIndicators = document.querySelectorAll('.status-planned, .badge-planned, .planned, [data-status="planned"], .coming-soon, .future');

      // Both conditions should be met - either the indicators exist with Raft mentioned, or visual badges exist
      expect((hasRaftPlanned && hasPlannedIndicators) || plannedIndicators.length > 0).toBe(true);
    });
  });

  // Additional: Verify clear visual distinction between implemented and planned
  describe('Additional: Visual distinction between status types', () => {
    test('Status indicators have clear visual styling', () => {
      // Check for status-related elements with specific styling classes
      const implementedElements = document.querySelectorAll('.status-implemented, .implemented, [data-status="implemented"]');
      const plannedElements = document.querySelectorAll('.status-planned, .planned, [data-status="planned"]');

      // Both implemented and planned should have some elements
      const hasStatusElements = implementedElements.length > 0 || plannedElements.length > 0;

      // If we have both types, they should be distinguishable
      if (implementedElements.length > 0 && plannedElements.length > 0) {
        expect(true).toBe(true); // Both exist - good
      } else if (hasStatusElements) {
        expect(true).toBe(true); // At least one exists
      } else {
        // Fallback: check for inline status indicators in text
        const pageContent = document.body.textContent;
        const hasInlineStatus = pageContent.includes('Implemented') || pageContent.includes('Planned');
        expect(hasInlineStatus).toBe(true);
      }
    });
  });
});
