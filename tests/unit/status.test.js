/**
 * Unit tests for Project Status Section (Scenario 4).
 *
 * Tests:
 * - Status section uses semantic section element with id='status'
 * - h2 heading with 'Status' or 'Project Status'
 * - Status items are in ul/ol and li elements with appropriate ARIA attributes
 * - Completed features have checkmark indicators
 * - Planned features have empty checkbox indicators
 * - Proper semantic HTML structure
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Project Status Section HTML Structure', () => {
  let html;
  let parser;
  let doc;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
    parser = new DOMParser();
    doc = parser.parseFromString(html, 'text/html');
  });

  test('status uses semantic section element with id="status"', () => {
    const statusSection = doc.querySelector('section#status');
    expect(statusSection).not.toBeNull();
    expect(statusSection.tagName.toLowerCase()).toBe('section');
    expect(statusSection.id).toBe('status');
  });

  test('status section has h2 heading containing "Status" or "Project Status"', () => {
    const statusSection = doc.querySelector('section#status');
    const h2 = statusSection.querySelector('h2');
    expect(h2).not.toBeNull();
    const headingText = h2.textContent.trim();
    expect(
      headingText.toLowerCase().includes('status') ||
      headingText.toLowerCase().includes('project status')
    ).toBe(true);
  });

  test('status list uses ul element with role="list"', () => {
    const statusSection = doc.querySelector('section#status');
    const list = statusSection.querySelector('ul[role="list"]');
    expect(list).not.toBeNull();
  });

  test('status list has aria-label describing the list purpose', () => {
    const statusSection = doc.querySelector('section#status');
    const list = statusSection.querySelector('ul');
    expect(list).not.toBeNull();
    const ariaLabel = list.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('status');
  });

  test('completed features are in li elements with status-completed class', () => {
    const statusSection = doc.querySelector('section#status');
    const completedItems = statusSection.querySelectorAll('li.status-completed');
    expect(completedItems.length).toBeGreaterThanOrEqual(4);
  });

  test('planned features are in li elements with status-planned class', () => {
    const statusSection = doc.querySelector('section#status');
    const plannedItems = statusSection.querySelectorAll('li.status-planned');
    expect(plannedItems.length).toBeGreaterThanOrEqual(1);
  });

  test('completed items contain checkmark svg', () => {
    const statusSection = doc.querySelector('section#status');
    const completedItems = statusSection.querySelectorAll('li.status-completed');

    completedItems.forEach((item) => {
      const checkmark = item.querySelector('.status-checkmark');
      expect(checkmark).not.toBeNull();
    });
  });

  test('planned items contain empty checkbox svg', () => {
    const statusSection = doc.querySelector('section#status');
    const plannedItems = statusSection.querySelectorAll('li.status-planned');

    plannedItems.forEach((item) => {
      const empty = item.querySelector('.status-empty');
      expect(empty).not.toBeNull();
    });
  });

  test('status indicators have aria-hidden="true"', () => {
    const statusSection = doc.querySelector('section#status');
    const indicators = statusSection.querySelectorAll('.status-indicator');
    expect(indicators.length).toBeGreaterThan(0);

    indicators.forEach((indicator) => {
      expect(indicator.getAttribute('aria-hidden')).toBe('true');
    });
  });

  test('status section has aria-labelledby pointing to heading', () => {
    const statusSection = doc.querySelector('section#status');
    const ariaLabelledBy = statusSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBeTruthy();

    const heading = doc.getElementById(ariaLabelledBy);
    expect(heading).not.toBeNull();
    expect(heading.tagName.toLowerCase()).toBe('h2');
  });

  test('status section is inside main element', () => {
    const statusSection = doc.querySelector('section#status');
    const parentMain = statusSection.closest('main');
    expect(parentMain).not.toBeNull();
  });

  test('completed items contain text for all required features', () => {
    const statusSection = doc.querySelector('section#status');
    const completedItems = statusSection.querySelectorAll('li.status-completed');
    const allText = Array.from(completedItems)
      .map((item) => item.textContent)
      .join(' ');

    expect(allText.toLowerCase()).toContain('tokio');
    expect(allText.toLowerCase()).toContain('memcached protocol');
    expect(allText.toLowerCase()).toContain('memtable');
    expect(allText.toLowerCase()).toContain('skiplist');
    expect(allText.toLowerCase()).toContain('minor compaction');
    expect(allText.toLowerCase()).toContain('major compaction');
  });

  test('planned items contain text for "Raft"', () => {
    const statusSection = doc.querySelector('section#status');
    const plannedItems = statusSection.querySelectorAll('li.status-planned');
    const allText = Array.from(plannedItems)
      .map((item) => item.textContent)
      .join(' ');

    expect(allText.toLowerCase()).toContain('raft');
  });
});
