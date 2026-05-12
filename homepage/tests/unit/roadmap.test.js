/**
 * Unit tests for the Roadmap section.
 *
 * Owner: Scenario 6 — Roadmap section (REQ-7).
 *
 * Tests cover the five test cases in .something/scenario.json:
 *   1. #roadmap section + h2 "Roadmap"
 *   2. data-status="done" items: ≥4 and mention memcached, skip, minor, major
 *   3. data-status="planned" items: ≥1 and at least one mentions Raft
 *   4. Non-color visual distinction (icon + aria-label) — WCAG 1.4.1
 *   5. content.json roadmap schema (done.length ≥ 4, planned.length ≥ 1)
 *
 * The tests assemble the full public/index.html via loadFullPage so any
 * regression in section wiring is caught here, and additionally check the
 * standalone roadmap.html partial to keep the component self-contained.
 */

const fs = require('fs');
const path = require('path');

const { loadFullPage, loadPartial } = require('../helpers/dom');

function loadContent() {
  const contentPath = path.resolve(
    __dirname,
    '..',
    '..',
    'src',
    'data',
    'content.json'
  );
  return JSON.parse(fs.readFileSync(contentPath, 'utf8'));
}

describe('Roadmap section', () => {
  let doc;

  beforeEach(() => {
    doc = loadFullPage();
  });

  test('test_case_1: roadmap section exists with an h2 titled "Roadmap"', () => {
    const section = doc.getElementById('roadmap');
    expect(section).not.toBeNull();
    expect(section.tagName.toLowerCase()).toBe('section');

    const heading = section.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading.textContent.toLowerCase()).toContain('roadmap');
  });

  test('test_case_2: done items include memcached, skip, minor, major', () => {
    const doneItems = doc.querySelectorAll('#roadmap [data-status="done"]');
    expect(doneItems.length).toBeGreaterThanOrEqual(4);

    const combined = Array.from(doneItems)
      .map((el) => el.textContent)
      .join(' ')
      .toLowerCase();

    expect(combined).toContain('memcached');
    expect(combined).toContain('skip');
    expect(combined).toContain('minor');
    expect(combined).toContain('major');
  });

  test('test_case_3: planned items include at least one Raft entry', () => {
    const plannedItems = doc.querySelectorAll('#roadmap [data-status="planned"]');
    expect(plannedItems.length).toBeGreaterThanOrEqual(1);

    const mentionsRaft = Array.from(plannedItems).some((el) =>
      el.textContent.toLowerCase().includes('raft')
    );
    expect(mentionsRaft).toBe(true);
  });

  test('test_case_4: each item has non-color status distinction (icon and/or aria-label)', () => {
    const allItems = doc.querySelectorAll(
      '#roadmap [data-status="done"], #roadmap [data-status="planned"]'
    );
    expect(allItems.length).toBeGreaterThan(0);

    allItems.forEach((item) => {
      const status = item.getAttribute('data-status');
      const hasIcon = item.querySelector('svg') !== null;
      const ariaLabel = item.getAttribute('aria-label') || '';
      const labelEncodesStatus =
        (status === 'done' && /done|implemented|shipped|complete/i.test(ariaLabel)) ||
        (status === 'planned' && /planned|upcoming|next/i.test(ariaLabel));

      expect(hasIcon || labelEncodesStatus).toBe(true);
    });
  });

  test('test_case_5: content.json roadmap arrays are well-formed', () => {
    const content = loadContent();
    expect(content.roadmap).toBeDefined();
    expect(Array.isArray(content.roadmap.done)).toBe(true);
    expect(Array.isArray(content.roadmap.planned)).toBe(true);

    expect(content.roadmap.done.length).toBeGreaterThanOrEqual(4);
    expect(content.roadmap.planned.length).toBeGreaterThanOrEqual(1);

    const doneTitles = content.roadmap.done
      .map((entry) => entry.title || '')
      .join(' ')
      .toLowerCase();
    expect(doneTitles).toContain('memcached');
    expect(doneTitles).toContain('skip');
    expect(doneTitles).toContain('minor');
    expect(doneTitles).toContain('major');

    const plannedTitles = content.roadmap.planned
      .map((entry) => entry.title || '')
      .join(' ')
      .toLowerCase();
    expect(plannedTitles).toContain('raft');
  });

  test('regression: roadmap.html partial loads on its own', () => {
    const partial = loadPartial('src/components/roadmap/roadmap.html');
    expect(partial.querySelector('#roadmap')).not.toBeNull();
    expect(
      partial.querySelectorAll('#roadmap [data-status="done"]').length
    ).toBeGreaterThanOrEqual(4);
    expect(
      partial.querySelectorAll('#roadmap [data-status="planned"]').length
    ).toBeGreaterThanOrEqual(1);
  });
});
