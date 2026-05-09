/**
 * Scenario 15 - Semantic HTML Structure and Heading Hierarchy
 * Owner: Scenario 15
 *
 * Validates the homepage uses semantic HTML elements with a proper heading
 * hierarchy (NFR-5). Covers the document outline, ARIA landmark naming, and
 * the absence of presentational/legacy markup.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const INDEX_PATH = path.resolve(__dirname, '../../index.html');

function loadHomepage() {
  const html = fs.readFileSync(INDEX_PATH, 'utf-8');
  return new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
}

function loadHomepageFromString(html) {
  return new JSDOM(html, { url: 'http://localhost:3000' });
}

function findSkippedHeadingJump(document) {
  const headings = Array.from(
    document.querySelectorAll('h1,h2,h3,h4,h5,h6')
  );
  let previousLevel = 0;
  for (const heading of headings) {
    const level = parseInt(heading.tagName.slice(1), 10);
    if (previousLevel > 0 && level - previousLevel > 1) {
      return {
        previousLevel,
        currentLevel: level,
        text: heading.textContent.trim(),
      };
    }
    previousLevel = Math.max(previousLevel, level);
    // For the document outline check, we walk in document order: any
    // increase greater than 1 over the immediately preceding heading is
    // a violation.
    previousLevel = level;
  }
  return null;
}

function sectionAccessibleNameStatus(section, document) {
  const ariaLabel = section.getAttribute('aria-label');
  if (ariaLabel && ariaLabel.trim().length > 0) {
    return { ok: true, source: 'aria-label' };
  }
  const ariaLabelledBy = section.getAttribute('aria-labelledby');
  if (ariaLabelledBy && ariaLabelledBy.trim().length > 0) {
    const target = document.getElementById(ariaLabelledBy.trim());
    if (target) {
      return { ok: true, source: 'aria-labelledby' };
    }
    return { ok: false, source: 'aria-labelledby', reason: 'target-missing' };
  }
  return { ok: false, source: 'none' };
}

describe('Semantic HTML Structure and Heading Hierarchy', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadHomepage();
    document = dom.window.document;
  });

  afterEach(() => {
    dom = null;
    document = null;
  });

  // Test Case 1: Exactly one <h1> exists in the document.
  it('has exactly one <h1> element on the page', () => {
    const h1s = document.querySelectorAll('h1');
    expect(h1s.length).toBe(1);
  });

  // Test Case 2: Heading hierarchy never skips levels in document order.
  it('does not skip heading levels (no h1 -> h3 jumps)', () => {
    const violation = findSkippedHeadingJump(document);
    if (violation) {
      throw new Error(
        `Heading hierarchy violation: jump from h${violation.previousLevel} to h${violation.currentLevel} at heading "${violation.text}"`
      );
    }
    expect(violation).toBeNull();
  });

  // Test Case 3: Required landmark elements are present.
  it('contains <main>, <nav>, <section>, and <footer> landmarks', () => {
    const main = document.querySelector('main');
    const nav = document.querySelector('nav');
    const sections = document.querySelectorAll('section');
    const footer = document.querySelector('footer');

    expect(main, 'expected <main> landmark').not.toBeNull();
    expect(nav, 'expected <nav> landmark').not.toBeNull();
    expect(
      sections.length,
      'expected at least one <section> landmark'
    ).toBeGreaterThan(0);
    expect(footer, 'expected <footer> landmark').not.toBeNull();
  });

  // Test Case 4: Every <section> has an accessible name.
  it('every <section> has aria-labelledby (resolvable) or aria-label', () => {
    const sections = Array.from(document.querySelectorAll('section'));
    expect(sections.length).toBeGreaterThan(0);

    const offenders = sections
      .map((section) => ({
        id: section.id || '<no-id>',
        status: sectionAccessibleNameStatus(section, document),
      }))
      .filter((s) => !s.status.ok);

    if (offenders.length > 0) {
      const summary = offenders
        .map((o) => `#${o.id} (${o.status.source}${o.status.reason ? ':' + o.status.reason : ''})`)
        .join(', ');
      throw new Error(`Sections missing accessible names: ${summary}`);
    }
    expect(offenders).toEqual([]);
  });

  // Test Case 5: No presentational/legacy elements.
  it('contains zero <font>, <center>, <marquee>, or <blink> elements', () => {
    const presentational = document.querySelectorAll(
      'font, center, marquee, blink'
    );
    expect(presentational.length).toBe(0);
  });

  // Test Case 6: Features section uses semantic list markup.
  it('uses semantic list markup inside #features (ul/ol or <article>s)', () => {
    const features = document.querySelector('#features');
    expect(features, 'expected #features section').not.toBeNull();

    const listContainer = features.querySelector('ul, ol');
    const articles = features.querySelectorAll('article');

    const usesList = !!listContainer;
    const usesArticles = articles.length >= 2;

    expect(
      usesList || usesArticles,
      'expected #features to render items as <ul>/<ol> children or <article> elements (not loose <div>s)'
    ).toBe(true);

    if (usesList) {
      const listItems = listContainer.querySelectorAll(':scope > li');
      expect(
        listItems.length,
        'expected <ul>/<ol> to have multiple <li> children'
      ).toBeGreaterThanOrEqual(2);
    }
  });

  // Test Case 7: Negative case - documents with two <h1>s must be detected.
  it('detects documents that contain more than one <h1> as a violation', () => {
    const offendingHtml = `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Bad</title></head><body>
        <main>
          <h1>First</h1>
          <p>Paragraph</p>
          <h1>Second</h1>
        </main>
      </body></html>`;
    const offendingDom = loadHomepageFromString(offendingHtml);
    const offendingDocument = offendingDom.window.document;
    const h1s = offendingDocument.querySelectorAll('h1');
    expect(h1s.length).toBeGreaterThan(1);

    // The validator we use in the positive test should flag this document.
    let detected = false;
    let detectedAt = null;
    if (h1s.length !== 1) {
      detected = true;
      detectedAt = h1s[1].textContent;
    }
    expect(detected).toBe(true);
    expect(detectedAt).toBe('Second');
  });
});
