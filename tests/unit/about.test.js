/**
 * Unit tests for About Section (Scenario 2).
 *
 * Tests:
 * - About uses semantic section element with id='about'
 * - h2 heading with 'About' or 'What is MirDB'
 * - Paragraph text contains required descriptions
 * - Value proposition mentions persistence/drop-in replacement
 * - Proper semantic HTML structure
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('About Section HTML Structure', () => {
  let html;
  let parser;
  let doc;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
    parser = new DOMParser();
    doc = parser.parseFromString(html, 'text/html');
  });

  test('about uses semantic section element with id="about"', () => {
    const aboutSection = doc.querySelector('section#about');
    expect(aboutSection).not.toBeNull();
    expect(aboutSection.tagName.toLowerCase()).toBe('section');
    expect(aboutSection.id).toBe('about');
  });

  test('about section has h2 heading with "About" or "What is MirDB"', () => {
    const aboutSection = doc.querySelector('section#about');
    const h2 = aboutSection.querySelector('h2');
    expect(h2).not.toBeNull();
    const headingText = h2.textContent.trim();
    expect(
      headingText.toLowerCase().includes('about') ||
      headingText.toLowerCase().includes('what is mirdb')
    ).toBe(true);
  });

  test('about description contains "persistent key-value store written in Rust"', () => {
    const aboutSection = doc.querySelector('section#about');
    const paragraphs = aboutSection.querySelectorAll('p');
    const allText = Array.from(paragraphs).map(p => p.textContent).join(' ');
    expect(allText.toLowerCase()).toContain('persistent key-value store written in rust');
  });

  test('about description contains "Memcached protocol"', () => {
    const aboutSection = doc.querySelector('section#about');
    const paragraphs = aboutSection.querySelectorAll('p');
    const allText = Array.from(paragraphs).map(p => p.textContent).join(' ');
    expect(allText).toContain('Memcached protocol');
  });

  test('about text mentions value proposition with "drop-in replacement" or "persistence"', () => {
    const aboutSection = doc.querySelector('section#about');
    const paragraphs = aboutSection.querySelectorAll('p');
    const allText = Array.from(paragraphs).map(p => p.textContent).join(' ');
    const hasValueProp =
      allText.toLowerCase().includes('drop-in replacement') ||
      allText.toLowerCase().includes('persistence');
    expect(hasValueProp).toBe(true);
  });

  test('about section uses semantic paragraph markup for descriptions', () => {
    const aboutSection = doc.querySelector('section#about');
    const paragraphs = aboutSection.querySelectorAll('p');
    expect(paragraphs.length).toBeGreaterThanOrEqual(1);
  });

  test('about section has proper heading hierarchy (h2, no h1 inside)', () => {
    const aboutSection = doc.querySelector('section#about');
    const headings = aboutSection.querySelectorAll('h1, h2, h3, h4, h5, h6');
    expect(headings.length).toBeGreaterThanOrEqual(1);
    expect(headings[0].tagName.toLowerCase()).toBe('h2');
    // No h1 inside about section
    const h1Inside = aboutSection.querySelector('h1');
    expect(h1Inside).toBeNull();
  });

  test('about section is inside main element', () => {
    const aboutSection = doc.querySelector('section#about');
    const parentMain = aboutSection.closest('main');
    expect(parentMain).not.toBeNull();
  });

  test('about section content is wrapped in a container div', () => {
    const aboutSection = doc.querySelector('section#about');
    const container = aboutSection.querySelector('.container');
    expect(container).not.toBeNull();
    // h2 should be inside container
    const h2InsideContainer = container.querySelector('h2');
    expect(h2InsideContainer).not.toBeNull();
  });
});
