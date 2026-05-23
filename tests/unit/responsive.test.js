/**
 * Unit tests for responsive CSS module.
 * Owner: Scenario 9 - Responsive Design
 *
 * Tests:
 * - CSS media queries exist for mobile, tablet, and desktop breakpoints
 * - Touch target sizing rules exist
 * - Grid column rules exist at each breakpoint
 */

import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const cssPath = resolve(process.cwd(), 'css', 'responsive.css');

describe('responsive.css', () => {
  let cssContent;

  it('reads the responsive.css file', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toBeTruthy();
    expect(cssContent.length).toBeGreaterThan(0);
  });

  it('contains mobile breakpoint media query (max-width: 767px)', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toMatch(/@media\s*\(\s*max-width:\s*767px\s*\)/);
  });

  it('contains tablet breakpoint media query (min-width: 768px)', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toMatch(/@media\s*\(\s*min-width:\s*768px/);
  });

  it('contains desktop breakpoint media query (min-width: 1024px)', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toMatch(/@media\s*\(\s*min-width:\s*1024px\s*\)/);
  });

  it('contains 1-column grid rule for mobile', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    const mobileSection = cssContent.match(/@media\s*\(\s*max-width:\s*767px\s*\)[^}]*\{([^}]*)\}/gs);
    // Check for single column grid in the mobile section
    expect(cssContent).toMatch(/grid-template-columns:\s*1fr/);
  });

  it('contains 2-column grid rule for tablet', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toMatch(/grid-template-columns:\s*repeat\(2,\s*1fr\)/);
  });

  it('contains 4-column grid rule for desktop', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toMatch(/grid-template-columns:\s*repeat\(4,\s*1fr\)/);
  });

  it('contains touch target sizing rules (min-height: 44px, min-width: 44px)', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toMatch(/min-height:\s*44px/);
    expect(cssContent).toMatch(/min-width:\s*44px/);
  });

  it('contains reduced motion preference media query', () => {
    cssContent = readFileSync(cssPath, 'utf-8');
    expect(cssContent).toMatch(/@media\s*\(\s*prefers-reduced-motion:\s*reduce\s*\)/);
  });
});
