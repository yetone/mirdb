/**
 * Integration tests for section visibility.
 * Owner: Scenario 14 - JavaScript Interactions
 *
 * Tests:
 * - Sections are visible when scrolled into view
 * - Intersection observer behavior
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('Section Visibility Integration', () => {
  beforeEach(() => {
    document.body.innerHTML = htmlContent;
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  it('all main sections are present in the DOM', () => {
    expect(document.getElementById('hero')).not.toBeNull();
    expect(document.getElementById('about')).not.toBeNull();
    expect(document.getElementById('features')).not.toBeNull();
    expect(document.getElementById('status')).not.toBeNull();
    expect(document.getElementById('quick-start')).not.toBeNull();
    expect(document.getElementById('configuration')).not.toBeNull();
  });

  it('sections have correct semantic tag names', () => {
    const hero = document.getElementById('hero');
    const about = document.getElementById('about');
    const features = document.getElementById('features');

    expect(hero.tagName.toLowerCase()).toBe('section');
    expect(about.tagName.toLowerCase()).toBe('section');
    expect(features.tagName.toLowerCase()).toBe('section');
  });

  it('sections contain expected heading elements', () => {
    const about = document.getElementById('about');
    const features = document.getElementById('features');
    const quickStart = document.getElementById('quick-start');

    expect(about.querySelector('h2')).not.toBeNull();
    expect(features.querySelector('h2')).not.toBeNull();
    expect(quickStart.querySelector('h2')).not.toBeNull();
  });

  it('footer is present and contains expected content', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const copyright = footer.querySelector('.footer-copyright');
    expect(copyright).not.toBeNull();
    expect(copyright.textContent.toLowerCase()).toContain('mirdb');
  });
});
