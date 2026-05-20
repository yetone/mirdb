/**
 * Hero Section Layout Integration Tests
 * Owner: Scenario 1 - Hero Section
 *
 * Tests:
 * - Hero section layout within page flow
 * - Hero is first visible section above the fold
 * - CTA link navigation behavior
 * - Container alignment and spacing
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

describe('Hero Section Layout', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('hero section is the first child of main', () => {
    const main = document.querySelector('main');
    const children = Array.from(main.children);
    const firstSection = children.find(child => child.tagName.toLowerCase() === 'section');
    expect(firstSection).toBeTruthy();
    expect(firstSection.id).toBe('hero');
  });

  test('hero section has a content container', () => {
    const hero = document.querySelector('section#hero');
    const content = hero.querySelector('.hero-content');
    expect(content).toBeTruthy();
  });

  test('hero content contains all required elements', () => {
    const hero = document.querySelector('section#hero');
    const content = hero.querySelector('.hero-content');

    expect(content.querySelector('img.hero-logo')).toBeTruthy();
    expect(content.querySelector('h1.hero-tagline')).toBeTruthy();
    expect(content.querySelector('p.hero-subtext')).toBeTruthy();
    expect(content.querySelector('.hero-cta-group')).toBeTruthy();
  });

  test('hero CTA group contains exactly two buttons', () => {
    const hero = document.querySelector('section#hero');
    const ctaGroup = hero.querySelector('.hero-cta-group');
    const ctas = ctaGroup.querySelectorAll('.hero-cta');
    expect(ctas.length).toBe(2);
  });

  test('primary CTA appears before secondary CTA in DOM', () => {
    const hero = document.querySelector('section#hero');
    const ctaGroup = hero.querySelector('.hero-cta-group');
    const ctas = ctaGroup.querySelectorAll('.hero-cta');

    expect(ctas[0].classList.contains('hero-cta-primary')).toBe(true);
    expect(ctas[1].classList.contains('hero-cta-secondary')).toBe(true);
  });

  test('hero background element exists for visual effect', () => {
    const hero = document.querySelector('section#hero');
    const bg = hero.querySelector('.hero-background');
    expect(bg).toBeTruthy();
  });
});

describe('Hero Section Navigation Behavior', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('primary CTA navigates to GitHub in new tab', () => {
    const hero = document.querySelector('section#hero');
    const primaryCta = hero.querySelector('.hero-cta-primary');

    expect(primaryCta.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    expect(primaryCta.getAttribute('target')).toBe('_blank');
  });

  test('secondary CTA links to quickstart anchor', () => {
    const hero = document.querySelector('section#hero');
    const secondaryCta = hero.querySelector('.hero-cta-secondary');

    expect(secondaryCta.getAttribute('href')).toBe('#quickstart');
  });

  test('secondary CTA uses same-page anchor navigation', () => {
    const hero = document.querySelector('section#hero');
    const secondaryCta = hero.querySelector('.hero-cta-secondary');
    const href = secondaryCta.getAttribute('href');

    expect(href.startsWith('#')).toBe(true);
    expect(href).toBe('#quickstart');
  });
});
