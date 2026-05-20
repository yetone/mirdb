/**
 * Navigation Unit Tests
 * Owner: Scenario 6 - Navigation and Header
 *
 * Tests:
 * - Header DOM structure
 * - Navigation links presence and hrefs
 * - Logo presence and attributes
 * - Theme toggle button presence
 * - Hamburger menu button presence
 * - ARIA attributes
 * - CSS styles for sticky header
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const navCssPath = path.join(__dirname, '../../css/nav.css');
const navCss = fs.readFileSync(navCssPath, 'utf-8');

describe('Header Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('header element exists', () => {
    const header = document.querySelector('header');
    expect(header).toBeTruthy();
  });

  test('header contains a logo link', () => {
    const header = document.querySelector('header');
    const logo = header.querySelector('.header-logo');
    expect(logo).toBeTruthy();
  });

  test('logo link has correct href', () => {
    const logo = document.querySelector('.header-logo');
    expect(logo.getAttribute('href')).toBe('#');
  });

  test('logo contains an image with correct src', () => {
    const logo = document.querySelector('.header-logo');
    const img = logo.querySelector('img');
    expect(img).toBeTruthy();
    expect(img.getAttribute('src')).toMatch(/assets\/logo\.gif/i);
  });

  test('logo image has alt text', () => {
    const logo = document.querySelector('.header-logo');
    const img = logo.querySelector('img');
    expect(img.getAttribute('alt')).toBeTruthy();
    expect(img.getAttribute('alt').length).toBeGreaterThan(0);
  });

  test('header contains desktop navigation', () => {
    const header = document.querySelector('header');
    const desktopNav = header.querySelector('.desktop-nav');
    expect(desktopNav).toBeTruthy();
  });

  test('desktop nav has aria-label', () => {
    const desktopNav = document.querySelector('.desktop-nav');
    expect(desktopNav.getAttribute('aria-label')).toBeTruthy();
  });
});

describe('Navigation Links', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('nav links list exists', () => {
    const navLinks = document.querySelectorAll('.nav-links');
    expect(navLinks.length).toBeGreaterThanOrEqual(1);
  });

  test('Features link exists with correct href', () => {
    const featuresLink = document.querySelector('.nav-links a[href="#features"]');
    expect(featuresLink).toBeTruthy();
    expect(featuresLink.textContent.trim()).toBe('Features');
  });

  test('Docs link exists with correct href', () => {
    const docsLink = document.querySelector('.nav-links a[href="#docs"]');
    expect(docsLink).toBeTruthy();
    expect(docsLink.textContent.trim()).toBe('Docs');
  });

  test('GitHub link exists with correct href', () => {
    const githubLink = document.querySelector('.nav-links a[href="https://github.com/yetone/mirdb"]');
    expect(githubLink).toBeTruthy();
    expect(githubLink.textContent.trim()).toBe('GitHub');
  });

  test('GitHub link opens in new tab', () => {
    const githubLink = document.querySelector('.nav-links a[href="https://github.com/yetone/mirdb"]');
    expect(githubLink.getAttribute('target')).toBe('_blank');
  });

  test('GitHub link has security rel attributes', () => {
    const githubLink = document.querySelector('.nav-links a[href="https://github.com/yetone/mirdb"]');
    const rel = githubLink.getAttribute('rel') || '';
    expect(rel).toContain('noopener');
  });
});

describe('Hamburger Menu', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('hamburger button exists', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();
  });

  test('hamburger has aria-label', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger.getAttribute('aria-label')).toBeTruthy();
  });

  test('hamburger has aria-expanded attribute', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger.getAttribute('aria-expanded')).toBe('false');
  });

  test('hamburger has aria-controls attribute', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger.getAttribute('aria-controls')).toBe('mobile-nav');
  });

  test('hamburger contains three lines', () => {
    const hamburger = document.querySelector('.hamburger');
    const lines = hamburger.querySelectorAll('.hamburger-line');
    expect(lines.length).toBe(3);
  });

  test('mobile nav exists', () => {
    const mobileNav = document.querySelector('.mobile-nav');
    expect(mobileNav).toBeTruthy();
  });

  test('mobile nav has id matching aria-controls', () => {
    const mobileNav = document.querySelector('.mobile-nav');
    const hamburger = document.querySelector('.hamburger');
    expect(mobileNav.id).toBe(hamburger.getAttribute('aria-controls'));
  });
});

describe('Theme Toggle', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('theme toggle button exists in desktop nav', () => {
    const desktopNav = document.querySelector('.desktop-nav');
    const themeToggle = desktopNav.querySelector('.theme-toggle');
    expect(themeToggle).toBeTruthy();
  });

  test('theme toggle has aria-label', () => {
    const themeToggle = document.querySelector('.theme-toggle');
    expect(themeToggle.getAttribute('aria-label')).toBeTruthy();
  });

  test('theme toggle has type button', () => {
    const themeToggle = document.querySelector('.theme-toggle');
    expect(themeToggle.getAttribute('type')).toBe('button');
  });
});

describe('Navigation CSS', () => {
  test('nav.css contains sticky/fixed header styles', () => {
    expect(navCss).toMatch(/position:\s*fixed/i);
  });

  test('header has top positioning', () => {
    expect(navCss).toMatch(/top:\s*0/i);
  });

  test('header has z-index for stacking', () => {
    expect(navCss).toMatch(/z-index:/i);
  });

  test('nav.css contains hamburger styles', () => {
    expect(navCss).toMatch(/\.hamburger/i);
  });

  test('nav.css contains mobile nav styles', () => {
    expect(navCss).toMatch(/\.mobile-nav/i);
  });

  test('nav.css contains theme toggle styles', () => {
    expect(navCss).toMatch(/\.theme-toggle/i);
  });

  test('nav.css contains dark mode styles', () => {
    expect(navCss).toMatch(/\[data-theme="dark"\]/i);
  });

  test('nav.css contains focus styles for accessibility', () => {
    expect(navCss).toMatch(/:focus/i);
  });

  test('mobile nav has transform for slide animation', () => {
    expect(navCss).toMatch(/transform:/i);
  });

  test('hamburger has aria-expanded animation styles', () => {
    expect(navCss).toMatch(/\[aria-expanded="true"\]/i);
  });
});
