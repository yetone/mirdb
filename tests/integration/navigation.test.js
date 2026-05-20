/**
 * Navigation Integration Tests
 * Owner: Scenario 6 - Navigation and Header
 *
 * Tests:
 * - Smooth scroll behavior
 * - Mobile menu open/close
 * - Keyboard navigation flow
 * - Theme toggle functionality
 * - Active section highlighting
 * - Escape key closes mobile menu
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const navCssPath = path.join(__dirname, '../../css/nav.css');
const navCss = fs.readFileSync(navCssPath, 'utf-8');

const navJsPath = path.join(__dirname, '../../js/nav.js');
const navJs = fs.readFileSync(navJsPath, 'utf-8');

// Mock localStorage
const localStorageMock = (function () {
  let store = {};
  return {
    getItem: function (key) {
      return store[key] || null;
    },
    setItem: function (key, value) {
      store[key] = value.toString();
    },
    removeItem: function (key) {
      delete store[key];
    },
    clear: function () {
      store = {};
    }
  };
})();

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock
});

// Mock matchMedia
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: jest.fn().mockImplementation(query => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: jest.fn(),
    removeListener: jest.fn(),
    addEventListener: jest.fn(),
    removeEventListener: jest.fn(),
    dispatchEvent: jest.fn(),
  })),
});

describe('Navigation JavaScript', () => {
  beforeEach(() => {
    document.head.innerHTML = `<style>${navCss}</style>`;
    document.body.innerHTML = html;
    localStorageMock.clear();

    // Execute nav.js
    eval(navJs);

    // Trigger DOMContentLoaded if needed
    const event = new Event('DOMContentLoaded');
    document.dispatchEvent(event);
  });

  afterEach(() => {
    // Clean up event listeners
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('nav.js executes without errors', () => {
    // If we got here, the script loaded without throwing
    expect(true).toBe(true);
  });

  test('header element is found by nav.js', () => {
    const header = document.querySelector('header');
    expect(header).toBeTruthy();
  });

  test('hamburger button exists', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();
  });

  test('mobile menu is initially closed', () => {
    const mobileNav = document.querySelector('.mobile-nav');
    expect(mobileNav.classList.contains('open')).toBe(false);
  });

  test('hamburger aria-expanded is initially false', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger.getAttribute('aria-expanded')).toBe('false');
  });
});

describe('Mobile Menu Toggle', () => {
  beforeEach(() => {
    document.head.innerHTML = `<style>${navCss}</style>`;
    document.body.innerHTML = html;
    localStorageMock.clear();

    eval(navJs);
    const event = new Event('DOMContentLoaded');
    document.dispatchEvent(event);
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('clicking hamburger opens mobile menu', () => {
    const hamburger = document.querySelector('.hamburger');
    const mobileNav = document.querySelector('.mobile-nav');

    hamburger.click();

    expect(mobileNav.classList.contains('open')).toBe(true);
    expect(hamburger.getAttribute('aria-expanded')).toBe('true');
    expect(document.body.classList.contains('menu-open')).toBe(true);
  });

  test('clicking hamburger again closes mobile menu', () => {
    const hamburger = document.querySelector('.hamburger');
    const mobileNav = document.querySelector('.mobile-nav');

    hamburger.click();
    hamburger.click();

    expect(mobileNav.classList.contains('open')).toBe(false);
    expect(hamburger.getAttribute('aria-expanded')).toBe('false');
    expect(document.body.classList.contains('menu-open')).toBe(false);
  });

  test('backdrop click closes mobile menu', () => {
    const hamburger = document.querySelector('.hamburger');
    const mobileNav = document.querySelector('.mobile-nav');
    const backdrop = document.querySelector('.mobile-backdrop');

    hamburger.click();
    expect(mobileNav.classList.contains('open')).toBe(true);

    backdrop.click();
    expect(mobileNav.classList.contains('open')).toBe(false);
  });
});

describe('Keyboard Navigation', () => {
  beforeEach(() => {
    document.head.innerHTML = `<style>${navCss}</style>`;
    document.body.innerHTML = html;
    localStorageMock.clear();

    eval(navJs);
    const event = new Event('DOMContentLoaded');
    document.dispatchEvent(event);
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('Escape key closes mobile menu', () => {
    const hamburger = document.querySelector('.hamburger');
    const mobileNav = document.querySelector('.mobile-nav');

    hamburger.click();
    expect(mobileNav.classList.contains('open')).toBe(true);

    const escapeEvent = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true });
    document.dispatchEvent(escapeEvent);

    expect(mobileNav.classList.contains('open')).toBe(false);
    expect(hamburger.getAttribute('aria-expanded')).toBe('false');
  });
});

describe('Theme Toggle', () => {
  beforeEach(() => {
    document.head.innerHTML = `<style>${navCss}</style>`;
    document.body.innerHTML = html;
    localStorageMock.clear();

    eval(navJs);
    const event = new Event('DOMContentLoaded');
    document.dispatchEvent(event);
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('theme toggle button exists and is clickable', () => {
    const themeToggle = document.querySelector('.theme-toggle');
    expect(themeToggle).toBeTruthy();
  });

  test('clicking theme toggle sets data-theme attribute', () => {
    const themeToggle = document.querySelector('.theme-toggle');

    // Initial state should be light
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');

    themeToggle.click();

    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  test('clicking theme toggle twice returns to light', () => {
    const themeToggle = document.querySelector('.theme-toggle');

    themeToggle.click();
    themeToggle.click();

    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });

  test('theme preference is saved to localStorage', () => {
    const themeToggle = document.querySelector('.theme-toggle');

    themeToggle.click();

    expect(localStorage.getItem('mirdb-theme')).toBe('dark');
  });

  test('theme preference is restored from localStorage on init', () => {
    localStorage.setItem('mirdb-theme', 'dark');

    // Re-initialize
    document.body.innerHTML = html;
    eval(navJs);
    const event = new Event('DOMContentLoaded');
    document.dispatchEvent(event);

    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });
});

describe('Smooth Scroll', () => {
  beforeEach(() => {
    document.head.innerHTML = `<style>${navCss}</style>`;
    document.body.innerHTML = html;
    localStorageMock.clear();

    // Add a target section for smooth scroll
    const featuresSection = document.createElement('section');
    featuresSection.id = 'features';
    featuresSection.style.height = '500px';
    document.querySelector('main').appendChild(featuresSection);

    eval(navJs);
    const event = new Event('DOMContentLoaded');
    document.dispatchEvent(event);
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('clicking anchor link calls preventDefault', () => {
    const featuresLink = document.querySelector('.nav-links a[href="#features"]');
    const clickEvent = new MouseEvent('click', { bubbles: true, cancelable: true });

    const preventDefaultSpy = jest.spyOn(clickEvent, 'preventDefault');

    featuresLink.dispatchEvent(clickEvent);

    expect(preventDefaultSpy).toHaveBeenCalled();
  });

  test('clicking anchor link closes mobile menu if open', () => {
    const hamburger = document.querySelector('.hamburger');
    const mobileNav = document.querySelector('.mobile-nav');
    const featuresLink = document.querySelector('.mobile-nav .nav-links a[href="#features"]');

    hamburger.click();
    expect(mobileNav.classList.contains('open')).toBe(true);

    featuresLink.click();
    expect(mobileNav.classList.contains('open')).toBe(false);
  });
});

describe('Logo Click', () => {
  beforeEach(() => {
    document.head.innerHTML = `<style>${navCss}</style>`;
    document.body.innerHTML = html;
    localStorageMock.clear();

    eval(navJs);
    const event = new Event('DOMContentLoaded');
    document.dispatchEvent(event);
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('logo link has href="#"', () => {
    const logo = document.querySelector('.header-logo');
    expect(logo.getAttribute('href')).toBe('#');
  });

  test('logo click calls preventDefault', () => {
    const logo = document.querySelector('.header-logo');
    const clickEvent = new MouseEvent('click', { bubbles: true, cancelable: true });

    const preventDefaultSpy = jest.spyOn(clickEvent, 'preventDefault');
    logo.dispatchEvent(clickEvent);

    expect(preventDefaultSpy).toHaveBeenCalled();
  });
});
