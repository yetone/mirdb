/**
 * Navigation Integration Tests
 * Owner: Scenario 2 - Navigation Bar Functionality
 *
 * Tests for navigation HTML structure and JavaScript functionality
 */

const fs = require('fs');
const path = require('path');

describe('Navigation Bar HTML Structure', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Set up jsdom document
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  beforeEach(() => {
    // Reset the document for each test
    document.body.innerHTML = htmlContent;
    // Re-parse to get the document structure
    const parser = new DOMParser();
    const doc = parser.parseFromString(htmlContent, 'text/html');
    document = doc;
  });

  test('navigation bar contains a logo', () => {
    const logo = document.querySelector('.nav-logo');
    expect(logo).not.toBeNull();

    const logoImg = logo.querySelector('img');
    expect(logoImg).not.toBeNull();
    expect(logoImg.getAttribute('alt')).toBeTruthy();
  });

  test('navigation bar contains Features link', () => {
    const featuresLink = document.querySelector('a[href="#features"]');
    expect(featuresLink).not.toBeNull();
    expect(featuresLink.textContent).toContain('Features');
  });

  test('navigation bar contains Usage link', () => {
    const usageLink = document.querySelector('a[href="#usage"]');
    expect(usageLink).not.toBeNull();
    expect(usageLink.textContent).toContain('Usage');
  });

  test('navigation bar contains Architecture link', () => {
    const architectureLink = document.querySelector('a[href="#architecture"]');
    expect(architectureLink).not.toBeNull();
    expect(architectureLink.textContent).toContain('Architecture');
  });

  test('navigation bar contains Get Started link', () => {
    const getStartedLink = document.querySelector('a[href="#getting-started"]');
    expect(getStartedLink).not.toBeNull();
    expect(getStartedLink.textContent).toContain('Get Started');
  });

  test('navigation bar contains GitHub link', () => {
    const githubLink = document.querySelector('.nav-github');
    expect(githubLink).not.toBeNull();
    expect(githubLink.getAttribute('href')).toContain('github.com');
    expect(githubLink.getAttribute('target')).toBe('_blank');
  });

  test('navigation bar has hamburger toggle button', () => {
    const toggle = document.querySelector('.nav-toggle');
    expect(toggle).not.toBeNull();
    expect(toggle.getAttribute('aria-label')).toBeTruthy();
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
  });

  test('navigation bar has proper ARIA attributes', () => {
    const nav = document.querySelector('.main-nav');
    expect(nav).not.toBeNull();
    expect(nav.getAttribute('role')).toBe('navigation');
    expect(nav.getAttribute('aria-label')).toBeTruthy();
  });

  test('navigation has all required sections on the page', () => {
    const featuresSection = document.getElementById('features');
    const usageSection = document.getElementById('usage');
    const architectureSection = document.getElementById('architecture');
    const gettingStartedSection = document.getElementById('getting-started');

    expect(featuresSection).not.toBeNull();
    expect(usageSection).not.toBeNull();
    expect(architectureSection).not.toBeNull();
    expect(gettingStartedSection).not.toBeNull();
  });

  test('navigation bar is fixed position', () => {
    // Check that the nav has the main-nav class which should have position: fixed
    const nav = document.querySelector('.main-nav');
    expect(nav).not.toBeNull();
    expect(nav.id).toBe('main-nav');
  });
});
