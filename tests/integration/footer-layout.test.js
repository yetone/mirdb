/**
 * Footer Layout Integration Tests
 * Owner: Scenario 7 - Footer
 *
 * Tests:
 * - Footer position within page flow (after main)
 * - Footer container alignment and spacing
 * - Footer content layout structure
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

describe('Footer Page Flow', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('footer follows main element in DOM order', () => {
    const main = document.querySelector('main');
    const footer = document.querySelector('footer');

    expect(main).toBeTruthy();
    expect(footer).toBeTruthy();

    // Footer should come after main
    const mainIndex = Array.from(document.body.children).indexOf(main);
    const footerIndex = Array.from(document.body.children).indexOf(footer);
    expect(footerIndex).toBeGreaterThan(mainIndex);
  });

  test('footer is a direct child of body', () => {
    const footer = document.querySelector('footer');
    expect(footer.parentElement.tagName.toLowerCase()).toBe('body');
  });

  test('footer contains container with max-width class', () => {
    const footer = document.querySelector('footer');
    const container = footer.querySelector('.container');
    expect(container).toBeTruthy();
    expect(container.classList.contains('container')).toBe(true);
  });
});

describe('Footer Content Layout', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('footer content wrapper exists', () => {
    const footer = document.querySelector('footer');
    const content = footer.querySelector('.footer-content');
    expect(content).toBeTruthy();
  });

  test('footer contains copyright section', () => {
    const footer = document.querySelector('footer');
    const copyright = footer.querySelector('.footer-copyright');
    expect(copyright).toBeTruthy();
  });

  test('footer contains license section', () => {
    const footer = document.querySelector('footer');
    const license = footer.querySelector('.footer-license');
    expect(license).toBeTruthy();
  });

  test('footer contains links section', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelector('.footer-links');
    expect(links).toBeTruthy();
  });

  test('footer has at least two distinct link items', () => {
    const footer = document.querySelector('footer');
    const listItems = footer.querySelectorAll('.footer-link-list li');
    expect(listItems.length).toBeGreaterThanOrEqual(2);
  });
});
