/**
 * Unit test for Footer Section
 * TC1: Verify the page has a footer element
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Footer Section Display', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  test('TC1: Page has a footer element', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();
    expect(footer.classList.contains('footer')).toBe(true);
  });

  test('Footer has proper container structure', () => {
    const footer = document.querySelector('footer');
    const container = footer.querySelector('.container');
    expect(container).not.toBeNull();
  });

  test('Footer has navigation links section', () => {
    const footer = document.querySelector('footer');
    const footerLinks = footer.querySelector('.footer-links');
    expect(footerLinks).not.toBeNull();
    expect(footerLinks.tagName.toLowerCase()).toBe('nav');
  });

  test('Footer has copyright paragraph', () => {
    const footer = document.querySelector('footer');
    const paragraph = footer.querySelector('p');
    expect(paragraph).not.toBeNull();
    expect(paragraph.textContent).toBeTruthy();
  });
});
