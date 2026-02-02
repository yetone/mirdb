/**
 * Security Unit Tests
 * Owner: Scenario 19 - Security Headers and Links
 *
 * Tests:
 * - External links have rel="noopener noreferrer"
 * - No inline event handlers (onclick, onmouseover, etc.)
 * - Security best practices for links with target="_blank"
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('Security - External Links', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('TC1: All external links with target="_blank" have rel="noopener noreferrer"', () => {
    // Find all anchor tags with target="_blank"
    const targetBlankRegex = /<a[^>]*target="_blank"[^>]*>/gi;
    const targetBlankLinks = htmlContent.match(targetBlankRegex) || [];

    expect(targetBlankLinks.length).toBeGreaterThan(0);

    const violations = [];

    targetBlankLinks.forEach((link, index) => {
      // Extract href for identification
      const hrefMatch = link.match(/href="([^"]*)"/);
      const href = hrefMatch ? hrefMatch[1] : `link ${index + 1}`;

      // Check for rel attribute with noopener and noreferrer
      const relMatch = link.match(/rel="([^"]*)"/);

      if (!relMatch) {
        violations.push(`Link to ${href}: Missing rel attribute`);
      } else {
        const relValue = relMatch[1];
        if (!relValue.includes('noopener')) {
          violations.push(`Link to ${href}: Missing 'noopener' in rel attribute`);
        }
        if (!relValue.includes('noreferrer')) {
          violations.push(`Link to ${href}: Missing 'noreferrer' in rel attribute`);
        }
      }
    });

    expect(violations).toEqual([]);
  });

  test('TC1b: All https:// links have rel="noopener noreferrer"', () => {
    // Find all anchor tags with href starting with https://
    const httpsLinkRegex = /<a[^>]*href="https:\/\/[^"]*"[^>]*>/gi;
    const httpsLinks = htmlContent.match(httpsLinkRegex) || [];

    expect(httpsLinks.length).toBeGreaterThan(0);

    const violations = [];

    httpsLinks.forEach((link, index) => {
      // Extract href for identification
      const hrefMatch = link.match(/href="([^"]*)"/);
      const href = hrefMatch ? hrefMatch[1] : `link ${index + 1}`;

      // Check for rel attribute with noopener and noreferrer
      const relMatch = link.match(/rel="([^"]*)"/);

      if (!relMatch) {
        violations.push(`Link to ${href}: Missing rel attribute`);
      } else {
        const relValue = relMatch[1];
        if (!relValue.includes('noopener')) {
          violations.push(`Link to ${href}: Missing 'noopener' in rel attribute`);
        }
        if (!relValue.includes('noreferrer')) {
          violations.push(`Link to ${href}: Missing 'noreferrer' in rel attribute`);
        }
      }
    });

    expect(violations).toEqual([]);
  });

  test('External links count should match links with security attributes', () => {
    // Count links with target="_blank"
    const targetBlankLinks = htmlContent.match(/<a[^>]*target="_blank"[^>]*>/gi) || [];

    // Count links with both noopener and noreferrer
    const secureLinks = htmlContent.match(/<a[^>]*rel="noopener noreferrer"[^>]*>/gi) || [];

    // All target="_blank" links should have security attributes
    expect(secureLinks.length).toBeGreaterThanOrEqual(targetBlankLinks.length);
  });
});

describe('Security - No Inline Event Handlers', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('TC3: No inline onclick handlers', () => {
    const onclickElements = htmlContent.match(/onclick\s*=/gi) || [];
    expect(onclickElements).toEqual([]);
  });

  test('TC3b: No inline ondblclick handlers', () => {
    const ondblclickElements = htmlContent.match(/ondblclick\s*=/gi) || [];
    expect(ondblclickElements).toEqual([]);
  });

  test('TC3c: No inline onmouseover handlers', () => {
    const onmouseoverElements = htmlContent.match(/onmouseover\s*=/gi) || [];
    expect(onmouseoverElements).toEqual([]);
  });

  test('TC3d: No inline onmouseout handlers', () => {
    const onmouseoutElements = htmlContent.match(/onmouseout\s*=/gi) || [];
    expect(onmouseoutElements).toEqual([]);
  });

  test('TC3e: No inline onkeydown handlers', () => {
    const onkeydownElements = htmlContent.match(/onkeydown\s*=/gi) || [];
    expect(onkeydownElements).toEqual([]);
  });

  test('TC3f: No inline onkeyup handlers', () => {
    const onkeyupElements = htmlContent.match(/onkeyup\s*=/gi) || [];
    expect(onkeyupElements).toEqual([]);
  });

  test('TC3g: No inline onfocus handlers', () => {
    const onfocusElements = htmlContent.match(/onfocus\s*=/gi) || [];
    expect(onfocusElements).toEqual([]);
  });

  test('TC3h: No inline onblur handlers', () => {
    const onblurElements = htmlContent.match(/onblur\s*=/gi) || [];
    expect(onblurElements).toEqual([]);
  });

  test('TC3i: No inline onchange handlers', () => {
    const onchangeElements = htmlContent.match(/onchange\s*=/gi) || [];
    expect(onchangeElements).toEqual([]);
  });

  test('TC3j: No inline onsubmit handlers', () => {
    const onsubmitElements = htmlContent.match(/onsubmit\s*=/gi) || [];
    expect(onsubmitElements).toEqual([]);
  });

  test('TC3k: No inline onload handlers', () => {
    const onloadElements = htmlContent.match(/onload\s*=/gi) || [];
    expect(onloadElements).toEqual([]);
  });

  test('TC3l: No inline onerror handlers', () => {
    const onerrorElements = htmlContent.match(/onerror\s*=/gi) || [];
    expect(onerrorElements).toEqual([]);
  });

  test('TC3m: No javascript: protocol in href attributes', () => {
    const javascriptLinks = htmlContent.match(/href\s*=\s*["']javascript:/gi) || [];
    expect(javascriptLinks).toEqual([]);
  });

  test('Combined: No inline event handlers at all', () => {
    const inlineEventHandlers = [
      'onclick',
      'ondblclick',
      'onmousedown',
      'onmouseup',
      'onmouseover',
      'onmouseout',
      'onmousemove',
      'onmouseenter',
      'onmouseleave',
      'onkeydown',
      'onkeyup',
      'onkeypress',
      'onfocus',
      'onblur',
      'onchange',
      'oninput',
      'onsubmit',
      'onreset',
      'onscroll',
      'onresize',
      'ontouchstart',
      'ontouchend',
      'ontouchmove'
    ];

    const violations = [];

    inlineEventHandlers.forEach(handler => {
      const regex = new RegExp(`${handler}\\s*=`, 'gi');
      const matches = htmlContent.match(regex) || [];
      if (matches.length > 0) {
        violations.push(`Found ${matches.length} inline ${handler} handler(s)`);
      }
    });

    expect(violations).toEqual([]);
  });
});

describe('Security - Content Security', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('No inline style with eval() or expression()', () => {
    // Check for dangerous CSS expressions
    const dangerousCSS = htmlContent.match(/style\s*=\s*["'][^"']*(?:expression\(|eval\()/gi) || [];
    expect(dangerousCSS).toEqual([]);
  });

  test('No data: URIs in script src', () => {
    const dataScripts = htmlContent.match(/<script[^>]*src\s*=\s*["']data:/gi) || [];
    expect(dataScripts).toEqual([]);
  });

  test('Script tags use module type or external src', () => {
    // Find all script tags
    const scriptTags = htmlContent.match(/<script[^>]*>[\s\S]*?<\/script>/gi) || [];

    const violations = [];

    scriptTags.forEach((script, index) => {
      // Check for JSON-LD type (acceptable inline content)
      if (script.includes('type="application/ld+json"')) {
        return; // JSON-LD is acceptable
      }

      // Check if it has an external src
      if (script.match(/src\s*=/)) {
        return; // External scripts are acceptable
      }

      // Check for inline JavaScript content (not empty)
      const content = script.replace(/<script[^>]*>|<\/script>/gi, '').trim();
      if (content && !script.includes('type="application/ld+json"')) {
        violations.push(`Script ${index + 1}: Contains non-JSON-LD inline JavaScript`);
      }
    });

    expect(violations).toEqual([]);
  });

  test('No HTTP resources in HTML (mixed content prevention)', () => {
    // Check for http:// URLs in src or href (excluding protocol-relative or intentional http links in text)
    const httpSrc = htmlContent.match(/src\s*=\s*["']http:\/\//gi) || [];
    const httpHref = htmlContent.match(/href\s*=\s*["']http:\/\//gi) || [];

    const allHttpResources = [...httpSrc, ...httpHref];

    expect(allHttpResources).toEqual([]);
  });
});
