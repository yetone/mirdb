/**
 * Responsive design integration tests — Scenario 8 (NFR-2).
 *
 * Validates that:
 *   - responsive.css defines three distinct breakpoints
 *     (mobile <=640px, tablet 641-1024px, desktop >=1025px).
 *   - The features grid reflows from 1 -> 2 -> 3+ columns.
 *   - The nav toggle (hamburger) is visible at mobile and hidden at desktop.
 *   - initNavigation correctly opens the menu on click in the mobile layout.
 *
 * Each `test` corresponds to a test_case id in .something/scenario.json.
 */

const fs = require('fs');
const path = require('path');
const { loadFullPage, loadPartial } = require('../helpers/dom');

const REPO_ROOT = path.resolve(__dirname, '..', '..');

function readCss(rel) {
  return fs.readFileSync(path.join(REPO_ROOT, rel), 'utf8');
}

// ----- CSS text parsing helpers -------------------------------------------

/**
 * Pull out the top-level @media query blocks from a CSS string.
 * Returns an array of { query, body } entries where `body` is the raw
 * concatenated text inside the braces of that @media block.
 */
function extractMediaBlocks(css) {
  const blocks = [];
  for (let i = 0; i < css.length; i++) {
    if (css.startsWith('@media', i)) {
      const braceStart = css.indexOf('{', i);
      if (braceStart === -1) break;
      const query = css.slice(i + 6, braceStart).trim();
      let depth = 1;
      let j = braceStart + 1;
      while (j < css.length && depth > 0) {
        if (css[j] === '{') depth++;
        else if (css[j] === '}') depth--;
        if (depth === 0) break;
        j++;
      }
      const body = css.slice(braceStart + 1, j);
      blocks.push({ query, body });
      i = j;
    }
  }
  return blocks;
}

/**
 * Find the declaration body for a selector inside a CSS body fragment.
 * Returns the raw inner text of the first matching rule, or null.
 */
function extractRuleBody(cssBody, selector) {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(
    '(?:^|[}\\s,])' + escaped + '\\s*(?:,[^{]*)?\\{([^{}]*)\\}',
    'm'
  );
  const m = re.exec(cssBody);
  return m ? m[1].trim() : null;
}

/** Read a single declared CSS property value from a rule body. */
function getProp(ruleBody, prop) {
  if (!ruleBody) return null;
  const re = new RegExp('(?:^|;|\\s)' + prop + '\\s*:\\s*([^;]+)', 'i');
  const m = re.exec(ruleBody);
  return m ? m[1].trim() : null;
}

/**
 * Build a fake MediaQueryList for use inside our matchMedia stub.
 * Only the fields jsdom navigation code actually touches are populated.
 */
function makeMatchMediaStub(matchedQueries) {
  const normalized = matchedQueries.map((q) => q.replace(/\s+/g, ''));
  return function matchMedia(query) {
    const key = String(query || '').replace(/\s+/g, '');
    const matches = normalized.includes(key);
    return {
      matches,
      media: query,
      onchange: null,
      addEventListener() {},
      removeEventListener() {},
      addListener() {},
      removeListener() {},
      dispatchEvent() {
        return false;
      },
    };
  };
}

// ----- Tests ---------------------------------------------------------------

describe('Responsive layout — Scenario 8', () => {
  let css;
  let blocks;

  beforeAll(() => {
    css = readCss('src/styles/responsive.css');
    blocks = extractMediaBlocks(css);
  });

  // test_case 1
  test('responsive.css declares at least three @media breakpoints', () => {
    expect(blocks.length).toBeGreaterThanOrEqual(3);

    const hasMobile = blocks.some(
      (b) => /max-width:\s*640px/.test(b.query) && !/min-width/.test(b.query)
    );
    const hasTablet = blocks.some(
      (b) =>
        /min-width:\s*641px/.test(b.query) &&
        /max-width:\s*1024px/.test(b.query)
    );
    const hasDesktop = blocks.some(
      (b) => /min-width:\s*1025px/.test(b.query)
    );

    expect(hasMobile).toBe(true);
    expect(hasTablet).toBe(true);
    expect(hasDesktop).toBe(true);
  });

  // test_case 2
  test('mobile @media rule reflows .features-grid to a single column', () => {
    const mobile = blocks.find(
      (b) => /max-width:\s*640px/.test(b.query) && !/min-width/.test(b.query)
    );
    expect(mobile).toBeDefined();

    const ruleBody = extractRuleBody(mobile.body, '.features-grid');
    expect(ruleBody).not.toBeNull();

    const cols = getProp(ruleBody, 'grid-template-columns');
    expect(cols).toBeTruthy();
    // 1fr OR repeat(1, ...) is acceptable
    expect(/^1fr$|repeat\(\s*1\s*,/.test(cols)).toBe(true);
  });

  // test_case 3
  test('tablet @media rule (641-1024px) sets .features-grid to two columns', () => {
    const tablet = blocks.find(
      (b) =>
        /min-width:\s*641px/.test(b.query) &&
        /max-width:\s*1024px/.test(b.query)
    );
    expect(tablet).toBeDefined();

    const ruleBody = extractRuleBody(tablet.body, '.features-grid');
    expect(ruleBody).not.toBeNull();

    const cols = getProp(ruleBody, 'grid-template-columns');
    expect(cols).toBeTruthy();
    expect(/repeat\(\s*2\s*,/.test(cols)).toBe(true);
  });

  // test_case 4
  test('desktop @media rule (min-width 1025px) sets .features-grid to 3 or 4 columns', () => {
    const desktop = blocks.find(
      (b) => /min-width:\s*1025px/.test(b.query) && !/max-width/.test(b.query)
    );
    expect(desktop).toBeDefined();

    const ruleBody = extractRuleBody(desktop.body, '.features-grid');
    expect(ruleBody).not.toBeNull();

    const cols = getProp(ruleBody, 'grid-template-columns');
    expect(cols).toBeTruthy();
    expect(/repeat\(\s*(3|4)\s*,/.test(cols)).toBe(true);
  });

  // test_case 5
  test('mobile viewport: .nav-toggle is visible and .nav-menu collapsed', () => {
    // Confirm responsive.css declares the toggle as displayed at mobile.
    const mobile = blocks.find(
      (b) => /max-width:\s*640px/.test(b.query) && !/min-width/.test(b.query)
    );
    expect(mobile).toBeDefined();

    const toggleBody = extractRuleBody(mobile.body, '.nav-toggle');
    expect(toggleBody).not.toBeNull();
    const toggleDisplay = getProp(toggleBody, 'display');
    expect(toggleDisplay).toBeTruthy();
    expect(toggleDisplay).not.toBe('none');

    // And confirm .nav-menu is collapsed (display:none) inside the same block,
    // showing only when the .open class is applied.
    const menuBody = extractRuleBody(mobile.body, '.nav-menu');
    expect(menuBody).not.toBeNull();
    expect(getProp(menuBody, 'display')).toBe('none');

    // The matchMedia stub also flips matches=true for the mobile query, so any
    // future helper relying on it reads the right value.
    const doc = loadFullPage();
    doc.defaultView.matchMedia = makeMatchMediaStub(['(max-width: 640px)']);

    const mq = doc.defaultView.matchMedia('(max-width: 640px)');
    expect(mq.matches).toBe(true);
    expect(
      doc.defaultView.matchMedia('(min-width: 1025px)').matches
    ).toBe(false);
  });

  // test_case 6
  test('desktop viewport: .nav-toggle is hidden, .nav-menu visible', () => {
    const desktop = blocks.find(
      (b) => /min-width:\s*1025px/.test(b.query) && !/max-width/.test(b.query)
    );
    expect(desktop).toBeDefined();

    const toggleBody = extractRuleBody(desktop.body, '.nav-toggle');
    expect(toggleBody).not.toBeNull();
    expect(getProp(toggleBody, 'display')).toBe('none');

    const menuBody = extractRuleBody(desktop.body, '.nav-menu');
    expect(menuBody).not.toBeNull();
    const menuDisplay = getProp(menuBody, 'display');
    expect(menuDisplay).toBeTruthy();
    expect(menuDisplay).not.toBe('none');

    // matchMedia for desktop should report matches=true.
    const doc = loadFullPage();
    doc.defaultView.matchMedia = makeMatchMediaStub(['(min-width: 1025px)']);

    expect(
      doc.defaultView.matchMedia('(min-width: 1025px)').matches
    ).toBe(true);
    expect(
      doc.defaultView.matchMedia('(max-width: 640px)').matches
    ).toBe(false);
  });

  // test_case 7
  test('mobile viewport + initNavigation: clicking .nav-toggle opens .nav-menu', () => {
    const doc = loadPartial('src/components/navigation/navigation.html');
    doc.defaultView.matchMedia = makeMatchMediaStub(['(max-width: 640px)']);

    // Prevent the script's auto-init so the test can drive it deterministically.
    doc.MirdbNavigationSkipAutoInit = true;
    delete require.cache[require.resolve('../../src/scripts/navigation.js')];
    require('../../src/scripts/navigation.js');
    window.MirdbNavigation.initNavigation(doc);

    const toggle = doc.querySelector('.nav-toggle');
    const menu = doc.getElementById('nav-menu');
    expect(toggle).not.toBeNull();
    expect(menu).not.toBeNull();

    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(menu.classList.contains('open')).toBe(false);

    toggle.click();

    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(menu.classList.contains('open')).toBe(true);
  });
});
