/**
 * Navigation integration tests — Scenario 4.
 *
 * Validates smooth-scroll behavior, mobile hamburger toggle,
 * and keyboard dismissal. Each `test` corresponds to a test_case
 * id in .something/scenario.json.
 */

const { loadPartial } = require('../helpers/dom');

describe('Navigation flow', () => {
  // test_case 5
  test('clicking internal anchor triggers smooth scroll and prevents default jump', () => {
    const doc = loadPartial('src/components/navigation/navigation.html');

    // Inject a target section so scrollIntoView has something to scroll to
    const targetSection = doc.createElement('section');
    targetSection.id = 'features';
    // jsdom doesn't implement scrollIntoView, so add a mock
    targetSection.scrollIntoView = jest.fn();
    doc.body.appendChild(targetSection);

    // Prevent auto-init and bind manually
    doc.MirdbNavigationSkipAutoInit = true;
    delete require.cache[require.resolve('../../src/scripts/navigation.js')];
    require('../../src/scripts/navigation.js');
    window.MirdbNavigation.initNavigation(doc);

    const scrollSpy = jest.spyOn(targetSection, 'scrollIntoView');
    const anchor = doc.querySelector('a[href="#features"]');
    expect(anchor).not.toBeNull();

    const clickEvent = new doc.defaultView.Event('click', {
      bubbles: true,
      cancelable: true,
    });
    anchor.dispatchEvent(clickEvent);

    expect(clickEvent.defaultPrevented).toBe(true);
    expect(scrollSpy).toHaveBeenCalledTimes(1);
    expect(scrollSpy).toHaveBeenCalledWith(
      expect.objectContaining({ behavior: 'smooth' })
    );

    scrollSpy.mockRestore();
  });

  // test_case 6
  test('clicking .nav-toggle once opens the menu', () => {
    const doc = loadPartial('src/components/navigation/navigation.html');

    doc.MirdbNavigationSkipAutoInit = true;
    delete require.cache[require.resolve('../../src/scripts/navigation.js')];
    require('../../src/scripts/navigation.js');
    window.MirdbNavigation.initNavigation(doc);

    const toggle = doc.querySelector('.nav-toggle');
    const menu = doc.getElementById('nav-menu');

    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(menu.classList.contains('open')).toBe(false);

    toggle.click();

    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(menu.classList.contains('open')).toBe(true);
  });

  // test_case 7
  test('clicking .nav-toggle a second time closes the menu', () => {
    const doc = loadPartial('src/components/navigation/navigation.html');

    doc.MirdbNavigationSkipAutoInit = true;
    delete require.cache[require.resolve('../../src/scripts/navigation.js')];
    require('../../src/scripts/navigation.js');
    window.MirdbNavigation.initNavigation(doc);

    const toggle = doc.querySelector('.nav-toggle');
    const menu = doc.getElementById('nav-menu');

    // Open first
    toggle.click();
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(menu.classList.contains('open')).toBe(true);

    // Close second
    toggle.click();
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(menu.classList.contains('open')).toBe(false);
  });

  // test_case 8
  test('pressing Escape closes an open mobile menu', () => {
    const doc = loadPartial('src/components/navigation/navigation.html');

    doc.MirdbNavigationSkipAutoInit = true;
    delete require.cache[require.resolve('../../src/scripts/navigation.js')];
    require('../../src/scripts/navigation.js');
    window.MirdbNavigation.initNavigation(doc);

    const toggle = doc.querySelector('.nav-toggle');
    const menu = doc.getElementById('nav-menu');

    // Open the menu
    toggle.click();
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(menu.classList.contains('open')).toBe(true);

    // Dispatch Escape key
    const escapeEvent = new doc.defaultView.KeyboardEvent('keydown', {
      key: 'Escape',
      bubbles: true,
    });
    doc.dispatchEvent(escapeEvent);

    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(menu.classList.contains('open')).toBe(false);
  });
});
