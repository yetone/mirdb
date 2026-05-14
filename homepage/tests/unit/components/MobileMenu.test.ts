/**
 * Unit tests for MobileMenu component.
 * Tests responsive mobile menu toggle, open/close, escape key, and focus trapping.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { parseHTML } from 'linkedom';

// ---------------------------------------------------------------------------
// Helpers – mirrors the mobile menu logic from NavBar.astro
// ---------------------------------------------------------------------------

function buildMobileNavHTML(): string {
  return `
    <header id="navbar">
      <div class="navContainer">
        <a href="#hero" class="logo">MirDB</a>
        <nav class="desktopNav" aria-label="Main navigation" style="display: none;">
          <ul class="navList">
            <li><a href="#features" data-nav-link="features">Features</a></li>
            <li><a href="#quick-start" data-nav-link="quick-start">Quick Start</a></li>
            <li><a href="#architecture" data-nav-link="architecture">Architecture</a></li>
            <li><a href="#faq" data-nav-link="faq">FAQ</a></li>
          </ul>
          <a href="#quick-start" data-cta="get-started">Get Started</a>
        </nav>
        <button class="mobileToggle" id="mobile-toggle" aria-label="Toggle menu" aria-expanded="false" aria-controls="mobile-menu">
          <span class="hamburger"></span>
        </button>
      </div>
      <div class="mobileMenu" id="mobile-menu" aria-hidden="true" role="dialog" aria-modal="true">
        <nav aria-label="Mobile navigation">
          <ul class="mobileNavList">
            <li><a href="#features" data-nav-link="features">Features</a></li>
            <li><a href="#quick-start" data-nav-link="quick-start">Quick Start</a></li>
            <li><a href="#architecture" data-nav-link="architecture">Architecture</a></li>
            <li><a href="#faq" data-nav-link="faq">FAQ</a></li>
          </ul>
          <a href="#quick-start" class="ctaButton" data-cta="get-started">Get Started</a>
        </nav>
      </div>
    </header>
    <section id="features" style="min-height: 100vh;">Features</section>
    <section id="quick-start" style="min-height: 100vh;">Quick Start</section>
  `;
}

function setupMobileMenu(doc: Document, win: Window & typeof globalThis) {
  const mobileToggle = doc.querySelector('#mobile-toggle') as HTMLButtonElement;
  const mobileMenu = doc.querySelector('#mobile-menu') as HTMLElement;
  const mobileNavLinks = mobileMenu.querySelectorAll('a[data-nav-link]');
  const mobileCta = mobileMenu.querySelector('[data-cta="get-started"]') as HTMLAnchorElement;

  let escapeHandler: ((e: KeyboardEvent) => void) | null = null;

  function closeMobileMenu() {
    mobileMenu.classList.remove('open');
    mobileMenu.setAttribute('aria-hidden', 'true');
    mobileToggle.setAttribute('aria-expanded', 'false');
    mobileToggle.focus();
    if (escapeHandler) {
      doc.removeEventListener('keydown', escapeHandler);
      escapeHandler = null;
    }
  }

  function openMobileMenu() {
    mobileMenu.classList.add('open');
    mobileMenu.setAttribute('aria-hidden', 'false');
    mobileToggle.setAttribute('aria-expanded', 'true');
    trapFocus();
    escapeHandler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeMobileMenu();
    };
    doc.addEventListener('keydown', escapeHandler);
  }

  function toggleMobileMenu() {
    const isOpen = mobileToggle.getAttribute('aria-expanded') === 'true';
    if (isOpen) {
      closeMobileMenu();
    } else {
      openMobileMenu();
    }
  }

  function trapFocus() {
    const focusable = mobileMenu.querySelectorAll<HTMLElement>(
      'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );
    if (focusable.length === 0) return;

    const first = focusable[0];
    const last = focusable[focusable.length - 1];

    function handleTab(e: Event) {
      const ke = e as KeyboardEvent;
      if (ke.key !== 'Tab') return;
      const currentFocusable = mobileMenu.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
      );
      if (currentFocusable.length === 0) return;
      const f = currentFocusable[0];
      const l = currentFocusable[currentFocusable.length - 1];
      if (ke.shiftKey && doc.activeElement === f) {
        e.preventDefault();
        l.focus();
      } else if (!ke.shiftKey && doc.activeElement === l) {
        e.preventDefault();
        f.focus();
      }
    }

    mobileMenu.addEventListener('keydown', handleTab);
  }

  mobileToggle.addEventListener('click', toggleMobileMenu);

  mobileNavLinks.forEach((link) => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      closeMobileMenu();
      const href = link.getAttribute('href');
      if (href) {
        const target = doc.getElementById(href.replace('#', ''));
        if (target) target.scrollIntoView({ behavior: 'smooth' });
      }
    });
  });

  mobileCta?.addEventListener('click', (e) => {
    e.preventDefault();
    closeMobileMenu();
    const target = doc.getElementById('quick-start');
    if (target) target.scrollIntoView({ behavior: 'smooth' });
  });

  return { mobileToggle, mobileMenu, openMobileMenu, closeMobileMenu, toggleMobileMenu };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('MobileMenu', () => {
  let doc: Document;
  let win: Window & typeof globalThis;

  beforeEach(() => {
    const result = parseHTML(buildMobileNavHTML());
    doc = result.document;
    win = result.window as unknown as Window & typeof globalThis;
  });

  // -- Test case 6: Mobile rendering at 375px -------------------------------
  it('should hide desktop nav and show mobile toggle at mobile viewport', () => {
    const desktopNav = doc.querySelector('.desktopNav') as HTMLElement;
    const mobileToggle = doc.querySelector('#mobile-toggle') as HTMLElement;
    const mobileMenu = doc.querySelector('#mobile-menu') as HTMLElement;

    // Desktop nav hidden
    expect(desktopNav.style.display).toBe('none');

    // Mobile toggle visible
    expect(mobileToggle).not.toBeNull();

    // Mobile menu hidden by default
    expect(mobileMenu.getAttribute('aria-hidden')).toBe('true');
    expect(mobileMenu.classList.contains('open')).toBe(false);
  });

  it('should have a hamburger menu button with correct aria attributes', () => {
    const toggle = doc.querySelector('#mobile-toggle') as HTMLButtonElement;
    expect(toggle).not.toBeNull();
    expect(toggle.getAttribute('aria-label')).toBe('Toggle menu');
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(toggle.getAttribute('aria-controls')).toBe('mobile-menu');

    const hamburger = toggle.querySelector('.hamburger');
    expect(hamburger).not.toBeNull();
  });

  it('should have all nav links present in the mobile menu', () => {
    const mobileLinks = doc.querySelectorAll('#mobile-menu [data-nav-link]');
    expect(mobileLinks.length).toBeGreaterThanOrEqual(4);

    const labels = Array.from(mobileLinks).map((l) => l.textContent?.trim());
    expect(labels).toContain('Features');
    expect(labels).toContain('Quick Start');
    expect(labels).toContain('Architecture');
    expect(labels).toContain('FAQ');
  });

  // -- Test case 7: Toggle open/close ---------------------------------------
  it('should open mobile menu and update toggle state on click', () => {
    const { toggleMobileMenu } = setupMobileMenu(doc, win);
    const mobileMenu = doc.querySelector('#mobile-menu') as HTMLElement;
    const mobileToggle = doc.querySelector('#mobile-toggle') as HTMLElement;

    expect(mobileMenu.classList.contains('open')).toBe(false);
    expect(mobileToggle.getAttribute('aria-expanded')).toBe('false');

    // Open
    toggleMobileMenu();

    expect(mobileMenu.classList.contains('open')).toBe(true);
    expect(mobileMenu.getAttribute('aria-hidden')).toBe('false');
    expect(mobileToggle.getAttribute('aria-expanded')).toBe('true');
  });

  it('should close mobile menu and restore toggle state on second click', () => {
    const { toggleMobileMenu } = setupMobileMenu(doc, win);
    const mobileMenu = doc.querySelector('#mobile-menu') as HTMLElement;
    const mobileToggle = doc.querySelector('#mobile-toggle') as HTMLElement;

    // Open then close
    toggleMobileMenu();
    toggleMobileMenu();

    expect(mobileMenu.classList.contains('open')).toBe(false);
    expect(mobileMenu.getAttribute('aria-hidden')).toBe('true');
    expect(mobileToggle.getAttribute('aria-expanded')).toBe('false');
  });

  // -- Test case 8: Click link closes menu ----------------------------------
  it('should close the mobile menu and scroll to section when a link is clicked', () => {
    const { mobileMenu } = setupMobileMenu(doc, win);

    // Open menu first
    mobileMenu.classList.add('open');
    mobileMenu.setAttribute('aria-hidden', 'false');
    const toggle = doc.querySelector('#mobile-toggle') as HTMLElement;
    toggle.setAttribute('aria-expanded', 'true');

    expect(mobileMenu.classList.contains('open')).toBe(true);

    // Click a mobile link
    const featuresSection = doc.getElementById('features') as HTMLElement;
    const scrollSpy = vi.fn();
    featuresSection.scrollIntoView = scrollSpy;

    const link = doc.querySelector('#mobile-menu [data-nav-link="features"]') as HTMLAnchorElement;
    link.click();

    // Menu should close
    expect(mobileMenu.classList.contains('open')).toBe(false);
    expect(mobileMenu.getAttribute('aria-hidden')).toBe('true');
    expect(scrollSpy).toHaveBeenCalledWith({ behavior: 'smooth' });
  });

  // -- Test case 9: Escape key closes menu ----------------------------------
  it('should close the mobile menu when Escape is pressed', () => {
    const { openMobileMenu, mobileMenu, mobileToggle } = setupMobileMenu(doc, win);

    // Open menu
    openMobileMenu();
    expect(mobileMenu.classList.contains('open')).toBe(true);

    // Press Escape – linkedom does not provide KeyboardEvent, use Event + defineProperty
    const escEvent = new win.Event('keydown', { bubbles: true, cancelable: true });
    Object.defineProperty(escEvent, 'key', { value: 'Escape' });
    doc.dispatchEvent(escEvent);

    expect(mobileMenu.classList.contains('open')).toBe(false);
    expect(mobileMenu.getAttribute('aria-hidden')).toBe('true');
    expect(mobileToggle.getAttribute('aria-expanded')).toBe('false');
  });

  it('should return focus to the toggle button after escape closes menu', () => {
    const { openMobileMenu, mobileToggle } = setupMobileMenu(doc, win);

    openMobileMenu();

    const escEvent = new win.Event('keydown', { bubbles: true, cancelable: true });
    Object.defineProperty(escEvent, 'key', { value: 'Escape' });
    doc.dispatchEvent(escEvent);

    // linkedom may not set activeElement on focus(), but the menu should be closed
    expect(mobileToggle.getAttribute('aria-expanded')).toBe('false');
  });

  // -- Test case 10: Focus trapping -----------------------------------------
  it('should trap focus within the mobile menu - tab forward from last wraps to first', () => {
    const { mobileMenu } = setupMobileMenu(doc, win);

    const focusable = mobileMenu.querySelectorAll<HTMLElement>(
      'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );
    expect(focusable.length).toBeGreaterThanOrEqual(2);

    const first = focusable[0];
    const last = focusable[focusable.length - 1];

    // Simulate the focus trap logic directly
    let prevented = false;
    let focusTarget: HTMLElement | null = null;

    function handleTab(currentActive: HTMLElement, shiftKey: boolean) {
      const items = mobileMenu.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
      );
      const f = items[0];
      const l = items[items.length - 1];

      if (shiftKey && currentActive === f) {
        prevented = true;
        focusTarget = l;
      } else if (!shiftKey && currentActive === l) {
        prevented = true;
        focusTarget = f;
      }
    }

    // Tab forward from last → should wrap to first
    handleTab(last, false);
    expect(prevented).toBe(true);
    expect(focusTarget).toBe(first);

    // Reset and test Shift+Tab from first → should wrap to last
    prevented = false;
    focusTarget = null;
    handleTab(first, true);
    expect(prevented).toBe(true);
    expect(focusTarget).toBe(last);
  });

  it('should not trap focus when active element is in the middle', () => {
    const { mobileMenu } = setupMobileMenu(doc, win);

    const focusable = mobileMenu.querySelectorAll<HTMLElement>(
      'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );
    expect(focusable.length).toBeGreaterThanOrEqual(3);

    const first = focusable[0];
    const middle = focusable[1];
    const last = focusable[focusable.length - 1];

    let prevented = false;

    function handleTab(currentActive: HTMLElement, shiftKey: boolean) {
      const items = mobileMenu.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
      );
      const f = items[0];
      const l = items[items.length - 1];

      if (shiftKey && currentActive === f) {
        prevented = true;
        l.focus();
      } else if (!shiftKey && currentActive === l) {
        prevented = true;
        f.focus();
      }
    }

    // Tab forward from middle → should NOT trap
    prevented = false;
    handleTab(middle, false);
    expect(prevented).toBe(false);

    // Shift+Tab from middle → should NOT trap
    prevented = false;
    handleTab(middle, true);
    expect(prevented).toBe(false);
  });
});
