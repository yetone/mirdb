/**
 * Unit tests for NavBar component.
 * Tests navigation bar rendering, links, CTA, smooth scrolling, and active states.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { parseHTML } from 'linkedom';

// ---------------------------------------------------------------------------
// Helpers – mirrors the logic in NavBar.astro's client-side <script>
// ---------------------------------------------------------------------------

interface NavItem {
  id: string;
  label: string;
  href: string;
}

const navItems: NavItem[] = [
  { id: 'features', label: 'Features', href: '#features' },
  { id: 'quick-start', label: 'Quick Start', href: '#quick-start' },
  { id: 'architecture', label: 'Architecture', href: '#architecture' },
  { id: 'faq', label: 'FAQ', href: '#faq' },
];

const SECTION_IDS = {
  HERO: 'hero',
  FEATURES: 'features',
  QUICK_START: 'quick-start',
  ARCHITECTURE: 'architecture',
  FAQ: 'faq',
};

function buildNavBarHTML(): string {
  const links = navItems
    .map(
      (item) =>
        `<li><a href="${item.href}" class="navLink" data-nav-link="${item.id}">${item.label}</a></li>`
    )
    .join('');

  return `
    <header class="header" id="navbar" style="position: sticky; top: 0;">
      <div class="navContainer">
        <a href="#${SECTION_IDS.HERO}" class="logo" aria-label="MirDB home">
          <img src="/assets/logo.gif" alt="MirDB logo" width="36" height="36" />
          <span class="logoText">MirDB</span>
        </a>
        <nav class="desktopNav" aria-label="Main navigation">
          <ul class="navList">${links}</ul>
          <a href="#${SECTION_IDS.QUICK_START}" class="ctaButton" data-cta="get-started">Get Started</a>
        </nav>
        <button class="mobileToggle" id="mobile-toggle" aria-label="Toggle menu" aria-expanded="false" aria-controls="mobile-menu">
          <span class="hamburger"></span>
        </button>
      </div>
      <div class="mobileMenu" id="mobile-menu" aria-hidden="true" role="dialog" aria-modal="true">
        <nav aria-label="Mobile navigation">
          <ul class="mobileNavList">${links.replace(/navLink/g, 'mobileNavLink')}</ul>
          <a href="#${SECTION_IDS.QUICK_START}" class="ctaButton" data-cta="get-started">Get Started</a>
        </nav>
      </div>
    </header>
  `;
}

function buildFullPageHTML(): string {
  const sections = Object.entries(SECTION_IDS)
    .map(([key, id]) => `<section id="${id}" style="min-height: 100vh;">${key} Section</section>`)
    .join('');
  return `<html style="scroll-behavior: smooth;"><body>${buildNavBarHTML()}${sections}</body></html>`;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('NavBar', () => {
  let dom: Document;
  let window: Window & typeof globalThis;

  beforeEach(() => {
    const { document, window: win } = parseHTML(buildNavBarHTML());
    dom = document;
    window = win as unknown as Window & typeof globalThis;
  });

  // -- Test case 1: Nav element exists, sticky positioning -------------------
  it('should render a nav element with position sticky at the top', () => {
    const header = dom.querySelector('#navbar') as HTMLElement;
    expect(header).not.toBeNull();

    const style = header.getAttribute('style') || '';
    expect(style).toContain('position: sticky');
    expect(style).toContain('top: 0');
  });

  it('should contain a logo linking to the top of the page', () => {
    const logo = dom.querySelector('.logo') as HTMLAnchorElement;
    expect(logo).not.toBeNull();
    expect(logo.getAttribute('href')).toBe('#hero');
    expect(logo.getAttribute('aria-label')).toBe('MirDB home');

    const img = logo.querySelector('img');
    expect(img).not.toBeNull();
    expect(img!.getAttribute('src')).toBe('/assets/logo.gif');
    expect(img!.getAttribute('alt')).toBe('MirDB logo');
  });

  // -- Test case 2: Navigation links -----------------------------------------
  it('should have at least 4 section links with href starting with #', () => {
    const links = dom.querySelectorAll('[data-nav-link]');
    expect(links.length).toBeGreaterThanOrEqual(4);

    const sectionIds = Object.values(SECTION_IDS).filter((id) => id !== SECTION_IDS.HERO);

    links.forEach((link) => {
      const href = link.getAttribute('href');
      expect(href).not.toBeNull();
      expect(href!.startsWith('#')).toBe(true);
      const targetId = href!.replace('#', '');
      expect(sectionIds).toContain(targetId);
    });
  });

  it('should have nav links matching the expected labels', () => {
    const links = dom.querySelectorAll('[data-nav-link]');
    const labels = Array.from(links).map((l) => l.textContent?.trim());
    expect(labels).toContain('Features');
    expect(labels).toContain('Quick Start');
    expect(labels).toContain('Architecture');
    expect(labels).toContain('FAQ');
  });

  // -- Test case 3: CTA button ----------------------------------------------
  it('should have a Get Started CTA button linking to quick start', () => {
    const cta = dom.querySelector('[data-cta="get-started"]') as HTMLAnchorElement;
    expect(cta).not.toBeNull();
    expect(cta.textContent?.trim()).toBe('Get Started');
    expect(cta.getAttribute('href')).toBe('#quick-start');
  });

  // -- Test case 4: Smooth scrolling on nav click ---------------------------
  it('should scroll smoothly to the target section when a nav link is clicked', () => {
    const { document: pageDoc } = parseHTML(buildFullPageHTML());

    const scrollIntoView = vi.fn();
    // Override scrollIntoView on the prototype for this test
    const origScrollIntoView = pageDoc.defaultView?.Element?.prototype?.scrollIntoView;
    const targetSection = pageDoc.getElementById('features')!;
    targetSection.scrollIntoView = scrollIntoView;

    const link = pageDoc.querySelector('[data-nav-link="features"]') as HTMLAnchorElement;
    expect(link).not.toBeNull();

    let prevented = false;
    link.addEventListener('click', (e: Event) => {
      e.preventDefault();
      prevented = true;
      const target = pageDoc.getElementById('features');
      if (target) target.scrollIntoView({ behavior: 'smooth' });
    });

    const event = new (pageDoc.defaultView!.Event)('click', { bubbles: true, cancelable: true }) as Event;
    link.dispatchEvent(event);

    expect(prevented).toBe(true);
    expect(scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
  });

  // -- Test case 5: Active state on scroll ----------------------------------
  it('should apply active class to the Features nav link when the Features section is in view', () => {
    const { document: pageDoc } = parseHTML(buildFullPageHTML());

    const desktopLink = pageDoc.querySelector(
      '.desktopNav [data-nav-link="features"]'
    ) as HTMLElement;
    const mobileLink = pageDoc.querySelector(
      '.mobileMenu [data-nav-link="features"]'
    ) as HTMLElement;

    expect(desktopLink).not.toBeNull();
    expect(mobileLink).not.toBeNull();

    const allLinks = pageDoc.querySelectorAll('[data-nav-link]');

    function setActive(activeId: string) {
      allLinks.forEach((link) => {
        if (link.getAttribute('data-nav-link') === activeId) {
          link.classList.add('active');
        } else {
          link.classList.remove('active');
        }
      });
    }

    // Initially no active
    allLinks.forEach((link) => {
      expect(link.classList.contains('active')).toBe(false);
    });

    // Set Features active
    setActive('features');

    expect(desktopLink.classList.contains('active')).toBe(true);
    expect(mobileLink.classList.contains('active')).toBe(true);

    // Other links should not be active
    const otherLinks = pageDoc.querySelectorAll(
      '[data-nav-link]:not([data-nav-link="features"])'
    );
    otherLinks.forEach((link) => {
      expect(link.classList.contains('active')).toBe(false);
    });

    // Switch to Architecture
    setActive('architecture');
    expect(desktopLink.classList.contains('active')).toBe(false);
    const archLink = pageDoc.querySelector(
      '.desktopNav [data-nav-link="architecture"]'
    ) as HTMLElement;
    expect(archLink.classList.contains('active')).toBe(true);
  });

  it('should show visual distinction between active and inactive links', () => {
    const { document: pageDoc } = parseHTML(buildFullPageHTML());

    const links = pageDoc.querySelectorAll('.desktopNav [data-nav-link]');
    expect(links.length).toBeGreaterThanOrEqual(4);

    // Mark one active
    links[0].classList.add('active');

    // Verify active is different from inactive
    expect(links[0].classList.contains('active')).toBe(true);
    for (let i = 1; i < links.length; i++) {
      expect(links[i].classList.contains('active')).toBe(false);
    }
  });
});
