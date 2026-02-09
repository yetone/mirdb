/**
 * Unit tests for Hero component.
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Tests the hero section rendering including title, tagline,
 * description, logo, and CTA buttons.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHero } from '../../../src/components/Hero';

describe('Hero Component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderHero();
  });

  it('renders a section element with proper id and class', () => {
    expect(section.tagName).toBe('SECTION');
    expect(section.id).toBe('hero');
    expect(section.className).toContain('hero-section');
  });

  it('contains project title "MirDB"', () => {
    const title = section.querySelector('.hero-title');
    expect(title).not.toBeNull();
    expect(title?.textContent).toBe('MirDB');
    expect(title?.tagName).toBe('H1');
    expect(title?.id).toBe('hero-title');
  });

  it('contains tagline "A Persistent Key-Value Store with Memcached Protocol"', () => {
    const tagline = section.querySelector('.hero-tagline');
    expect(tagline).not.toBeNull();
    expect(tagline?.textContent).toBe('A Persistent Key-Value Store with Memcached Protocol');
  });

  it('contains brief description explaining MirDB purpose', () => {
    const description = section.querySelector('.hero-description');
    expect(description).not.toBeNull();
    const text = description?.textContent || '';
    expect(text.length).toBeGreaterThan(50);
    expect(text.toLowerCase()).toContain('key-value');
    expect(text.toLowerCase()).toContain('rust');
    expect(text.toLowerCase()).toContain('lsm');
  });

  it('displays logo.gif animation', () => {
    const logo = section.querySelector('.hero-logo-image') as HTMLImageElement;
    expect(logo).not.toBeNull();
    expect(logo.tagName).toBe('IMG');
    expect(logo.src).toContain('logo.gif');
    expect(logo.alt).toBe('MirDB Logo');
  });

  it('contains "Get Started" CTA button', () => {
    const ctaContainer = section.querySelector('.hero-cta');
    expect(ctaContainer).not.toBeNull();

    const buttons = ctaContainer?.querySelectorAll('.hero-btn');
    expect(buttons?.length).toBeGreaterThanOrEqual(2);

    const getStartedBtn = Array.from(buttons || []).find(
      (btn) => btn.textContent === 'Get Started'
    );
    expect(getStartedBtn).not.toBeNull();
    expect(getStartedBtn?.classList.contains('hero-btn-primary')).toBe(true);
  });

  it('contains "View on GitHub" CTA button with correct link', () => {
    const ctaContainer = section.querySelector('.hero-cta');
    const buttons = ctaContainer?.querySelectorAll('.hero-btn');

    const githubBtn = Array.from(buttons || []).find(
      (btn) => btn.textContent === 'View on GitHub'
    ) as HTMLAnchorElement;

    expect(githubBtn).not.toBeNull();
    expect(githubBtn.href).toContain('github.com');
    expect(githubBtn.href).toContain('mirdb');
    expect(githubBtn.target).toBe('_blank');
    expect(githubBtn.rel).toContain('noopener');
  });

  it('has proper heading structure for accessibility', () => {
    const heading = section.querySelector('h1');
    expect(heading).not.toBeNull();
    expect(heading?.id).toBe('hero-title');

    expect(section.getAttribute('aria-labelledby')).toBe('hero-title');
  });

  it('Get Started button has href to quick-start section', () => {
    const getStartedBtn = section.querySelector('.hero-btn-primary') as HTMLAnchorElement;
    expect(getStartedBtn).not.toBeNull();
    expect(getStartedBtn.href).toContain('#quick-start');
  });

  it('has container with proper classes', () => {
    const container = section.querySelector('.container');
    expect(container).not.toBeNull();
    expect(container?.classList.contains('hero-container')).toBe(true);
  });

  it('logo container exists', () => {
    const logoContainer = section.querySelector('.hero-logo');
    expect(logoContainer).not.toBeNull();
  });

  it('content container exists', () => {
    const content = section.querySelector('.hero-content');
    expect(content).not.toBeNull();
  });

  it('elements are in correct order', () => {
    const container = section.querySelector('.hero-container');
    const children = Array.from(container?.children || []);

    expect(children.length).toBe(2);
    expect(children[0].classList.contains('hero-logo')).toBe(true);
    expect(children[1].classList.contains('hero-content')).toBe(true);
  });

  it('all CTA buttons have hover-friendly styles class', () => {
    const buttons = section.querySelectorAll('.hero-btn');
    expect(buttons.length).toBeGreaterThanOrEqual(2);

    buttons.forEach((btn) => {
      expect(btn.classList.contains('hero-btn')).toBe(true);
    });
  });

  it('GitHub button opens in new tab securely', () => {
    const githubBtn = section.querySelector('.hero-btn-secondary') as HTMLAnchorElement;
    expect(githubBtn.target).toBe('_blank');
    expect(githubBtn.rel).toContain('noopener');
    expect(githubBtn.rel).toContain('noreferrer');
  });
});

describe('Hero Component - Smooth Scroll', () => {
  let scrollIntoViewMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    // Mock scrollIntoView which is not implemented in jsdom
    scrollIntoViewMock = vi.fn();
    Element.prototype.scrollIntoView = scrollIntoViewMock;
    document.body.innerHTML = '<div id="quick-start"></div>';
  });

  it('Get Started button click handler calls scrollIntoView', () => {
    const section = renderHero();
    const getStartedBtn = section.querySelector('.hero-btn-primary') as HTMLAnchorElement;

    const clickEvent = new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
    });
    getStartedBtn.dispatchEvent(clickEvent);

    expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
  });

  it('Get Started button click prevents default navigation', () => {
    const section = renderHero();
    const getStartedBtn = section.querySelector('.hero-btn-primary') as HTMLAnchorElement;

    const clickEvent = new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
    });

    const preventDefaultSpy = vi.spyOn(clickEvent, 'preventDefault');
    getStartedBtn.dispatchEvent(clickEvent);

    expect(preventDefaultSpy).toHaveBeenCalled();
  });
});
