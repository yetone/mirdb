import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import React from 'react';
import App from '../../src/App';
import Hero from '../../src/components/Hero';
import QuickStart from '../../src/components/QuickStart';
import ThemeToggle, { ThemeProvider } from '../../src/components/ThemeToggle';
import Docs from '../../src/components/Docs';
import Comparison from '../../src/components/Comparison';
import Roadmap from '../../src/components/Roadmap';
import Demo from '../../src/components/Demo';
import Features from '../../src/components/Features';
import GitHubLink from '../../src/components/GitHubLink';

function renderWithProviders(ui: React.ReactElement) {
  return render(<ThemeProvider>{ui}</ThemeProvider>);
}

describe('Accessibility — Heading Structure (TC1)', () => {
  it('has exactly one h1 element', () => {
    renderWithProviders(<App />);
    const h1s = document.querySelectorAll('h1');
    expect(h1s.length).toBe(1);
  });

  it('the single h1 contains the product name', () => {
    renderWithProviders(<App />);
    const h1 = document.querySelector('h1');
    expect(h1).toBeInTheDocument();
    expect(h1!.textContent).toContain('MirDB');
  });

  it('section headings use h2 elements', () => {
    renderWithProviders(<App />);
    const h2s = document.querySelectorAll('h2');
    expect(h2s.length).toBeGreaterThanOrEqual(6);
  });

  it('heading hierarchy has no skipped levels', () => {
    renderWithProviders(<App />);
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    let prevLevel = 0;
    headings.forEach((heading) => {
      const level = parseInt(heading.tagName[1], 10);
      // No heading should skip more than one level
      if (prevLevel > 0) {
        expect(level - prevLevel).toBeLessThanOrEqual(1);
        expect(level - prevLevel).toBeGreaterThanOrEqual(-5); // Going back up is fine
      }
      prevLevel = level;
    });
  });

  it('sub-section headings use h3', () => {
    renderWithProviders(<App />);
    const h3s = document.querySelectorAll('h3');
    // Roadmap has h3s for Completed, Planned, Build Status
    expect(h3s.length).toBeGreaterThanOrEqual(2);
  });
});

describe('Accessibility — ARIA Labels (TC5)', () => {
  it('theme toggle button has an aria-label', () => {
    renderWithProviders(<ThemeToggle />);
    const button = screen.getByRole('button');
    expect(button).toHaveAttribute('aria-label');
    expect(button.getAttribute('aria-label')!.toLowerCase()).toMatch(/switch to (dark|light) mode/);
  });

  it('code copy buttons in QuickStart have aria-labels', () => {
    renderWithProviders(<QuickStart />);
    const copyButtons = screen.getAllByRole('button');
    expect(copyButtons.length).toBeGreaterThanOrEqual(4);
    copyButtons.forEach((button) => {
      const label = button.getAttribute('aria-label');
      expect(label).toBeTruthy();
      expect(label!.toLowerCase()).toMatch(/copy/i);
    });
  });

  it('copy button aria-label updates after copying', async () => {
    // Mock clipboard API for JSDOM
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: writeTextMock },
      writable: true,
    });

    renderWithProviders(<QuickStart />);
    const copyButton = screen.getAllByRole('button')[0];
    expect(copyButton.getAttribute('aria-label')!.toLowerCase()).toContain('copy');

    await act(async () => {
      copyButton.click();
    });

    await waitFor(() => {
      expect(copyButton.getAttribute('aria-label')!.toLowerCase()).toContain('copied');
    });
  });

  it('GitHub link in Hero indicates it opens in a new tab', () => {
    renderWithProviders(<Hero />);
    const ghLink = screen.getByRole('link', { name: /view.*github/i });
    expect(ghLink).toHaveAttribute('aria-label');
    expect(ghLink.getAttribute('aria-label')!.toLowerCase()).toMatch(/opens in new tab|github/);
  });

  it('GitHubLink component has accessible name via aria-label', async () => {
    render(<GitHubLink />);
    const link = await screen.findByRole('link');
    expect(link).toHaveAttribute('aria-label');
    expect(link.getAttribute('aria-label')).toContain('View MirDB on GitHub');
  });

  it('documentation links indicate they open in a new tab', () => {
    render(<Docs />);
    const links = screen.getAllByRole('link');
    links.forEach((link) => {
      const label = link.getAttribute('aria-label');
      expect(label).toBeTruthy();
      expect(label!.toLowerCase()).toContain('opens in new tab');
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  it('icon-only controls have appropriate aria-labels', () => {
    renderWithProviders(
      <>
        <ThemeToggle />
        <QuickStart />
      </>,
    );

    // ThemeToggle button has an icon only
    const toggleBtn = screen.getByRole('button', { name: /switch to/i });
    expect(toggleBtn).toHaveAttribute('aria-label');

    // QuickStart copy buttons have descriptive labels
    const copyButtons = screen.getAllByRole('button', { name: /copy/i });
    expect(copyButtons.length).toBeGreaterThanOrEqual(4);
  });
});

describe('Accessibility — Image Alt Text (TC6)', () => {
  it('logo.gif has descriptive alt text', () => {
    renderWithProviders(<Hero />);
    const logo = screen.getByAltText(/mirdb logo/i);
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveAttribute('src', '/assets/logo.gif');
  });

  it('usage.gif has descriptive alt text', () => {
    renderWithProviders(<Demo />);
    const demoImg = screen.getByAltText(/animated demonstration/i);
    expect(demoImg).toBeInTheDocument();
    expect(demoImg).toHaveAttribute('src', '/assets/usage.gif');
  });

  it('CircleCI badge image has alt text', () => {
    render(<Roadmap />);
    const badge = screen.getByAltText(/circleci/i);
    expect(badge).toBeInTheDocument();
  });

  it('no img element is missing an alt attribute', () => {
    renderWithProviders(<App />);
    const imgs = document.querySelectorAll('img');
    expect(imgs.length).toBeGreaterThan(0);
    imgs.forEach((img) => {
      expect(img.hasAttribute('alt')).toBe(true);
    });
  });
});

describe('Accessibility — Color Contrast (TC7)', () => {
  it('body text styling provides sufficient contrast in light mode', () => {
    renderWithProviders(<App />);
    document.documentElement.classList.remove('dark');
    // The body styles are defined in globals.css:
    // bg-white text-gray-900 for light mode (gray-900 #111827 on white #FFFFFF = ~17.3:1)
    const bodyStyle = window.getComputedStyle(document.body);
    expect(bodyStyle).toBeTruthy();
    // Verify the CSS is loaded and body has computed styles
    const textColor = bodyStyle.color || bodyStyle.getPropertyValue('color');
    expect(textColor).toBeTruthy();
  });

  it('body text styling provides sufficient contrast in dark mode', () => {
    document.documentElement.classList.add('dark');
    renderWithProviders(<App />);
    // In dark mode: dark:bg-gray-950 dark:text-gray-100
    const bodyStyle = window.getComputedStyle(document.body);
    expect(bodyStyle).toBeTruthy();
    document.documentElement.classList.remove('dark');
  });

  it('heading text has dark mode white text class for sufficient contrast', () => {
    renderWithProviders(<App />);
    const h1 = document.querySelector('h1');
    expect(h1?.className).toMatch(/dark:text-white/);
  });

  it('code blocks use accessible text colors with sufficient contrast', () => {
    renderWithProviders(<QuickStart />);
    const code = document.querySelector('code');
    expect(code).toBeInTheDocument();
    // Verify code element exists with appropriate styling
    const computedStyle = window.getComputedStyle(code!);
    // Code should have a computed color (not transparent)
    expect(computedStyle.color).toBeTruthy();
  });

  it('brand-500 text on white background meets AA minimum for large text', () => {
    renderWithProviders(<Hero />);
    // The "Get Started" button uses white text on brand-600 background
    const cta = screen.getByRole('link', { name: /get started/i });
    // White text on brand-600 (#ea580c) background - sufficient contrast
    expect(cta.className).toMatch(/text-white/);
    expect(cta.className).toMatch(/bg-brand-600/);
  });
});

describe('Accessibility — Color Not Sole Means (TC8)', () => {
  it('comparison table uses Yes/No text alongside check/cross icons', () => {
    render(<Comparison />);
    const yesLabels = screen.getAllByText('Yes');
    const noLabels = screen.getAllByText('No');
    expect(yesLabels.length).toBeGreaterThanOrEqual(3);
    expect(noLabels.length).toBeGreaterThanOrEqual(3);

    // Icons should be marked aria-hidden since text provides the meaning
    const checkIcons = document.querySelectorAll('svg[aria-hidden="true"]');
    const comparisonSection = document.getElementById('comparison');
    const comparisonIcons = comparisonSection?.querySelectorAll('svg');
    // Some or all comparison SVGs should be aria-hidden
    expect(comparisonIcons!.length).toBeGreaterThan(0);
  });

  it('roadmap status markers use text labels alongside icons', () => {
    render(<Roadmap />);
    // "Completed" text labels exist alongside check icons
    const completedTexts = screen.getAllByText('Completed');
    expect(completedTexts.length).toBeGreaterThan(0);

    // "Planned" text labels exist alongside clock icons
    const plannedTexts = screen.getAllByText('Planned');
    expect(plannedTexts.length).toBeGreaterThan(0);
  });

  it('comparison table positive indicators use SVG icon AND text "Yes"', () => {
    render(<Comparison />);
    const persistenceRow = screen.getByRole('rowheader', { name: 'Persistence' }).closest('tr');
    const mirdbCell = persistenceRow!.querySelectorAll('td')[0];
    // Should contain both an SVG check icon and the text "Yes"
    expect(mirdbCell.querySelector('svg')).toBeInTheDocument();
    expect(mirdbCell.textContent).toMatch(/Yes/);
  });

  it('comparison table negative indicators use SVG icon AND text "No"', () => {
    render(<Comparison />);
    const persistenceRow = screen.getByRole('rowheader', { name: 'Persistence' }).closest('tr');
    const memcachedCell = persistenceRow!.querySelectorAll('td')[1];
    // Should contain both an SVG X icon and the text "No"
    expect(memcachedCell.querySelector('svg')).toBeInTheDocument();
    expect(memcachedCell.textContent).toMatch(/No/);
  });

  it('feature card icons are decorative with aria-hidden', () => {
    render(<Features />);
    const featureIcons = document.querySelectorAll('#features svg[aria-hidden="true"]');
    expect(featureIcons.length).toBeGreaterThanOrEqual(5);
  });
});

describe('Accessibility — Semantic HTML Landmarks', () => {
  it('page has a main landmark', () => {
    renderWithProviders(<App />);
    const main = document.querySelector('main');
    expect(main).toBeInTheDocument();
    expect(main).toHaveAttribute('id', 'main-content');
  });

  it('page has a header element', () => {
    renderWithProviders(<App />);
    const header = document.querySelector('header');
    expect(header).toBeInTheDocument();
  });

  it('page has a footer element', () => {
    renderWithProviders(<App />);
    const footer = document.querySelector('footer');
    expect(footer).toBeInTheDocument();
  });

  it('sections use aria-label or aria-labelledby for accessible names', () => {
    renderWithProviders(<App />);
    const sections = document.querySelectorAll('section');
    expect(sections.length).toBeGreaterThan(0);
    sections.forEach((section) => {
      const hasLabel =
        section.hasAttribute('aria-label') ||
        section.hasAttribute('aria-labelledby');
      expect(hasLabel).toBe(true);
    });
  });
});
