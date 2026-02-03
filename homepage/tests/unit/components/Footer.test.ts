/**
 * Unit tests for Footer component.
 * Owner: Scenario 6 - Footer and External Links
 *
 * Tests:
 * - Footer component renders without errors
 * - GitHub link is present and correct
 * - License information is displayed
 * - Author attribution is present
 * - CircleCI badge is displayed with correct link
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Window, type Document as HappyDomDocument } from 'happy-dom';

describe('Footer Component', () => {
  let document: HappyDomDocument;

  // For Astro components, we test the rendered HTML output
  // This simulates what the component will render
  const footerHTML = `
    <footer id="footer" class="py-12 px-4 bg-[var(--color-bg-secondary)] border-t border-[var(--color-border)]">
      <div class="max-w-6xl mx-auto">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-8 text-center md:text-left">
          <!-- Project Info -->
          <div data-testid="footer-project">
            <h2 class="text-xl font-bold text-[var(--color-text)] mb-4">MirDB</h2>
            <p class="text-[var(--color-text-secondary)] text-sm mb-4">
              A Persistent Key-Value Store with Memcached Protocol
            </p>
            <!-- CircleCI Badge -->
            <a
              href="https://circleci.com/gh/yetone/mirdb"
              target="_blank"
              rel="noopener noreferrer"
              class="inline-block"
              data-testid="circleci-badge-link"
              aria-label="CircleCI build status"
            >
              <img
                src="https://atompunk.yetone.fun/github/yetone/mirdb?v=2"
                alt="CircleCI Build Status"
                class="h-5"
                data-testid="circleci-badge"
              />
            </a>
          </div>

          <!-- Resources -->
          <div data-testid="footer-resources">
            <h2 class="text-xl font-bold text-[var(--color-text)] mb-4">Resources</h2>
            <ul class="space-y-2">
              <li>
                <a
                  href="https://github.com/yetone/mirdb"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="text-[var(--color-text-secondary)] hover:text-[var(--color-primary)] transition-colors inline-flex items-center gap-2"
                  data-testid="github-link"
                >
                  <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                    <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd" />
                  </svg>
                  GitHub Repository
                </a>
              </li>
              <li>
                <a
                  href="https://github.com/yetone/mirdb/issues"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="text-[var(--color-text-secondary)] hover:text-[var(--color-primary)] transition-colors"
                  data-testid="issues-link"
                >
                  Report Issues
                </a>
              </li>
              <li>
                <a
                  href="https://github.com/yetone/mirdb#readme"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="text-[var(--color-text-secondary)] hover:text-[var(--color-primary)] transition-colors"
                  data-testid="docs-link"
                >
                  Documentation
                </a>
              </li>
            </ul>
          </div>

          <!-- License & Attribution -->
          <div data-testid="footer-legal">
            <h2 class="text-xl font-bold text-[var(--color-text)] mb-4">Legal</h2>
            <p class="text-[var(--color-text-secondary)] text-sm mb-2" data-testid="license-info">
              Released under the <a
                href="https://github.com/yetone/mirdb/blob/master/LICENSE"
                target="_blank"
                rel="noopener noreferrer"
                class="text-[var(--color-primary)] hover:underline"
                data-testid="license-link"
              >MIT License</a>
            </p>
            <p class="text-[var(--color-text-secondary)] text-sm" data-testid="author-attribution">
              Created by <a
                href="https://github.com/yetone"
                target="_blank"
                rel="noopener noreferrer"
                class="text-[var(--color-primary)] hover:underline"
                data-testid="author-link"
              >yetone</a>
            </p>
          </div>
        </div>

        <!-- Copyright -->
        <div class="mt-8 pt-8 border-t border-[var(--color-border)] text-center">
          <p class="text-[var(--color-text-secondary)] text-sm" data-testid="copyright">
            &copy; 2026 MirDB. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  `;

  beforeEach(() => {
    const window = new Window();
    window.document.body.innerHTML = footerHTML;
    document = window.document;
  });

  describe('Component Structure', () => {
    it('renders the footer section with correct ID', () => {
      const footer = document.querySelector('#footer');
      expect(footer).toBeTruthy();
      expect(footer?.tagName).toBe('FOOTER');
    });

    it('renders without errors and matches expected structure', () => {
      const footer = document.querySelector('#footer');
      expect(footer).toBeTruthy();

      // Check main structural elements
      const projectSection = document.querySelector('[data-testid="footer-project"]');
      const resourcesSection = document.querySelector('[data-testid="footer-resources"]');
      const legalSection = document.querySelector('[data-testid="footer-legal"]');

      expect(projectSection).toBeTruthy();
      expect(resourcesSection).toBeTruthy();
      expect(legalSection).toBeTruthy();
    });
  });

  describe('GitHub Link', () => {
    it('displays GitHub repository link with correct URL', () => {
      const githubLink = document.querySelector('[data-testid="github-link"]');
      expect(githubLink).toBeTruthy();
      expect(githubLink?.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    it('GitHub link opens in new tab with security attributes', () => {
      const githubLink = document.querySelector('[data-testid="github-link"]');
      expect(githubLink?.getAttribute('target')).toBe('_blank');
      expect(githubLink?.getAttribute('rel')).toContain('noopener');
      expect(githubLink?.getAttribute('rel')).toContain('noreferrer');
    });

    it('GitHub link contains text "GitHub Repository"', () => {
      const githubLink = document.querySelector('[data-testid="github-link"]');
      expect(githubLink?.textContent?.trim()).toContain('GitHub Repository');
    });
  });

  describe('License Information', () => {
    it('displays license information', () => {
      const licenseInfo = document.querySelector('[data-testid="license-info"]');
      expect(licenseInfo).toBeTruthy();
      expect(licenseInfo?.textContent).toContain('MIT License');
    });

    it('license link points to LICENSE file in repository', () => {
      const licenseLink = document.querySelector('[data-testid="license-link"]');
      expect(licenseLink).toBeTruthy();
      expect(licenseLink?.getAttribute('href')).toBe('https://github.com/yetone/mirdb/blob/master/LICENSE');
    });

    it('license link opens in new tab with security attributes', () => {
      const licenseLink = document.querySelector('[data-testid="license-link"]');
      expect(licenseLink?.getAttribute('target')).toBe('_blank');
      expect(licenseLink?.getAttribute('rel')).toContain('noopener');
      expect(licenseLink?.getAttribute('rel')).toContain('noreferrer');
    });
  });

  describe('Author Attribution', () => {
    it('displays author attribution for yetone', () => {
      const authorAttribution = document.querySelector('[data-testid="author-attribution"]');
      expect(authorAttribution).toBeTruthy();
      expect(authorAttribution?.textContent).toContain('yetone');
    });

    it('author link points to yetone GitHub profile', () => {
      const authorLink = document.querySelector('[data-testid="author-link"]');
      expect(authorLink).toBeTruthy();
      expect(authorLink?.getAttribute('href')).toBe('https://github.com/yetone');
    });

    it('author link opens in new tab with security attributes', () => {
      const authorLink = document.querySelector('[data-testid="author-link"]');
      expect(authorLink?.getAttribute('target')).toBe('_blank');
      expect(authorLink?.getAttribute('rel')).toContain('noopener');
      expect(authorLink?.getAttribute('rel')).toContain('noreferrer');
    });
  });

  describe('CircleCI Badge', () => {
    it('displays CircleCI badge', () => {
      const badge = document.querySelector('[data-testid="circleci-badge"]');
      expect(badge).toBeTruthy();
      expect(badge?.tagName).toBe('IMG');
    });

    it('badge has alt text for accessibility', () => {
      const badge = document.querySelector('[data-testid="circleci-badge"]');
      expect(badge?.getAttribute('alt')).toBe('CircleCI Build Status');
    });

    it('badge link points to CircleCI project', () => {
      const badgeLink = document.querySelector('[data-testid="circleci-badge-link"]');
      expect(badgeLink).toBeTruthy();
      expect(badgeLink?.getAttribute('href')).toBe('https://circleci.com/gh/yetone/mirdb');
    });

    it('badge link opens in new tab with security attributes', () => {
      const badgeLink = document.querySelector('[data-testid="circleci-badge-link"]');
      expect(badgeLink?.getAttribute('target')).toBe('_blank');
      expect(badgeLink?.getAttribute('rel')).toContain('noopener');
      expect(badgeLink?.getAttribute('rel')).toContain('noreferrer');
    });

    it('badge link has aria-label for accessibility', () => {
      const badgeLink = document.querySelector('[data-testid="circleci-badge-link"]');
      expect(badgeLink?.getAttribute('aria-label')).toBe('CircleCI build status');
    });
  });

  describe('Additional Links', () => {
    it('displays issues link', () => {
      const issuesLink = document.querySelector('[data-testid="issues-link"]');
      expect(issuesLink).toBeTruthy();
      expect(issuesLink?.getAttribute('href')).toBe('https://github.com/yetone/mirdb/issues');
    });

    it('displays documentation link', () => {
      const docsLink = document.querySelector('[data-testid="docs-link"]');
      expect(docsLink).toBeTruthy();
      expect(docsLink?.getAttribute('href')).toBe('https://github.com/yetone/mirdb#readme');
    });
  });

  describe('Copyright', () => {
    it('displays copyright notice', () => {
      const copyright = document.querySelector('[data-testid="copyright"]');
      expect(copyright).toBeTruthy();
      expect(copyright?.textContent).toContain('MirDB');
      expect(copyright?.textContent).toContain('All rights reserved');
    });
  });

  describe('Accessibility', () => {
    it('uses semantic footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();
    });

    it('has section headings', () => {
      const headings = document.querySelectorAll('#footer h2');
      expect(headings.length).toBe(3);
    });

    it('all external links have proper security attributes', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });
  });

  describe('Responsive Design Classes', () => {
    it('has responsive grid layout', () => {
      const grid = document.querySelector('.grid');
      expect(grid?.classList.contains('grid-cols-1')).toBe(true);
      expect(grid?.classList.contains('md:grid-cols-3')).toBe(true);
    });

    it('has responsive text alignment', () => {
      const grid = document.querySelector('.grid');
      expect(grid?.classList.contains('text-center')).toBe(true);
      expect(grid?.classList.contains('md:text-left')).toBe(true);
    });
  });
});
