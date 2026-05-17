import { describe, it, expect, beforeAll, vi } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { useState } from 'react';
import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

import Home from '../../src/pages/Home';
import FeaturesSection from '../../src/components/homepage/FeaturesSection';

/**
 * Scenario 9 — Performance and Bundle Size.
 *
 * Covers NFR-1 (load budget, JSDOM-level regression guard) and NFR-3 (page
 * size budget under 500KB excluding scripts). Network/script hygiene checks
 * verify that the public homepage stays free of authenticated-API calls and
 * render-blocking third-party scripts.
 *
 * JSDOM cannot reproduce a 2-second 3G budget; the unit assertions here are
 * regression guards that catch obvious slow-downs. The real budget is
 * validated by the production bundle assertion against the dist/ output.
 */

const PROJECT_ROOT = path.resolve(__dirname, '..', '..');
const DIST_DIR = path.join(PROJECT_ROOT, 'dist');

const PAGE_SIZE_BUDGET_BYTES = 500 * 1024;
const RENDER_BUDGET_MS = 100;
const JS_BUDGET_BYTES = 1024 * 1024;

const renderHome = () =>
  render(
    <MemoryRouter initialEntries={['/']}>
      <Home />
    </MemoryRouter>,
  );

function collectFiles(dir: string, predicate: (name: string) => boolean): string[] {
  if (!fs.existsSync(dir)) return [];
  const out: string[] = [];
  const stack: string[] = [dir];
  while (stack.length > 0) {
    const current = stack.pop()!;
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) stack.push(full);
      else if (predicate(entry.name)) out.push(full);
    }
  }
  return out;
}

function sumFileSizes(files: string[]): number {
  return files.reduce((total, file) => total + fs.statSync(file).size, 0);
}

describe('Homepage performance and bundle size (NFR-1, NFR-3)', () => {
  describe('Render performance (NFR-1 regression guard)', () => {
    it('renders <Home /> within the JSDOM render budget', () => {
      const start = performance.now();
      renderHome();
      const elapsed = performance.now() - start;

      // Sanity: the page rendered at all
      expect(screen.getByRole('heading', { level: 1, name: /mirdb/i })).toBeInTheDocument();

      // Regression guard - JSDOM is slow but should still beat 100ms.
      expect(elapsed).toBeLessThan(RENDER_BUDGET_MS);
    });
  });

  describe('Re-render isolation', () => {
    it('does not re-render <FeaturesSection /> when an unrelated component updates state', () => {
      let featuresRenderCount = 0;

      function CountingFeatures() {
        featuresRenderCount += 1;
        return <FeaturesSection />;
      }

      function UnrelatedCounter() {
        const [count, setCount] = useState(0);
        return (
          <button
            type="button"
            data-testid="unrelated-counter-btn"
            onClick={() => setCount((c) => c + 1)}
          >
            count: {count}
          </button>
        );
      }

      function Wrapper() {
        return (
          <>
            <UnrelatedCounter />
            <CountingFeatures />
          </>
        );
      }

      render(
        <MemoryRouter>
          <Wrapper />
        </MemoryRouter>,
      );

      // Initial mount renders the features section exactly once.
      const baseline = featuresRenderCount;
      expect(baseline).toBeGreaterThanOrEqual(1);

      const btn = screen.getByTestId('unrelated-counter-btn');

      act(() => {
        fireEvent.click(btn);
      });
      act(() => {
        fireEvent.click(btn);
      });
      act(() => {
        fireEvent.click(btn);
      });

      // The counter advanced (sanity), but the sibling features section did
      // not re-render because its parent has no state change.
      expect(btn.textContent).toMatch(/count: 3/);
      expect(featuresRenderCount).toBe(baseline);
    });
  });

  describe('Production build payload (NFR-3)', () => {
    beforeAll(() => {
      if (process.env.SKIP_BUILD === '1') return;
      if (fs.existsSync(DIST_DIR) && fs.existsSync(path.join(DIST_DIR, 'index.html'))) {
        return;
      }
      try {
        execSync('npx vite build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          env: { ...process.env, NODE_ENV: 'production' },
        });
      } catch (err) {
        // Surface a useful failure when the test runs without a built dist/.
        // eslint-disable-next-line no-console
        console.warn('[homepage.perf] vite build failed:', (err as Error).message);
      }
    }, 180_000);

    it('serves an index.html plus CSS payload under the 500KB page-size budget', () => {
      const indexHtml = path.join(DIST_DIR, 'index.html');
      expect(fs.existsSync(indexHtml)).toBe(true);

      const htmlSize = fs.statSync(indexHtml).size;
      const cssFiles = collectFiles(DIST_DIR, (name) => name.endsWith('.css'));
      const cssSize = sumFileSizes(cssFiles);

      const totalNonScriptBytes = htmlSize + cssSize;
      expect(totalNonScriptBytes).toBeLessThan(PAGE_SIZE_BUDGET_BYTES);
    });

    it('documents the JS bundle size and keeps it within a sane budget', () => {
      const jsFiles = collectFiles(DIST_DIR, (name) => name.endsWith('.js'));
      expect(jsFiles.length).toBeGreaterThan(0);
      const jsSize = sumFileSizes(jsFiles);
      expect(jsSize).toBeLessThan(JS_BUDGET_BYTES);
    });
  });

  describe('Public homepage avoids authenticated endpoints', () => {
    it('makes no fetch calls to /api/users/me or /api/urls during render', () => {
      const fetchSpy = vi.fn(async () => new Response('{}', { status: 200 }));
      const originalFetch = globalThis.fetch;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (globalThis as any).fetch = fetchSpy;

      try {
        renderHome();

        const authCalls = fetchSpy.mock.calls.filter((call) => {
          const target = call[0];
          const url =
            typeof target === 'string'
              ? target
              : target instanceof URL
                ? target.toString()
                : target && typeof target === 'object' && 'url' in target
                  ? String((target as { url: string }).url)
                  : '';
          return /\/api\/(users\/me|urls)(\b|\/|$)/.test(url);
        });

        expect(authCalls).toHaveLength(0);
      } finally {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (globalThis as any).fetch = originalFetch;
      }
    });
  });

  describe('No blocking third-party scripts', () => {
    it('adds no external <script src> elements via the homepage component tree', () => {
      const headScriptsBefore = document.head.querySelectorAll('script[src]').length;
      const { container } = renderHome();

      // Scripts directly rendered into the component output (none expected).
      const containerScripts = Array.from(container.querySelectorAll('script[src]'));
      expect(containerScripts).toHaveLength(0);

      // Helmet may inject into document.head. Any that appear during render
      // must be non-blocking (defer or async).
      const headScripts = Array.from(document.head.querySelectorAll('script[src]'));
      const addedByRender = headScripts.slice(headScriptsBefore);
      const blocking = addedByRender.filter(
        (s) => !s.hasAttribute('async') && !s.hasAttribute('defer'),
      );
      expect(blocking).toHaveLength(0);
    });
  });
});
