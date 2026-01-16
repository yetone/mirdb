import { test, expect, chromium } from '@playwright/test'
import { playAudit } from 'playwright-lighthouse'

const LIGHTHOUSE_THRESHOLDS = {
  performance: 90,
  accessibility: 90,
  'best-practices': 90,
  seo: 90,
}

// Desktop configuration to disable mobile throttling
const desktopConfig = {
  extends: 'lighthouse:default',
  settings: {
    formFactor: 'desktop' as const,
    screenEmulation: {
      mobile: false,
      width: 1350,
      height: 940,
      deviceScaleFactor: 1,
      disabled: false,
    },
    // Disable network and CPU throttling for faster, more accurate local testing
    throttlingMethod: 'provided' as const,
    throttling: {
      rttMs: 0,
      throughputKbps: 0,
      cpuSlowdownMultiplier: 1,
      requestLatencyMs: 0,
      downloadThroughputKbps: 0,
      uploadThroughputKbps: 0,
    },
  },
}

// Lighthouse tests need to use a separate browser instance with remote debugging
test.describe.configure({ mode: 'serial' })

test.describe('Homepage Lighthouse Audit (NFR-2: Performance Score 90+)', () => {
  test('TC1: Performance score >= 90', async () => {
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9222'],
    })
    const page = await browser.newPage()
    await page.goto('http://localhost:4173/')
    await page.waitForLoadState('networkidle')

    const result = await playAudit({
      page,
      port: 9222,
      thresholds: {
        performance: LIGHTHOUSE_THRESHOLDS.performance,
      },
      config: {
        ...desktopConfig,
        settings: {
          ...desktopConfig.settings,
          onlyCategories: ['performance'],
        },
      },
    })

    const performanceScore = result.lhr.categories.performance?.score
      ? Math.round(result.lhr.categories.performance.score * 100)
      : 0

    console.log(`Performance Score: ${performanceScore}`)

    if (result.lhr.audits) {
      const audits = result.lhr.audits
      console.log('Performance Metrics:')
      console.log(`  First Contentful Paint (FCP): ${audits['first-contentful-paint']?.displayValue || 'N/A'}`)
      console.log(`  Largest Contentful Paint (LCP): ${audits['largest-contentful-paint']?.displayValue || 'N/A'}`)
      console.log(`  Total Blocking Time (TBT): ${audits['total-blocking-time']?.displayValue || 'N/A'}`)
      console.log(`  Cumulative Layout Shift (CLS): ${audits['cumulative-layout-shift']?.displayValue || 'N/A'}`)
      console.log(`  Speed Index: ${audits['speed-index']?.displayValue || 'N/A'}`)
    }

    expect(performanceScore).toBeGreaterThanOrEqual(LIGHTHOUSE_THRESHOLDS.performance)
    await browser.close()
  })

  test('TC2: Accessibility score >= 90', async () => {
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9223'],
    })
    const page = await browser.newPage()
    await page.goto('http://localhost:4173/')
    await page.waitForLoadState('networkidle')

    const result = await playAudit({
      page,
      port: 9223,
      thresholds: {
        accessibility: LIGHTHOUSE_THRESHOLDS.accessibility,
      },
      config: {
        ...desktopConfig,
        settings: {
          ...desktopConfig.settings,
          onlyCategories: ['accessibility'],
        },
      },
    })

    const accessibilityScore = result.lhr.categories.accessibility?.score
      ? Math.round(result.lhr.categories.accessibility.score * 100)
      : 0

    console.log(`Accessibility Score: ${accessibilityScore}`)

    if (result.lhr.audits) {
      const failingAudits = Object.entries(result.lhr.audits)
        .filter(([, audit]) => {
          const a = audit as { score?: number | null }
          return a.score !== null && a.score !== undefined && a.score < 1
        })
        .map(([id, audit]) => {
          const a = audit as { title?: string; score?: number }
          return { id, title: a.title, score: a.score }
        })

      if (failingAudits.length > 0) {
        console.log('Failing Accessibility Audits:')
        failingAudits.forEach(audit => {
          console.log(`  - ${audit.id}: ${audit.title} (score: ${audit.score})`)
        })
      }
    }

    expect(accessibilityScore).toBeGreaterThanOrEqual(LIGHTHOUSE_THRESHOLDS.accessibility)
    await browser.close()
  })

  test('TC3: Best Practices score >= 90', async () => {
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9224'],
    })
    const page = await browser.newPage()
    await page.goto('http://localhost:4173/')
    await page.waitForLoadState('networkidle')

    const result = await playAudit({
      page,
      port: 9224,
      thresholds: {
        'best-practices': LIGHTHOUSE_THRESHOLDS['best-practices'],
      },
      config: {
        ...desktopConfig,
        settings: {
          ...desktopConfig.settings,
          onlyCategories: ['best-practices'],
        },
      },
    })

    const bestPracticesScore = result.lhr.categories['best-practices']?.score
      ? Math.round(result.lhr.categories['best-practices'].score * 100)
      : 0

    console.log(`Best Practices Score: ${bestPracticesScore}`)

    if (result.lhr.audits) {
      const failingAudits = Object.entries(result.lhr.audits)
        .filter(([, audit]) => {
          const a = audit as { score?: number | null }
          return a.score !== null && a.score !== undefined && a.score < 1
        })
        .map(([id, audit]) => {
          const a = audit as { title?: string; score?: number }
          return { id, title: a.title, score: a.score }
        })

      if (failingAudits.length > 0) {
        console.log('Failing Best Practices Audits:')
        failingAudits.forEach(audit => {
          console.log(`  - ${audit.id}: ${audit.title} (score: ${audit.score})`)
        })
      }
    }

    expect(bestPracticesScore).toBeGreaterThanOrEqual(LIGHTHOUSE_THRESHOLDS['best-practices'])
    await browser.close()
  })

  test('TC4: SEO score >= 90', async () => {
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9225'],
    })
    const page = await browser.newPage()
    await page.goto('http://localhost:4173/')
    await page.waitForLoadState('networkidle')

    const result = await playAudit({
      page,
      port: 9225,
      thresholds: {
        seo: LIGHTHOUSE_THRESHOLDS.seo,
      },
      config: {
        ...desktopConfig,
        settings: {
          ...desktopConfig.settings,
          onlyCategories: ['seo'],
        },
      },
    })

    const seoScore = result.lhr.categories.seo?.score
      ? Math.round(result.lhr.categories.seo.score * 100)
      : 0

    console.log(`SEO Score: ${seoScore}`)

    if (result.lhr.audits) {
      const failingAudits = Object.entries(result.lhr.audits)
        .filter(([, audit]) => {
          const a = audit as { score?: number | null }
          return a.score !== null && a.score !== undefined && a.score < 1
        })
        .map(([id, audit]) => {
          const a = audit as { title?: string; score?: number }
          return { id, title: a.title, score: a.score }
        })

      if (failingAudits.length > 0) {
        console.log('Failing SEO Audits:')
        failingAudits.forEach(audit => {
          console.log(`  - ${audit.id}: ${audit.title} (score: ${audit.score})`)
        })
      }
    }

    expect(seoScore).toBeGreaterThanOrEqual(LIGHTHOUSE_THRESHOLDS.seo)
    await browser.close()
  })
})
