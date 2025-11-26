const { chromium } = require('playwright');
const http = require('http');
const fs = require('fs');
const path = require('path');

class TestRunner {
  constructor() {
    this.results = [];
    this.passCount = 0;
    this.failCount = 0;
  }

  async startServer() {
    const server = http.createServer((req, res) => {
      let filePath = req.url === '/' ? '/index.html' : req.url;
      filePath = path.join(__dirname, '..', filePath);

      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(404, { 'Content-Type': 'text/plain' });
          res.end('Not Found');
          return;
        }

        const ext = path.extname(filePath);
        const contentType = {
          '.html': 'text/html',
          '.css': 'text/css',
          '.js': 'application/javascript',
          '.gif': 'image/gif',
        }[ext] || 'text/plain';

        res.writeHead(200, { 'Content-Type': contentType });
        res.end(data);
      });
    });

    return new Promise((resolve) => {
      server.listen(3000, 'localhost', () => {
        console.log('Test server running on http://localhost:3000');
        resolve(server);
      });
    });
  }

  log(message) {
    console.log(`\x1b[36m[TEST]\x1b[0m ${message}`);
  }

  pass(testName) {
    this.passCount++;
    this.results.push({ name: testName, status: 'pass' });
    console.log(`  \x1b[32m✓\x1b[0m ${testName}`);
  }

  fail(testName, error) {
    this.failCount++;
    this.results.push({ name: testName, status: 'fail', error: error.message });
    console.log(`  \x1b[31m✗\x1b[0m ${testName}`);
    console.log(`    Error: ${error.message}`);
  }

  async heroSectionLoads(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    const start = Date.now();
    await page.waitForSelector('.hero-section', { timeout: 2000 });
    const loadTime = Date.now() - start;

    if (loadTime > 2000) {
      throw new Error(`Hero section took ${loadTime}ms to load (expected <= 2000ms)`);
    }

    // Check all elements are visible
    await page.waitForSelector('.hero-title');
    await page.waitForSelector('.hero-tagline');
    await page.waitForSelector('.logo-image');
    await page.waitForSelector('#get-started-btn');
    await page.waitForSelector('#github-btn');
  }

  async productNameIsDisplayed(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('.hero-title');

    const heroTitle = await page.$('.hero-title');
    const tagName = await heroTitle.evaluate(el => el.tagName);
    const text = await heroTitle.evaluate(el => el.textContent.trim());

    if (tagName !== 'H1') {
      throw new Error(`Title is not an H1 tag, found ${tagName}`);
    }

    if (text !== 'MirDB') {
      throw new Error(`Title text is '${text}', expected 'MirDB'`);
    }
  }

  async taglineIsPresentAndReadable(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('.hero-tagline');

    const tagline = await page.$('.hero-tagline');
    const text = await tagline.evaluate(el => el.textContent.trim());
    const expectedTagline = 'Persistent Key-Value Store with Memcached Protocol Support';

    if (text !== expectedTagline) {
      throw new Error(`Tagline is '${text}', expected '${expectedTagline}'`);
    }

    const isVisible = await tagline.isVisible();
    if (!isVisible) {
      throw new Error('Tagline is not visible');
    }
  }

  async animatedLogoDisplays(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    const logo = await page.waitForSelector('.logo-image');

    const tagName = await logo.evaluate(el => el.tagName);
    if (tagName !== 'IMG') {
      throw new Error(`Logo is not an IMG tag, found ${tagName}`);
    }

    const src = await logo.getAttribute('src');
    if (!src.includes('logo.gif')) {
      throw new Error(`Logo src is '${src}', expected to contain 'logo.gif'`);
    }

    const alt = await logo.getAttribute('alt');
    if (!alt || alt.trim() === '') {
      throw new Error('Logo missing alt text');
    }

    // Check image loaded successfully
    const naturalWidth = await logo.evaluate(img => img.naturalWidth);
    if (naturalWidth === 0) {
      throw new Error('Gif animation failed to load (naturalWidth is 0)');
    }
  }

  async getStartedButtonScrolls(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    const button = await page.waitForSelector('#get-started-btn');
    const quickStartSection = await page.waitForSelector('#quick-start');

    if (!button) {
      throw new Error('Get Started button not found');
    }

    const buttonText = await button.textContent();
    if (!buttonText.includes('Get Started')) {
      throw new Error('Button does not contain correct text');
    }

    // Simulate click
    await button.click();

    // Wait for scroll to complete
    await page.waitForTimeout(600);

    // Check we're at the quick-start section
    const viewportCenter = await page.evaluate(() => window.scrollY + (window.innerHeight / 2));
    const quickStartTop = await quickStartSection.evaluate(el => el.offsetTop);
    const quickStartBottom = await quickStartSection.evaluate(el => el.offsetTop + el.offsetHeight);

    if (viewportCenter < quickStartTop || viewportCenter > quickStartBottom) {
      throw new Error('Page did not scroll to quick-start section');
    }
  }

  async githubButtonOpensNewTab(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    const githubBtn = await page.waitForSelector('#github-btn');

    const href = await githubBtn.getAttribute('href');
    const expected = 'https://github.com/yetone/mirdb';

    if (href !== expected) {
      throw new Error(`GitHub link is '${href}', expected '${expected}'`);
    }

    const target = await githubBtn.getAttribute('target');
    if (target !== '_blank') {
      throw new Error(`GitHub link target is '${target}', expected '_blank'`);
    }

    const rel = await githubBtn.getAttribute('rel');
    if (!rel || !rel.includes('noopener') || !rel.includes('noreferrer')) {
      throw new Error(`GitHub link missing security attributes (rel=noopener noreferrer)`);
    }
  }

  async allElementsAreVisible(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    const selectors = [
      '.hero-title',
      '.hero-tagline',
      '.logo-image',
      '#get-started-btn',
      '#github-btn'
    ];

    for (const selector of selectors) {
      const element = await page.waitForSelector(selector);
      const isVisible = await element.isVisible();
      if (!isVisible) {
        throw new Error(`Element ${selector} is not visible`);
      }
    }
  }
}

async function runTests() {
  const runner = new TestRunner();
  let server;
  let browser;

  try {
    runner.log('Starting test execution...');

    // Start server
    server = await runner.startServer();

    // Launch browser
    browser = await chromium.launch({ headless: true });

    const tests = [
      { name: 'Hero section loads within 2 seconds', fn: runner.heroSectionLoads },
      { name: 'Product name MirDB is displayed with h1 tag', fn: runner.productNameIsDisplayed },
      { name: 'Tagline is present and correct', fn: runner.taglineIsPresentAndReadable },
      { name: 'Animated logo displays correctly with alt text', fn: runner.animatedLogoDisplays },
      { name: "'Get Started' button scrolls to quick-start section", fn: runner.getStartedButtonScrolls },
      { name: "'View on GitHub' button links to correct repository", fn: runner.githubButtonOpensNewTab },
      { name: 'All hero elements are visible and properly styled', fn: runner.allElementsAreVisible },
    ];

    for (const test of tests) {
      try {
        runner.log(`Running: ${test.name}`);
        await test.fn.call(runner, browser);
        runner.pass(test.name);
      } catch (error) {
        runner.fail(test.name, error);
      }
    }

    // Print summary
    console.log('\n' + '='.repeat(50));
    console.log('TEST SUMMARY');
    console.log('='.repeat(50));
    console.log(`Total: ${tests.length}`);
    console.log(`\x1b[32mPassed: ${runner.passCount}\x1b[0m`);
    console.log(`\x1b[31mFailed: ${runner.failCount}\x1b[0m`);
    console.log('='.repeat(50));

    return runner.failCount === 0;

  } catch (error) {
    console.error('Test execution failed:', error);
    return false;
  } finally {
    if (browser) await browser.close();
    if (server) server.close();
  }
}

if (require.main === module) {
  runTests().then(success => {
    process.exit(success ? 0 : 1);
  }).catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { TestRunner, runTests };
