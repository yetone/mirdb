const { chromium } = require('playwright');
const http = require('http');
const fs = require('fs');
const path = require('path');

class NavigationTestRunner {
  constructor() {
    this.results = [];
    this.passCount = 0;
    this.failCount = 0;
    this.server = null;
    this.browser = null;
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
      server.listen(3001, 'localhost', () => {
        console.log('Test server running on http://localhost:3001');
        resolve(server);
      });
    });
  }

  log(message) {
    console.log(`\x1b[36m[NAV TEST]\x1b[0m ${message}`);
  }

  pass(testName) {
    this.passCount++;
    this.results.push({ name: testName, status: 'pass' });
    console.log(`  \x1b[32m✓\x1b[0m ${testName}`);
  }

  fail(testName, error) {
    this.failCount++;
    this.results.push({ name: testName, status: 'fail', error: error.message, stack: error.stack });
    console.log(`  \x1b[31m✗\x1b[0m ${testName}`);
    console.log(`    Error: ${error.message}`);
  }

  // Test Case 1: Hero GitHub link has correct URL
  async heroGithubLinkUrlIsCorrect(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');
    await page.waitForSelector('#github-btn', { timeout: 2000 });

    const githubBtn = await page.$('#github-btn');
    const href = await githubBtn.getAttribute('href');
    const expected = 'https://github.com/yetone/mirdb';

    if (href !== expected) {
      throw new Error(`Hero GitHub link is '${href}', expected '${expected}'`);
    }

    await context.close();
  }

  // Test Case 2: Hero GitHub link opens in new tab
  async heroGithubLinkOpensInNewTab(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');
    await page.waitForSelector('#github-btn', { timeout: 2000 });

    const githubBtn = await page.$('#github-btn');

    const target = await githubBtn.getAttribute('target');
    if (target !== '_blank') {
      throw new Error(`Hero GitHub link target is '${target}', expected '_blank'`);
    }

    const rel = await githubBtn.getAttribute('rel');
    if (!rel || !rel.includes('noopener') || !rel.includes('noreferrer')) {
      throw new Error(`Hero GitHub link missing security attributes (rel=noopener noreferrer)`);
    }

    await context.close();
  }

  // Test Case 3: Footer GitHub link has correct URL
  async footerGithubLinkUrlIsCorrect(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');
    await page.waitForSelector('.site-footer', { timeout: 2000 });

    const githubLink = await page.waitForSelector('.site-footer a[href*="github.com"]', { timeout: 2000 });
    const href = await githubLink.getAttribute('href');
    const expected = 'https://github.com/yetone/mirdb';

    if (href !== expected) {
      throw new Error(`Footer GitHub link is '${href}', expected '${expected}'`);
    }

    await context.close();
  }

  // Test Case 4: Footer GitHub link opens in new tab
  async footerGithubLinkOpensInNewTab(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');
    await page.waitForSelector('.site-footer', { timeout: 2000 });

    const githubLink = await page.$('.site-footer a[href*="github.com"]');

    const target = await githubLink.getAttribute('target');
    if (target !== '_blank') {
      throw new Error(`Footer GitHub link target is '${target}', expected '_blank'`);
    }

    const rel = await githubLink.getAttribute('rel');
    if (!rel || !rel.includes('noopener') || !rel.includes('noreferrer')) {
      throw new Error(`Footer GitHub link missing security attributes (rel=noopener noreferrer)`);
    }

    await context.close();
  }

  // Test Case 5: Get Started button scrolls to quick-start section
  async getStartedButtonScrollsSmoothly(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');
    await page.waitForSelector('#get-started-btn', { timeout: 2000 });

    const button = await page.$('#get-started-btn');
    const quickStartSection = await page.waitForSelector('#quick-start');

    if (!button) {
      throw new Error('Get Started button not found');
    }

    const buttonText = await button.textContent();
    if (!buttonText.includes('Get Started')) {
      throw new Error(`Button text is '${buttonText}', expected to contain 'Get Started'`);
    }

    // Simulate click
    await button.click();

    // Wait for scroll to complete (smooth scrolling)
    await page.waitForTimeout(1000);

    // Check we're at the quick-start section
    const viewportTop = await page.evaluate(() => window.scrollY);
    const viewportHeight = await page.evaluate(() => window.innerHeight);
    const quickStartTop = await quickStartSection.evaluate(el => el.offsetTop);
    const quickStartHeight = await quickStartSection.evaluate(el => el.offsetHeight);

    const isInView = viewportTop >= quickStartTop - viewportHeight / 2 &&
                     viewportTop <= quickStartTop + quickStartHeight;

    if (!isInView) {
      throw new Error('Page did not scroll to quick-start section');
    }

    await context.close();
  }

  // Test Case 6: Get Started button stays in same tab
  async getStartedButtonOpensInSameTab(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');
    await page.waitForSelector('#get-started-btn', { timeout: 2000 });

    const button = await page.$('#get-started-btn');

    // Check it doesn't have target="_blank"
    const target = await button.getAttribute('target');
    if (target === '_blank') {
      throw new Error('Get Started button opens in new tab, expected to stay in same tab');
    }

    await context.close();
  }

  // Test Case 7: Footer includes MIT license text
  async footerIncludesMitLicense(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');

    const footer = await page.waitForSelector('.site-footer');
    const footerText = await footer.textContent();

    if (!footerText.toLowerCase().includes('mit') && !footerText.toLowerCase().includes('license')) {
      throw new Error('Footer does not include MIT license information');
    }

    await context.close();
  }

  // Test Case 8: Footer includes license file link
  async footerIncludesLicenseLink(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');

    const licenseLink = await page.waitForSelector('.site-footer a[href*="LICENSE"]');

    const href = await licenseLink.getAttribute('href');
    if (!href.includes('LICENSE')) {
      throw new Error(`License link points to '${href}', expected to include 'LICENSE'`);
    }

    await context.close();
  }

  // Test Case 9: Footer includes project status
  async footerIncludesProjectStatus(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');

    const status = await page.waitForSelector('.status-badge');
    const statusText = await status.textContent();

    if (!statusText || statusText.trim() === '') {
      throw new Error('Project status badge is empty or missing');
    }

    await context.close();
  }

  // Test Case 10: All GitHub links point to same repository
  async allGithubLinksAreConsistent(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3001');

    // Find all direct repository links (not LICENSE or other sub-paths)
    const githubLinks = await page.$$('a[href*="github.com/yetone/mirdb"]');

    if (githubLinks.length < 2) {
      throw new Error(`Found ${githubLinks.length} GitHub links, expected at least 2`);
    }

    // Verify all direct repository links have the same URL
    const urls = await Promise.all(
      githubLinks.map(link => link.getAttribute('href'))
    );

    // Count only the main repository links (excluding LICENSE and other sub-paths)
    const repoLinks = urls.filter(url => url === 'https://github.com/yetone/mirdb');

    if (repoLinks.length < 2) {
      throw new Error(`Found ${repoLinks.length} main repository links, expected at least 2`);
    }

    await context.close();
  }
}

async function runNavigationTests() {
  const runner = new NavigationTestRunner();

  try {
    runner.log('Starting navigation and GitHub link test execution...');

    // Start server
    runner.server = await runner.startServer();

    // Launch browser
    runner.browser = await chromium.launch({ headless: true });

    const tests = [
      { name: 'Hero GitHub link points to correct repository URL', fn: runner.heroGithubLinkUrlIsCorrect },
      { name: 'Hero GitHub link opens in new tab with security attributes', fn: runner.heroGithubLinkOpensInNewTab },
      { name: 'Footer GitHub link points to correct repository URL', fn: runner.footerGithubLinkUrlIsCorrect },
      { name: 'Footer GitHub link opens in new tab with security attributes', fn: runner.footerGithubLinkOpensInNewTab },
      { name: 'Get Started button scrolls smoothly to quick-start section', fn: runner.getStartedButtonScrollsSmoothly },
      { name: 'Get Started button opens in same tab (no target=_blank)', fn: runner.getStartedButtonOpensInSameTab },
      { name: 'Footer includes MIT license information', fn: runner.footerIncludesMitLicense },
      { name: 'Footer includes link to LICENSE file', fn: runner.footerIncludesLicenseLink },
      { name: 'Footer includes project status badge', fn: runner.footerIncludesProjectStatus },
      { name: 'All GitHub links are consistent across the page', fn: runner.allGithubLinksAreConsistent },
    ];

    for (const test of tests) {
      try {
        runner.log(`Running: ${test.name}`);
        await test.fn.call(runner, runner.browser);
        runner.pass(test.name);
      } catch (error) {
        runner.fail(test.name, error);
      }
    }

    // Print summary
    console.log('\n' + '='.repeat(60));
    console.log('NAVIGATION & GITHUB LINK TEST SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total: ${tests.length}`);
    console.log(`\x1b[32mPassed: ${runner.passCount}\x1b[0m`);
    console.log(`\x1b[31mFailed: ${runner.failCount}\x1b[0m`);
    console.log('='.repeat(60));

    return runner.failCount === 0;

  } catch (error) {
    console.error('Test execution failed:', error);
    return false;
  } finally {
    if (runner.browser) await runner.browser.close();
    if (runner.server) runner.server.close();
  }
}

if (require.main === module) {
  runNavigationTests().then(success => {
    process.exit(success ? 0 : 1);
  }).catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { NavigationTestRunner, runNavigationTests };
