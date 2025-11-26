const { chromium } = require('playwright');
const http = require('http');
const fs = require('fs');
const path = require('path');

class QuickStartTestRunner {
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

  async quickStartSectionExists(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    const quickStartSection = await page.waitForSelector('#quick-start', { timeout: 2000 });

    if (!quickStartSection) {
      throw new Error('Quick-start section not found');
    }
  }

  async quickStartHasInstallationSubsection(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#quick-start', { timeout: 2000 });

    const installationBlock = await page.$('#quick-start code:has-text("git clone")');
    const cargoBlock = await page.$('#quick-start code:has-text("cargo build")');

    if (!installationBlock) {
      throw new Error('Git clone instruction not found in quick-start section');
    }

    if (!cargoBlock) {
      throw new Error('Cargo build instruction not found in quick-start section');
    }

    const gitCloneText = await installationBlock.innerText();
    const cargoText = await cargoBlock.innerText();

    if (!gitCloneText.includes('git clone') || !gitCloneText.includes('mirdb')) {
      throw new Error('Git clone command does not contain expected clone instructions');
    }

    if (!cargoText.includes('cargo build') || !cargoText.includes('--release')) {
      throw new Error('Cargo build command missing --release flag');
    }
  }

  async quickStartHasConfigurationExample(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#quick-start', { timeout: 2000 });

    const configBlock = await page.$('#quick-start code:has-text("[server]")');

    if (!configBlock) {
      throw new Error('mirdb.toml configuration example not found');
    }

    const configText = await configBlock.innerText();

    const requiredFields = ['server', 'work_dir', 'memtable_size', 'sstable_max_size'];
    for (const field of requiredFields) {
      if (!configText.includes(field)) {
        throw new Error(`Configuration example missing required field: ${field}`);
      }
    }
  }

  async quickStartHasUsageCodeSnippet(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#quick-start', { timeout: 2000 });

    const usageBlock = await page.$(
      '#quick-start code:has-text("memcache")'
    );

    if (!usageBlock) {
      throw new Error('Usage code snippet not found');
    }

    const usageText = await usageBlock.innerText();

    // Check for SET and GET operations
    if (!usageText.includes('SET') && !usageText.includes('set')) {
      throw new Error('Usage example missing SET operation');
    }

    if (!usageText.includes('GET') && !usageText.includes('get')) {
      throw new Error('Usage example missing GET operation');
    }

    if (!usageText.includes('localhost') || !usageText.includes('11211')) {
      throw new Error('Usage example missing connection to Memcached port (11211)');
    }
  }

  async quickStartHasOperationsList(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#quick-start', { timeout: 2000 });

    const operationsText = await page.innerText('#quick-start');

    const requiredOperations = ['GET', 'SET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND', 'INFO'];

    for (const op of requiredOperations) {
      if (!operationsText.includes(op)) {
        throw new Error(`Operations list missing required operation: ${op}`);
      }
    }
  }

  async supportedOperationsIncludeGetSet(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#quick-start', { timeout: 2000 });

    const operationsText = await page.innerText('#quick-start');

    if (!operationsText.includes('GET')) {
      throw new Error('Operations list missing GET');
    }

    if (!operationsText.includes('SET')) {
      throw new Error('Operations list missing SET');
    }

    if (!operationsText.includes('GETS')) {
      throw new Error('Operations list missing GETS');
    }
  }

  async quickStartHasMajorCompaction(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#quick-start', { timeout: 2000 });

    const operationsText = await page.innerText('#quick-start');

    if (!operationsText.includes('MajorCompaction')) {
      throw new Error('Operations list missing MajorCompaction');
    }
  }

  async scrollToQuickStartWorks(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    const getStartedButton = await page.waitForSelector('#get-started-btn');
    const quickStartSection = await page.waitForSelector('#quick-start');

    if (!getStartedButton || !quickStartSection) {
      throw new Error('Required elements not found');
    }

    await page.click('#get-started-btn');
    await page.waitForTimeout(800);

    // Check that we're reasonably scrolled down (at least below 500px)
    const scrollPos = await page.evaluate(() => window.scrollY);

    if (scrollPos < 500) {
      throw new Error(`Page did not scroll down enough: scroll position=${scrollPos}px`);
    }

    // Also verify we can see the quick-start section is now in viewport
    const quickStartInViewport = await quickStartSection.isVisible();
    if (!quickStartInViewport) {
      throw new Error('Quick start section is not visible after scrolling');
    }
  }
}

async function runQuickStartTests() {
  const runner = new QuickStartTestRunner();
  let server;
  let browser;

  try {
    runner.log('Starting quick-start test execution...');

    // Start server
    server = await runner.startServer();

    // Launch browser
    browser = await chromium.launch({ headless: true });

    const tests = [
      { name: 'Quick Start section exists with valid structure', fn: runner.quickStartSectionExists },
      { name: 'Installation commands include git clone and cargo build', fn: runner.quickStartHasInstallationSubsection },
      { name: 'Configuration example with mirdb.toml is present', fn: runner.quickStartHasConfigurationExample },
      { name: 'Basic usage code snippet shows SET and GET operations', fn: runner.quickStartHasUsageCodeSnippet },
      { name: 'Supported operations list includes GET, SET, DELETE, ADD, REPLACE', fn: runner.quickStartHasOperationsList },
      { name: 'Operations list includes extended commands GETS', fn: runner.supportedOperationsIncludeGetSet },
      { name: 'Operations list includes MajorCompaction', fn: runner.quickStartHasMajorCompaction },
      { name: 'Get Started button scrolls to quick-start section', fn: runner.scrollToQuickStartWorks },
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
    console.log('QUICK START TEST SUMMARY');
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
  runQuickStartTests().then(success => {
    process.exit(success ? 0 : 1);
  }).catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { QuickStartTestRunner, runQuickStartTests };
