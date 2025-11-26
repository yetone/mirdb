const { chromium } = require('playwright');
const http = require('http');
const fs = require('fs');
const path = require('path');

class ArchitectureTestRunner {
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

  async architectureSectionExists(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    // Check if architecture section exists
    const architectureSection = await page.waitForSelector('#architecture');
    const isVisible = await architectureSection.isVisible();

    if (!isVisible) {
      throw new Error('Architecture section is not visible');
    }

    // Check if it has the right title
    const title = await architectureSection.$('h2');
    const titleText = await title.textContent();

    if (titleText !== 'Architecture Overview') {
      throw new Error(`Architecture section title is '${titleText}', expected 'Architecture Overview'`);
    }
  }

  async lsmTreeExplanationPresent(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#architecture');

    // Check for LSM Tree heading
    const lsmHeading = await page.$('#architecture .architecture-explanation h3');
    const lsmHeadingText = await lsmHeading.textContent();

    if (!lsmHeadingText.includes('LSM Tree')) {
      throw new Error(`LSM Tree heading text is '${lsmHeadingText}', expected to contain 'LSM Tree'`);
    }

    // Check for explanation text
    const explanationText = await page.innerText('#architecture .architecture-explanation');

    const expectedKeywords = ['Log-Structured Merge-tree', 'WAL', 'Write-Ahead Log', 'Memtable', 'Skip-List', 'SSTable'];
    const missingKeywords = expectedKeywords.filter(keyword => !explanationText.includes(keyword));

    if (missingKeywords.length > 0) {
      throw new Error(`LSM Tree explanation missing keywords: ${missingKeywords.join(', ')}`);
    }
  }

  async architectureDiagramPresent(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');

    // Check for SVG diagram
    const diagramSvg = await page.waitForSelector('#architecture .architecture-diagram svg');
    const isVisible = await diagramSvg.isVisible();

    if (!isVisible) {
      throw new Error('Architecture diagram SVG is not visible');
    }

    // Check for essential diagram elements
    const svgContent = await diagramSvg.innerHTML();

    const requiredElements = [
      'Memcached',
      'Client',
      'Write-Ahead',
      'Log (WAL)',
      'Skip-List',
      'Memtable',
      'SSTable',
      'Compaction'
    ];

    const missingElements = requiredElements.filter(element => !svgContent.includes(element));

    if (missingElements.length > 0) {
      throw new Error(`Architecture diagram missing elements: ${missingElements.join(', ')}`);
    }

    // Check for arrow markers showing data flow
    if (!svgContent.includes('arrowhead') && !svgContent.includes('polygon points=')) {
      throw new Error('Architecture diagram does not show data flow direction with arrows');
    }
  }

  async architectureExplanationUnderstandable(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#architecture');

    const explanationText = await page.innerText('#architecture .architecture-explanation');

    // Check that explanation includes concrete benefits
    const expectedBenefits = [
      'write performance',
      'durability',
      'prevents data loss',
      'fast writes',
      'compacting'
    ];

    const foundBenefits = expectedBenefits.filter(benefit =>
      explanationText.toLowerCase().includes(benefit)
    );

    if (foundBenefits.length < 3) {
      throw new Error(`Architecture explanation should cover more benefits, only found: ${foundBenefits.join(', ')}`);
    }

    // Check that write path is clearly explained
    const writePathSteps = [
      'logged to disk',
      'memtable',
      'sstable',
      'compaction'
    ];

    const foundSteps = writePathSteps.filter(step =>
      explanationText.toLowerCase().includes(step)
    );

    if (foundSteps.length < 3) {
      throw new Error(`Architecture explanation should more clearly explain the write path, only found: ${foundSteps.join(', ')}`);
    }
  }

  async technicalAccuracy(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('http://localhost:3000');
    await page.waitForSelector('#architecture');

    const explanationText = await page.innerText('#architecture .architecture-explanation');

    // Check for technical accuracy based on PRD knowledge
    // These should match actual MirDB implementation
    const accurateDetails = [
      // WAL provides durability
      explanationText.toLowerCase().includes('durability') || explanationText.toLowerCase().includes('prevent'),

      // Memtable is in-memory
      explanationText.toLowerCase().includes('in-memory') || explanationText.toLowerCase().includes('memory'),

      // Skip-list is used (not B-tree or hash table)
      explanationText.toLowerCase().includes('skip-list') || explanationText.toLowerCase().includes('skip list'),

      // SSTables are immutable
      explanationText.toLowerCase().includes('immutable') || explanationText.toLowerCase().includes('persisted'),

      // Compaction optimizes storage
      explanationText.toLowerCase().includes('compaction') || explanationText.toLowerCase().includes('optimize'),

      // GET checks memtable first
      explanationText.toLowerCase().includes('get') || explanationText.toLowerCase().includes('read')
    ];

    const trueCount = accurateDetails.filter(Boolean).length;

    if (trueCount < 4) {
      throw new Error(`Architecture explanation accuracy issues: only ${trueCount}/6 accuracy checks passed`);
    }
  }
}

async function runArchitectureTests() {
  const runner = new ArchitectureTestRunner();
  let server;
  let browser;

  try {
    runner.log('Starting architecture test execution...');

    // Start server
    server = await runner.startServer();

    // Launch browser
    browser = await chromium.launch({ headless: true });

    const tests = [
      { name: 'Architecture section exists with correct heading', fn: runner.architectureSectionExists },
      { name: 'LSM Tree explanation is present and comprehensive', fn: runner.lsmTreeExplanationPresent },
      { name: 'Architecture diagram shows data flow through WAL → Memtable → SSTable', fn: runner.architectureDiagramPresent },
      { name: 'Explanation is understandable to intermediate developers', fn: runner.architectureExplanationUnderstandable },
      { name: 'Technical accuracy reflects actual MirDB implementation', fn: runner.technicalAccuracy },
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
    console.log('ARCHITECTURE TEST SUMMARY');
    console.log('='.repeat(50));
    console.log(`Total: ${tests.length}`);
    console.log(`\x1b[32mPassed: ${runner.passCount}\x1b[0m`);
    console.log(`\x1b[31mFailed: ${runner.failCount}\x1b[0m`);
    console.log('='.repeat(50));

    return runner.failCount === 0;

  } catch (error) {
    console.error('Architecture test execution failed:', error);
    return false;
  } finally {
    if (browser) await browser.close();
    if (server) server.close();
  }
}

if (require.main === module) {
  runArchitectureTests().then(success => {
    process.exit(success ? 0 : 1);
  }).catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { ArchitectureTestRunner, runArchitectureTests };
