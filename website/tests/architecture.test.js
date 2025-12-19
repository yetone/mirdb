// Architecture Section Unit Tests using jsdom
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Read the HTML file
const htmlPath = path.resolve(__dirname, '../dist/index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Create DOM
const dom = new JSDOM(htmlContent);
const document = dom.window.document;

// Helper function to run test and track results
function runTest(name, testFn) {
  try {
    testFn();
    console.log(`✓ ${name}`);
    return { name, passed: true };
  } catch (error) {
    console.error(`✗ ${name}`);
    console.error(`  Error: ${error.message}`);
    return { name, passed: false, error: error.message };
  }
}

console.log('\n=== Architecture Section Unit Tests ===\n');

const results = [];

// Test 1: Check architecture section exists
results.push(runTest('Architecture section exists', () => {
  const section = document.querySelector('[data-testid="architecture-section"]');
  assert(section !== null, 'Architecture section should exist');
}));

// Test 2: Check section has proper heading
results.push(runTest('Architecture section has heading', () => {
  const section = document.querySelector('[data-testid="architecture-section"]');
  const heading = section?.querySelector('h2');
  assert(heading !== null, 'Architecture section should have a heading');
  assert(heading.textContent.toLowerCase().includes('architecture'), 'Heading should mention architecture');
}));

// Test 3: Check architecture diagram container exists
results.push(runTest('Architecture diagram container exists', () => {
  const diagram = document.querySelector('[data-testid="architecture-diagram"]');
  assert(diagram !== null, 'Architecture diagram container should exist');
}));

// Test 4: Check WAL component in diagram
results.push(runTest('WAL component exists in diagram', () => {
  const walComponent = document.querySelector('[data-testid="diagram-wal"]');
  assert(walComponent !== null, 'WAL component should exist');
  assert(walComponent.textContent.includes('WAL'), 'WAL component should contain WAL text');
}));

// Test 5: Check Memtable component in diagram
results.push(runTest('Memtable component exists in diagram', () => {
  const memtableComponent = document.querySelector('[data-testid="diagram-memtable"]');
  assert(memtableComponent !== null, 'Memtable component should exist');
  assert(memtableComponent.textContent.includes('Memtable'), 'Memtable component should contain Memtable text');
}));

// Test 6: Check SSTable component in diagram
results.push(runTest('SSTable component exists in diagram', () => {
  const sstableComponent = document.querySelector('[data-testid="diagram-sstable"]');
  assert(sstableComponent !== null, 'SSTable component should exist');
  assert(sstableComponent.textContent.includes('SSTable'), 'SSTable component should contain SSTable text');
}));

// Test 7: Check flow arrows exist
results.push(runTest('Flow arrows exist in diagram', () => {
  const flowArrows = document.querySelectorAll('[data-testid^="flow-arrow"]');
  assert(flowArrows.length > 0, 'Flow arrows should exist in the diagram');
}));

// Test 8: Check LSM explanation exists
results.push(runTest('LSM tree explanation exists', () => {
  const explanation = document.querySelector('[data-testid="lsm-explanation"]');
  assert(explanation !== null, 'LSM tree explanation should exist');
}));

// Test 9: Check LSM explanation content
results.push(runTest('LSM explanation mentions LSM tree', () => {
  const explanation = document.querySelector('[data-testid="lsm-explanation"]');
  assert(explanation !== null, 'LSM tree explanation should exist');
  assert(explanation.textContent.toLowerCase().includes('lsm'), 'Explanation should mention LSM');
}));

// Test 10: Check LSM benefits section
results.push(runTest('LSM benefits section exists', () => {
  const benefits = document.querySelector('[data-testid="lsm-benefits"]');
  assert(benefits !== null, 'LSM benefits section should exist');
}));

// Test 11: Check architecture documentation link exists
results.push(runTest('Architecture documentation link exists', () => {
  const link = document.querySelector('[data-testid="architecture-docs-link"]');
  assert(link !== null, 'Architecture documentation link should exist');
}));

// Test 12: Check documentation link has href
results.push(runTest('Documentation link has valid href', () => {
  const link = document.querySelector('[data-testid="architecture-docs-link"]');
  assert(link !== null, 'Architecture documentation link should exist');
  const href = link.getAttribute('href');
  assert(href !== null && href.length > 0, 'Documentation link should have a valid href');
}));

// Test 13: Check documentation link text is descriptive
results.push(runTest('Documentation link has descriptive text', () => {
  const link = document.querySelector('[data-testid="architecture-docs-link"]');
  assert(link !== null, 'Architecture documentation link should exist');
  const text = link.textContent.toLowerCase();
  const hasDescriptiveText = text.includes('architecture') ||
                             text.includes('documentation') ||
                             text.includes('learn more') ||
                             text.includes('read more');
  assert(hasDescriptiveText, 'Documentation link should have descriptive text');
}));

// Summary
console.log('\n=== Test Summary ===');
const passed = results.filter(r => r.passed).length;
const failed = results.filter(r => !r.passed).length;
console.log(`Passed: ${passed}/${results.length}`);
console.log(`Failed: ${failed}/${results.length}`);

if (failed > 0) {
  console.log('\nFailed tests:');
  results.filter(r => !r.passed).forEach(r => {
    console.log(`  - ${r.name}: ${r.error}`);
  });
  process.exit(1);
}

console.log('\nAll tests passed!');
