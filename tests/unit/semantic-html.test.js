/**
 * Unit test for verifying semantic HTML structure of the Features section
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

// Test configuration
const indexPath = path.join(__dirname, '../../index.html');

function runTests() {
  console.log('Running Semantic HTML Structure Tests...\n');

  let passed = 0;
  let failed = 0;
  const results = [];

  // Read the HTML file
  let html;
  try {
    html = fs.readFileSync(indexPath, 'utf-8');
  } catch (error) {
    console.error('Failed to read index.html:', error.message);
    process.exit(1);
  }

  // Parse HTML with JSDOM
  const dom = new JSDOM(html);
  const document = dom.window.document;

  // Test 1: Features section uses <section> element
  function testSectionElement() {
    const featuresSection = document.querySelector('section#features');
    if (featuresSection) {
      console.log('✓ Features section uses <section> element');
      passed++;
      results.push({ test: 'section element', status: 'pass' });
    } else {
      console.log('✗ Features section should use <section> element with id="features"');
      failed++;
      results.push({ test: 'section element', status: 'fail', error: 'Missing <section id="features">' });
    }
  }

  // Test 2: Features section has proper heading hierarchy
  function testHeadingHierarchy() {
    const featuresSection = document.querySelector('section#features');
    if (!featuresSection) {
      console.log('✗ Cannot test heading hierarchy - features section not found');
      failed++;
      results.push({ test: 'heading hierarchy', status: 'fail', error: 'Features section not found' });
      return;
    }

    // Features section should have an h2 as its main heading (h1 is for the page title)
    const sectionHeading = featuresSection.querySelector('h2');
    if (sectionHeading) {
      console.log('✓ Features section has proper h2 heading');
      passed++;
      results.push({ test: 'heading hierarchy', status: 'pass' });
    } else {
      console.log('✗ Features section should have an h2 heading');
      failed++;
      results.push({ test: 'heading hierarchy', status: 'fail', error: 'Missing h2 heading in features section' });
    }
  }

  // Test 3: Feature cards use <article> elements or similar semantic structure
  function testFeatureCards() {
    const featuresSection = document.querySelector('section#features');
    if (!featuresSection) {
      console.log('✗ Cannot test feature cards - features section not found');
      failed++;
      results.push({ test: 'feature cards', status: 'fail', error: 'Features section not found' });
      return;
    }

    // Check for article elements or div elements with role="article" or class="feature-card"
    const articles = featuresSection.querySelectorAll('article, [role="article"], .feature-card');
    if (articles.length >= 4) {
      console.log(`✓ Features section contains ${articles.length} feature cards`);
      passed++;
      results.push({ test: 'feature cards', status: 'pass' });
    } else {
      console.log(`✗ Features section should contain at least 4 feature cards (found ${articles.length})`);
      failed++;
      results.push({ test: 'feature cards', status: 'fail', error: `Expected at least 4 feature cards, found ${articles.length}` });
    }
  }

  // Test 4: Feature cards have proper heading structure (h3 for card titles)
  function testCardHeadings() {
    const featuresSection = document.querySelector('section#features');
    if (!featuresSection) {
      console.log('✗ Cannot test card headings - features section not found');
      failed++;
      results.push({ test: 'card headings', status: 'fail', error: 'Features section not found' });
      return;
    }

    const featureCards = featuresSection.querySelectorAll('article, [role="article"], .feature-card');
    let cardsWithHeadings = 0;

    featureCards.forEach(card => {
      const heading = card.querySelector('h3');
      if (heading) {
        cardsWithHeadings++;
      }
    });

    if (cardsWithHeadings >= 4) {
      console.log(`✓ Feature cards have proper h3 headings (${cardsWithHeadings} cards)`);
      passed++;
      results.push({ test: 'card headings', status: 'pass' });
    } else {
      console.log(`✗ Feature cards should have h3 headings (found ${cardsWithHeadings} with h3)`);
      failed++;
      results.push({ test: 'card headings', status: 'fail', error: `Expected 4 cards with h3 headings, found ${cardsWithHeadings}` });
    }
  }

  // Run all tests
  testSectionElement();
  testHeadingHierarchy();
  testFeatureCards();
  testCardHeadings();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
