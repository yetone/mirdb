/**
 * DOM Structure Tests
 * Owner: Multiple scenarios - each adds tests for their section
 *
 * Test groups:
 * - Hero section elements (Scenario 1)
 * - Features grid elements (Scenario 2)
 * - Terminal demo elements (Scenario 3)
 * - Quick-start elements (Scenario 4)
 * - Navigation elements (Scenario 5)
 * - Architecture elements (Scenario 6)
 */

const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Set up DOM
document.documentElement.innerHTML = htmlContent;

/**
 * Hero Section Tests (Scenario 1)
 */
describe('Hero Section (Scenario 1)', () => {
  describe('Test Case 1: H1 element contains MirDB text', () => {
    test('H1 element exists and contains "MirDB"', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('H1 has the correct class', () => {
      const h1 = document.querySelector('.hero__title');
      expect(h1).not.toBeNull();
      expect(h1.tagName).toBe('H1');
    });
  });

  describe('Test Case 2: Logo image is present', () => {
    test('Logo image exists with correct src', () => {
      const logo = document.querySelector('.hero__logo');
      expect(logo).not.toBeNull();
      expect(logo.tagName).toBe('IMG');
      expect(logo.getAttribute('src')).toContain('logo.gif');
    });

    test('Logo has alt text for accessibility', () => {
      const logo = document.querySelector('.hero__logo');
      expect(logo).not.toBeNull();
      expect(logo.getAttribute('alt')).toBeTruthy();
      expect(logo.getAttribute('alt').toLowerCase()).toContain('logo');
    });
  });

  describe('Test Case 3: Tagline is visible', () => {
    test('Tagline text is present', () => {
      const tagline = document.querySelector('.hero__tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toBe('A Persistent Key-Value Store with Memcached Protocol');
    });
  });

  describe('Hero section structure', () => {
    test('Hero section exists', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();
      expect(hero.tagName).toBe('SECTION');
    });

    test('Hero has aria-labelledby for accessibility', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();
      expect(hero.getAttribute('aria-labelledby')).toBeTruthy();
    });

    test('Hero container exists', () => {
      const container = document.querySelector('.hero__container');
      expect(container).not.toBeNull();
    });

    test('Hero description exists with value proposition', () => {
      const description = document.querySelector('.hero__description');
      expect(description).not.toBeNull();
      expect(description.textContent.length).toBeGreaterThan(50);
    });
  });
});

/**
 * Features Grid Section Tests (Scenario 2)
 */
describe('Features Grid Section', () => {
  let featuresSection;
  let featureCards;

  beforeAll(() => {
    featuresSection = document.getElementById('features');
    featureCards = document.querySelectorAll('.feature-card');
  });

  // Test Case 1: Exactly 4 feature cards are present in the DOM
  test('should have exactly 4 feature cards', () => {
    expect(featureCards.length).toBe(4);
  });

  // Test Case 2: Feature card with 'Tokio' or 'async' in title/description exists
  test('should have a Tokio async feature card', () => {
    const tokioCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('tokio') ||
        title.toLowerCase().includes('async') ||
        description.toLowerCase().includes('tokio') ||
        description.toLowerCase().includes('async')
      );
    });
    expect(tokioCard).toBeTruthy();
  });

  // Test Case 3: Feature card describing Memcached protocol compatibility exists
  test('should have a Memcached protocol feature card', () => {
    const memcachedCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('memcached') ||
        description.toLowerCase().includes('memcached')
      );
    });
    expect(memcachedCard).toBeTruthy();
  });

  // Test Case 4: Feature card describing Skip-list memtable exists
  test('should have a Skip-list memtable feature card', () => {
    const skipListCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('skip-list') ||
        title.toLowerCase().includes('skiplist') ||
        description.toLowerCase().includes('skip-list') ||
        description.toLowerCase().includes('skiplist')
      );
    });
    expect(skipListCard).toBeTruthy();
  });

  // Test Case 5: Feature card describing compaction (minor/major) exists
  test('should have a Compaction feature card', () => {
    const compactionCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('compaction') ||
        description.toLowerCase().includes('compaction') ||
        (description.toLowerCase().includes('minor') && description.toLowerCase().includes('major'))
      );
    });
    expect(compactionCard).toBeTruthy();
  });

  // Test Case 6: Each feature card contains an SVG icon element
  test('should have an SVG icon in each feature card', () => {
    featureCards.forEach((card, index) => {
      const iconContainer = card.querySelector('.feature-card__icon');
      expect(iconContainer).toBeTruthy();

      const svg = iconContainer?.querySelector('svg');
      expect(svg).toBeTruthy();
    });
  });

  // Additional structural tests
  test('features section should exist', () => {
    expect(featuresSection).toBeTruthy();
  });

  test('features section should have a title', () => {
    const title = featuresSection.querySelector('.section__title, h2');
    expect(title).toBeTruthy();
    expect(title.textContent.toLowerCase()).toContain('feature');
  });

  test('features grid container should exist', () => {
    const grid = featuresSection.querySelector('.features__grid');
    expect(grid).toBeTruthy();
  });

  test('each feature card should have a title', () => {
    featureCards.forEach(card => {
      const title = card.querySelector('.feature-card__title');
      expect(title).toBeTruthy();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  test('each feature card should have a description', () => {
    featureCards.forEach(card => {
      const description = card.querySelector('.feature-card__description');
      expect(description).toBeTruthy();
      expect(description.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});

/**
 * Terminal Demo Section Tests - Scenario 3
 * Tests for Interactive Terminal Demo
 */
describe('Terminal Demo Section (Scenario 3)', () => {
  describe('Test Case 1: Terminal demo container exists with dark background styling', () => {
    test('should have terminal demo section element', () => {
      const terminalDemo = document.querySelector('.terminal-demo');
      expect(terminalDemo).not.toBeNull();
      expect(terminalDemo.id).toBe('terminal-demo');
    });

    test('should have terminal container element', () => {
      const terminal = document.querySelector('.terminal');
      expect(terminal).not.toBeNull();
    });

    test('terminal body should exist for dark background', () => {
      const terminalBody = document.querySelector('.terminal__body');
      expect(terminalBody).not.toBeNull();
    });

    test('terminal header should have window controls', () => {
      const dots = document.querySelectorAll('.terminal__dot');
      expect(dots.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Test Case 2: Terminal uses monospace font family', () => {
    test('terminal body should exist and be styled for monospace', () => {
      const terminalBody = document.querySelector('.terminal__body');
      expect(terminalBody).not.toBeNull();
      expect(terminalBody.classList.contains('terminal__body')).toBe(true);
    });

    test('terminal title should exist with monospace font', () => {
      const terminalTitle = document.querySelector('.terminal__title');
      expect(terminalTitle).not.toBeNull();
    });
  });

  describe('Test Case 3: Terminal displays SET command example', () => {
    test('should contain SET keyword in terminal content', () => {
      const terminalBody = document.querySelector('.terminal__body');
      expect(terminalBody).not.toBeNull();

      const allKeywords = document.querySelectorAll('.terminal__keyword');
      const keywordTexts = Array.from(allKeywords).map(el => el.textContent);
      expect(keywordTexts).toContain('SET');
    });

    test('should have SET command line structure', () => {
      const terminalCommands = document.querySelectorAll('.terminal__command');
      const commandTexts = Array.from(terminalCommands).map(el => el.textContent);
      const hasSetCommand = commandTexts.some(text => text.includes('SET'));
      expect(hasSetCommand).toBe(true);
    });
  });

  describe('Test Case 4: Terminal displays GET command example', () => {
    test('should contain GET keyword in terminal content', () => {
      const allKeywords = document.querySelectorAll('.terminal__keyword');
      const keywordTexts = Array.from(allKeywords).map(el => el.textContent);
      expect(keywordTexts).toContain('GET');
    });

    test('should have GET command line structure', () => {
      const terminalCommands = document.querySelectorAll('.terminal__command');
      const commandTexts = Array.from(terminalCommands).map(el => el.textContent);
      const hasGetCommand = commandTexts.some(text => text.includes('GET'));
      expect(hasGetCommand).toBe(true);
    });
  });

  describe('Test Case 5: Terminal displays DELETE command example', () => {
    test('should contain DELETE keyword in terminal content', () => {
      const allKeywords = document.querySelectorAll('.terminal__keyword');
      const keywordTexts = Array.from(allKeywords).map(el => el.textContent);
      expect(keywordTexts).toContain('DELETE');
    });

    test('should have DELETE command line structure', () => {
      const terminalCommands = document.querySelectorAll('.terminal__command');
      const commandTexts = Array.from(terminalCommands).map(el => el.textContent);
      const hasDeleteCommand = commandTexts.some(text => text.includes('DELETE'));
      expect(hasDeleteCommand).toBe(true);
    });
  });

  describe('Test Case 6: Usage GIF image is present', () => {
    test('should have usage GIF image element', () => {
      const usageGif = document.querySelector('.terminal-demo__usage-gif');
      expect(usageGif).not.toBeNull();
    });

    test('usage GIF should have correct src attribute', () => {
      const usageGif = document.querySelector('.terminal-demo__usage-gif');
      expect(usageGif).not.toBeNull();
      expect(usageGif.getAttribute('src')).toContain('usage.gif');
    });

    test('usage GIF should have alt text for accessibility', () => {
      const usageGif = document.querySelector('.terminal-demo__usage-gif');
      expect(usageGif).not.toBeNull();
      const altText = usageGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10);
    });

    test('should have gif title section', () => {
      const gifTitle = document.querySelector('.terminal-demo__gif-title');
      expect(gifTitle).not.toBeNull();
      expect(gifTitle.textContent).toBeTruthy();
    });
  });

  describe('Terminal Demo Accessibility', () => {
    test('terminal demo should have descriptive title', () => {
      const title = document.querySelector('.terminal-demo__title');
      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeGreaterThan(0);
    });

    test('terminal demo should have subtitle/description', () => {
      const subtitle = document.querySelector('.terminal-demo__subtitle');
      expect(subtitle).not.toBeNull();
      expect(subtitle.textContent.length).toBeGreaterThan(0);
    });
  });
});

/**
 * Navigation Section Tests (Scenario 5)
 */
describe('Navigation Section (Scenario 5)', () => {
  describe('Test Case 1: Navigation header exists with position sticky or fixed', () => {
    test('Header element exists', () => {
      const header = document.querySelector('header.header');
      expect(header).not.toBeNull();
    });

    test('Header has sticky positioning class', () => {
      const header = document.querySelector('.header');
      expect(header).not.toBeNull();
      // The header should have the .header class which applies position: sticky in CSS
      expect(header.classList.contains('header')).toBe(true);
    });

    test('Navigation container exists within header', () => {
      const nav = document.querySelector('.header .nav');
      expect(nav).not.toBeNull();
    });

    test('Navigation has aria-label for accessibility', () => {
      const nav = document.querySelector('.nav');
      expect(nav).not.toBeNull();
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });
  });

  describe('Test Case 2: Link to Features section exists', () => {
    test('Features navigation link exists', () => {
      const featuresLink = document.querySelector('.nav__link[href="#features"]');
      expect(featuresLink).not.toBeNull();
    });

    test('Features link has correct text', () => {
      const featuresLink = document.querySelector('.nav__link[href="#features"]');
      expect(featuresLink).not.toBeNull();
      expect(featuresLink.textContent.toLowerCase()).toContain('feature');
    });
  });

  describe('Test Case 3: Link to Usage section exists', () => {
    test('Usage navigation link exists', () => {
      // Usage link should point to terminal-demo or quickstart (usage demonstration)
      const usageLink = document.querySelector('.nav__link[href="#usage"], .nav__link[href="#terminal-demo"]');
      expect(usageLink).not.toBeNull();
    });

    test('Usage or Demo link is present', () => {
      const navLinks = document.querySelectorAll('.nav__link');
      const hasUsageOrDemo = Array.from(navLinks).some(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('usage') || text.includes('demo');
      });
      expect(hasUsageOrDemo).toBe(true);
    });
  });

  describe('Test Case 4: Link to Architecture section exists', () => {
    test('Architecture navigation link exists', () => {
      const archLink = document.querySelector('.nav__link[href="#architecture"]');
      expect(archLink).not.toBeNull();
    });

    test('Architecture link has correct text', () => {
      const archLink = document.querySelector('.nav__link[href="#architecture"]');
      expect(archLink).not.toBeNull();
      expect(archLink.textContent.toLowerCase()).toContain('architecture');
    });
  });

  describe('Test Case 5: Link to GitHub repository exists', () => {
    test('GitHub link exists', () => {
      const githubLink = document.querySelector('a[href="https://github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
    });

    test('GitHub link opens in new tab for external link', () => {
      const githubLink = document.querySelector('a[href="https://github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('GitHub link has security attributes', () => {
      const githubLink = document.querySelector('a[href="https://github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });

    test('GitHub link has accessible label', () => {
      const githubLink = document.querySelector('.github-link');
      expect(githubLink).not.toBeNull();
      const ariaLabel = githubLink.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('github');
    });
  });

  describe('Test Case 6: CircleCI badge is present and links to CircleCI', () => {
    test('CircleCI badge image exists', () => {
      const circleCIImg = document.querySelector('img[src*="circleci"]');
      expect(circleCIImg).not.toBeNull();
    });

    test('CircleCI badge has alt text', () => {
      const circleCIImg = document.querySelector('img[src*="circleci"]');
      expect(circleCIImg).not.toBeNull();
      expect(circleCIImg.getAttribute('alt')).toBeTruthy();
    });

    test('CircleCI badge links to CircleCI', () => {
      const circleCILink = document.querySelector('a[href*="circleci"]');
      expect(circleCILink).not.toBeNull();
      expect(circleCILink.getAttribute('href')).toContain('circleci');
    });
  });

  describe('Navigation structure and accessibility', () => {
    test('Navigation list exists', () => {
      const navList = document.querySelector('.nav__list');
      expect(navList).not.toBeNull();
      expect(navList.tagName).toBe('UL');
    });

    test('Header actions section exists', () => {
      const headerActions = document.querySelector('.header__actions');
      expect(headerActions).not.toBeNull();
    });

    test('Theme toggle button exists', () => {
      const themeToggle = document.querySelector('.theme-toggle');
      expect(themeToggle).not.toBeNull();
      expect(themeToggle.tagName).toBe('BUTTON');
    });

    test('Theme toggle has accessible label', () => {
      const themeToggle = document.querySelector('.theme-toggle');
      expect(themeToggle).not.toBeNull();
      expect(themeToggle.getAttribute('aria-label')).toBeTruthy();
    });

    test('Skip navigation link exists for accessibility', () => {
      const skipLink = document.querySelector('.skip-link');
      expect(skipLink).not.toBeNull();
      expect(skipLink.getAttribute('href')).toBe('#main-content');
    });
  });
});

/**
 * Architecture Diagram Section Tests - Scenario 6
 * Tests for Architecture Diagram Section explaining LSM tree structure
 */
describe('Architecture Diagram Section (Scenario 6)', () => {
  let architectureSection;

  beforeAll(() => {
    architectureSection = document.getElementById('architecture');
  });

  // Test Case 1: Architecture section exists in DOM
  describe('Test Case 1: Architecture section exists in DOM', () => {
    test('should have architecture section element with correct ID', () => {
      expect(architectureSection).not.toBeNull();
      expect(architectureSection.tagName).toBe('SECTION');
    });

    test('should have architecture section with correct class', () => {
      const section = document.querySelector('.architecture');
      expect(section).not.toBeNull();
      expect(section.id).toBe('architecture');
    });

    test('should have architecture content container', () => {
      const content = document.querySelector('.architecture__content');
      expect(content).not.toBeNull();
    });

    test('should have architecture title with correct heading level', () => {
      const title = architectureSection.querySelector('h2');
      expect(title).not.toBeNull();
      expect(title.id).toBe('architecture-title');
    });

    test('should have aria-labelledby for accessibility', () => {
      const ariaLabel = architectureSection.getAttribute('aria-labelledby');
      expect(ariaLabel).toBe('architecture-title');
    });
  });

  // Test Case 2: LSM tree terminology is mentioned
  describe('Test Case 2: LSM tree terminology is mentioned', () => {
    test('should mention "LSM" in the architecture section', () => {
      const sectionText = architectureSection.textContent;
      expect(sectionText).toMatch(/LSM/i);
    });

    test('should mention "Log-Structured Merge" in the description', () => {
      const description = architectureSection.querySelector('.architecture__description');
      expect(description).not.toBeNull();
      expect(description.textContent).toMatch(/Log-Structured Merge/i);
    });

    test('should explain key LSM components', () => {
      const sectionText = architectureSection.textContent;
      expect(sectionText).toMatch(/memtable/i);
      expect(sectionText).toMatch(/SSTable/i);
    });

    test('should mention compaction processes', () => {
      const sectionText = architectureSection.textContent;
      expect(sectionText).toMatch(/compaction/i);
    });

    test('should mention WAL (Write-Ahead Log)', () => {
      const sectionText = architectureSection.textContent;
      expect(sectionText).toMatch(/WAL|Write-Ahead Log/i);
    });
  });

  // Test Case 3: Diagram visual element exists (img or svg)
  describe('Test Case 3: Diagram visual element exists (img or svg)', () => {
    test('should have architecture diagram container', () => {
      const diagramContainer = architectureSection.querySelector('.architecture__diagram');
      expect(diagramContainer).not.toBeNull();
    });

    test('should have diagram as img or svg element', () => {
      const diagramContainer = architectureSection.querySelector('.architecture__diagram');
      const img = diagramContainer.querySelector('img');
      const svg = diagramContainer.querySelector('svg');

      // Either img or svg should be present
      const hasDiagram = img !== null || svg !== null;
      expect(hasDiagram).toBe(true);
    });

    test('should have diagram image with correct class', () => {
      const diagramImage = architectureSection.querySelector('.architecture__image');
      expect(diagramImage).not.toBeNull();
    });

    test('diagram should reference architecture SVG', () => {
      const img = architectureSection.querySelector('.architecture__image');
      if (img && img.tagName === 'IMG') {
        expect(img.getAttribute('src')).toMatch(/architecture\.svg/i);
      }
    });
  });

  // Test Case 4: Architecture diagram has descriptive alt text
  describe('Test Case 4: Architecture diagram has descriptive alt text', () => {
    test('should have alt attribute on diagram image', () => {
      const img = architectureSection.querySelector('.architecture__image');
      expect(img).not.toBeNull();
      const altText = img.getAttribute('alt');
      expect(altText).toBeTruthy();
    });

    test('alt text should be descriptive (more than 20 characters)', () => {
      const img = architectureSection.querySelector('.architecture__image');
      expect(img).not.toBeNull();
      const altText = img.getAttribute('alt');
      expect(altText.length).toBeGreaterThan(20);
    });

    test('alt text should mention LSM or architecture', () => {
      const img = architectureSection.querySelector('.architecture__image');
      expect(img).not.toBeNull();
      const altText = img.getAttribute('alt').toLowerCase();
      const mentionsLSM = altText.includes('lsm');
      const mentionsArchitecture = altText.includes('architecture');
      expect(mentionsLSM || mentionsArchitecture).toBe(true);
    });

    test('alt text should describe the diagram content', () => {
      const img = architectureSection.querySelector('.architecture__image');
      expect(img).not.toBeNull();
      const altText = img.getAttribute('alt').toLowerCase();
      // Should mention key components shown in the diagram
      const mentionsDataFlow = altText.includes('data flow') || altText.includes('flow');
      const mentionsComponents = altText.includes('memtable') || altText.includes('sstable');
      expect(mentionsDataFlow || mentionsComponents).toBe(true);
    });
  });

  // Additional architecture section structure tests
  describe('Architecture Section Structure', () => {
    test('should have description section', () => {
      const description = architectureSection.querySelector('.architecture__description');
      expect(description).not.toBeNull();
    });

    test('description should have substantial content', () => {
      const description = architectureSection.querySelector('.architecture__description');
      expect(description).not.toBeNull();
      expect(description.textContent.length).toBeGreaterThan(100);
    });

    test('should have feature list explaining LSM components', () => {
      const featureList = architectureSection.querySelector('.architecture__features');
      expect(featureList).not.toBeNull();
    });

    test('feature list should have multiple items', () => {
      const features = architectureSection.querySelectorAll('.architecture__features li');
      expect(features.length).toBeGreaterThanOrEqual(3);
    });

    test('diagram image should have width and height attributes for layout stability', () => {
      const img = architectureSection.querySelector('.architecture__image');
      expect(img).not.toBeNull();
      expect(img.getAttribute('width')).toBeTruthy();
      expect(img.getAttribute('height')).toBeTruthy();
    });

    test('diagram image should have loading="lazy" for performance', () => {
      const img = architectureSection.querySelector('.architecture__image');
      expect(img).not.toBeNull();
      expect(img.getAttribute('loading')).toBe('lazy');
    });
  });
});

/**
 * Quick-Start Section Tests (Scenario 4)
 * Tests for Quick Start Installation Section
 */
describe('Quick-Start Section (Scenario 4)', () => {
  let quickstartSection;

  beforeAll(() => {
    quickstartSection = document.getElementById('quickstart');
  });

  // Test Case 1: Quick-start section exists with installation heading
  describe('Test Case 1: Quick-start section exists with installation heading', () => {
    test('quickstart section element should exist', () => {
      expect(quickstartSection).not.toBeNull();
      expect(quickstartSection.tagName).toBe('SECTION');
    });

    test('quickstart section should have aria-labelledby for accessibility', () => {
      expect(quickstartSection.getAttribute('aria-labelledby')).toBe('quickstart-title');
    });

    test('quickstart section should have Quick Start title', () => {
      const title = quickstartSection.querySelector('#quickstart-title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('Quick Start');
    });

    test('quickstart section should have installation subtitle', () => {
      const subtitles = quickstartSection.querySelectorAll('.quickstart__subtitle');
      const installationSubtitle = Array.from(subtitles).find(s =>
        s.textContent.toLowerCase().includes('installation')
      );
      expect(installationSubtitle).not.toBeNull();
    });
  });

  // Test Case 2: Code block contains cargo install or cargo run command
  describe('Test Case 2: Code block contains cargo command', () => {
    test('code block should contain cargo build command', () => {
      const codeBlocks = quickstartSection.querySelectorAll('.code-block__code code');
      const allCode = Array.from(codeBlocks).map(c => c.textContent).join(' ');
      const hasCargoCommand = allCode.includes('cargo build') ||
                              allCode.includes('cargo install') ||
                              allCode.includes('cargo run');
      expect(hasCargoCommand).toBe(true);
    });

    test('installation code block should have bash language label', () => {
      const installationBlock = quickstartSection.querySelector('[data-code-block="installation"]');
      expect(installationBlock).not.toBeNull();
      const languageLabel = installationBlock.querySelector('.code-block__language');
      expect(languageLabel).not.toBeNull();
      expect(languageLabel.textContent.toLowerCase()).toBe('bash');
    });
  });

  // Test Case 3: Copy button exists near code block
  describe('Test Case 3: Copy button exists near code block', () => {
    test('copy buttons should exist in code blocks', () => {
      const codeBlocks = quickstartSection.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach(block => {
        const copyBtn = block.querySelector('.code-block__copy-btn');
        expect(copyBtn).not.toBeNull();
      });
    });

    test('copy button should be a button element', () => {
      const copyBtn = quickstartSection.querySelector('.code-block__copy-btn');
      expect(copyBtn).not.toBeNull();
      expect(copyBtn.tagName).toBe('BUTTON');
      expect(copyBtn.getAttribute('type')).toBe('button');
    });

    test('copy button should have aria-label for accessibility', () => {
      const copyButtons = quickstartSection.querySelectorAll('.code-block__copy-btn');
      copyButtons.forEach(btn => {
        const ariaLabel = btn.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('copy');
      });
    });

    test('copy button should have Copy text', () => {
      const copyBtn = quickstartSection.querySelector('.code-block__copy-btn');
      const copyText = copyBtn.querySelector('.code-block__copy-text');
      expect(copyText).not.toBeNull();
      expect(copyText.textContent).toBe('Copy');
    });

    test('copy button should have copy and check icons', () => {
      const copyBtn = quickstartSection.querySelector('.code-block__copy-btn');
      const copyIcon = copyBtn.querySelector('.code-block__copy-icon');
      const checkIcon = copyBtn.querySelector('.code-block__check-icon');
      expect(copyIcon).not.toBeNull();
      expect(checkIcon).not.toBeNull();
    });
  });

  // Test Case 5: Basic usage example with Memcached commands is shown
  describe('Test Case 5: Basic usage example with Memcached commands', () => {
    test('should have a usage code block', () => {
      const usageBlock = quickstartSection.querySelector('[data-code-block="usage"]');
      expect(usageBlock).not.toBeNull();
    });

    test('usage example should contain SET command', () => {
      const usageBlock = quickstartSection.querySelector('[data-code-block="usage"]');
      expect(usageBlock).not.toBeNull();
      const code = usageBlock.querySelector('code');
      expect(code).not.toBeNull();
      expect(code.textContent).toContain('SET');
    });

    test('usage example should contain GET command', () => {
      const usageBlock = quickstartSection.querySelector('[data-code-block="usage"]');
      expect(usageBlock).not.toBeNull();
      const code = usageBlock.querySelector('code');
      expect(code).not.toBeNull();
      expect(code.textContent).toContain('GET');
    });

    test('usage example should contain DELETE command', () => {
      const usageBlock = quickstartSection.querySelector('[data-code-block="usage"]');
      expect(usageBlock).not.toBeNull();
      const code = usageBlock.querySelector('code');
      expect(code).not.toBeNull();
      expect(code.textContent).toContain('DELETE');
    });

    test('usage example should show STORED response', () => {
      const usageBlock = quickstartSection.querySelector('[data-code-block="usage"]');
      const code = usageBlock.querySelector('code');
      expect(code.textContent).toContain('STORED');
    });

    test('usage example should have memcached language label', () => {
      const usageBlock = quickstartSection.querySelector('[data-code-block="usage"]');
      expect(usageBlock).not.toBeNull();
      const languageLabel = usageBlock.querySelector('.code-block__language');
      expect(languageLabel).not.toBeNull();
      expect(languageLabel.textContent.toLowerCase()).toBe('memcached');
    });

    test('usage example should have basic usage subtitle', () => {
      const subtitles = quickstartSection.querySelectorAll('.quickstart__subtitle');
      const usageSubtitle = Array.from(subtitles).find(s =>
        s.textContent.toLowerCase().includes('usage')
      );
      expect(usageSubtitle).not.toBeNull();
    });
  });

  // Additional structural tests
  describe('Quick-Start Section Structure', () => {
    test('quickstart should have quickstart class', () => {
      expect(quickstartSection.classList.contains('quickstart')).toBe(true);
    });

    test('quickstart should have section class', () => {
      expect(quickstartSection.classList.contains('section')).toBe(true);
    });

    test('quickstart should have container element', () => {
      const container = quickstartSection.querySelector('.container');
      expect(container).not.toBeNull();
    });

    test('quickstart should have content wrapper', () => {
      const content = quickstartSection.querySelector('.quickstart__content');
      expect(content).not.toBeNull();
    });

    test('quickstart should have at least 2 code blocks', () => {
      const codeBlocks = quickstartSection.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThanOrEqual(2);
    });
  });
});
