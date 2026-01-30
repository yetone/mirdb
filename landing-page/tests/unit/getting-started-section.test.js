/**
 * Getting Started Section Unit Tests
 * Owner: Scenario 6 - Getting Started Section
 *
 * Tests for validating HTML structure of the Getting Started section
 */
const fs = require('fs');
const path = require('path');

describe('Getting Started Section HTML Structure', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC1: Getting Started section HTML structure', () => {
    test('Contains getting-started section with correct id', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="getting-started"[^>]*class="getting-started/);
    });

    test('Contains section title "Get Started in Minutes"', () => {
      expect(htmlContent).toMatch(/Get Started in Minutes/);
    });

    test('Has getting-started__title class on section title', () => {
      expect(htmlContent).toMatch(/<h2[^>]*class="getting-started__title"[^>]*>/);
    });

    test('Contains installation steps numbered 1, 2, 3', () => {
      expect(htmlContent).toMatch(/<span[^>]*class="getting-started__step-number"[^>]*>1<\/span>/);
      expect(htmlContent).toMatch(/<span[^>]*class="getting-started__step-number"[^>]*>2<\/span>/);
      expect(htmlContent).toMatch(/<span[^>]*class="getting-started__step-number"[^>]*>3<\/span>/);
    });

    test('Contains step headers with titles', () => {
      expect(htmlContent).toMatch(/Install MirDB/);
      expect(htmlContent).toMatch(/Configure/);
      expect(htmlContent).toMatch(/Start the Server/);
    });

    test('Contains documentation link', () => {
      // Check that the docs link exists with both the class and href (order may vary)
      expect(htmlContent).toMatch(/getting-started__docs-link/);
      expect(htmlContent).toMatch(/href="https:\/\/github\.com\/yetone\/mirdb#readme"/);
    });
  });

  describe('TC2: Installation command present', () => {
    test('Contains cargo install command for MirDB', () => {
      expect(htmlContent).toMatch(/cargo install mirdb/);
    });

    test('Installation command is in a code block', () => {
      // The cargo install command should be inside a code element
      expect(htmlContent).toMatch(/<code[^>]*class="language-bash"[^>]*>[^<]*cargo install mirdb/);
    });

    test('Installation section has descriptive text', () => {
      expect(htmlContent).toMatch(/Rust's package manager/);
    });
  });

  describe('TC5: Configuration example content', () => {
    test('Shows basic TOML configuration', () => {
      expect(htmlContent).toMatch(/\[server\]/);
      expect(htmlContent).toMatch(/\[storage\]/);
    });

    test('Shows port configuration option', () => {
      expect(htmlContent).toMatch(/port\s*=\s*11211/);
    });

    test('Shows host configuration option', () => {
      expect(htmlContent).toMatch(/host\s*=\s*["']127\.0\.0\.1["']/);
    });

    test('Shows data_dir configuration option', () => {
      expect(htmlContent).toMatch(/data_dir\s*=/);
    });

    test('Shows WAL configuration option', () => {
      expect(htmlContent).toMatch(/wal_enabled\s*=\s*true/);
    });

    test('Shows memtable configuration section', () => {
      expect(htmlContent).toMatch(/\[memtable\]/);
      expect(htmlContent).toMatch(/max_size\s*=/);
    });

    test('Configuration is in TOML code block', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="code-block"[^>]*data-language="toml"/);
    });

    test('Configuration file name reference present', () => {
      expect(htmlContent).toMatch(/mirdb\.toml/);
    });
  });

  describe('Code blocks and copy buttons', () => {
    test('Contains multiple code blocks in getting-started section', () => {
      // Should have at least 3 code blocks (install, config, run)
      const gettingStartedSection = htmlContent.match(
        /<!-- SECTION: Getting Started[\s\S]*<!-- END SECTION: Getting Started -->/
      );
      expect(gettingStartedSection).toBeTruthy();
      const codeBlockMatches = gettingStartedSection[0].match(/<div[^>]*class="code-block"[^>]*>/g);
      expect(codeBlockMatches).toBeTruthy();
      expect(codeBlockMatches.length).toBeGreaterThanOrEqual(3);
    });

    test('Each code block has a copy button', () => {
      const gettingStartedSection = htmlContent.match(
        /<!-- SECTION: Getting Started[\s\S]*<!-- END SECTION: Getting Started -->/
      );
      expect(gettingStartedSection).toBeTruthy();
      const copyButtonMatches = gettingStartedSection[0].match(
        /<button[^>]*class="code-block__copy"[^>]*>/g
      );
      expect(copyButtonMatches).toBeTruthy();
      expect(copyButtonMatches.length).toBeGreaterThanOrEqual(3);
    });

    test('Copy buttons have proper accessibility attributes', () => {
      expect(htmlContent).toMatch(/<button[^>]*class="code-block__copy"[^>]*type="button"[^>]*aria-label="Copy code to clipboard"/);
    });
  });

  describe('Documentation link', () => {
    test('Contains link to full documentation', () => {
      expect(htmlContent).toMatch(/View Full Documentation/);
    });

    test('Documentation link points to GitHub README', () => {
      expect(htmlContent).toMatch(/href="https:\/\/github\.com\/yetone\/mirdb#readme"/);
    });

    test('Documentation link opens in new tab', () => {
      // Check for target="_blank" on the docs link
      const docsLinkMatch = htmlContent.match(
        /<a[^>]*getting-started__docs-link[^>]*>/
      );
      expect(docsLinkMatch).toBeTruthy();
      expect(docsLinkMatch[0]).toMatch(/target="_blank"/);
    });

    test('Documentation link has security attributes', () => {
      const docsLinkMatch = htmlContent.match(
        /<a[^>]*getting-started__docs-link[^>]*>/
      );
      expect(docsLinkMatch).toBeTruthy();
      expect(docsLinkMatch[0]).toMatch(/rel="noopener noreferrer"/);
    });

    test('Documentation section has intro text', () => {
      expect(htmlContent).toMatch(/Ready to dive deeper\?/);
    });
  });

  describe('Accessibility', () => {
    test('Section has semantic structure', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="getting-started"/);
    });

    test('Uses heading hierarchy (h2 for section, h3 for steps)', () => {
      const gettingStartedSection = htmlContent.match(
        /<!-- SECTION: Getting Started[\s\S]*<!-- END SECTION: Getting Started -->/
      );
      expect(gettingStartedSection).toBeTruthy();
      expect(gettingStartedSection[0]).toMatch(/<h2[^>]*class="getting-started__title"/);
      expect(gettingStartedSection[0]).toMatch(/<h3[^>]*class="getting-started__step-title"/);
    });

    test('Code elements use semantic pre/code structure', () => {
      expect(htmlContent).toMatch(/<pre><code[^>]*class="language-/);
    });

    test('SVG icons in copy buttons are hidden from screen readers', () => {
      const gettingStartedSection = htmlContent.match(
        /<!-- SECTION: Getting Started[\s\S]*<!-- END SECTION: Getting Started -->/
      );
      expect(gettingStartedSection).toBeTruthy();
      expect(gettingStartedSection[0]).toMatch(/aria-hidden="true"/);
    });
  });

  describe('Server start instructions', () => {
    test('Contains basic start command', () => {
      expect(htmlContent).toMatch(/mirdb\s*\n/);
    });

    test('Contains config flag example', () => {
      expect(htmlContent).toMatch(/--config\s+mirdb\.toml/);
    });

    test('Contains comments explaining usage', () => {
      expect(htmlContent).toMatch(/#\s*Start with default settings/);
      expect(htmlContent).toMatch(/#\s*Or with custom config/);
    });
  });
});
