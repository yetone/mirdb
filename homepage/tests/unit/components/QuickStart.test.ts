/**
 * Unit tests for QuickStart component.
 * Owner: Scenario 3 - Quick Start Section
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { renderQuickStart } from '@/components/QuickStart';

describe('QuickStart component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderQuickStart();
    document.body.appendChild(section);
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  describe('section structure', () => {
    it('should render a section element with correct id', () => {
      expect(section.tagName).toBe('SECTION');
      expect(section.id).toBe('quickstart');
    });

    it('should have correct class name', () => {
      expect(section.classList.contains('quickstart-section')).toBe(true);
    });

    it('should have aria-labelledby attribute', () => {
      expect(section.getAttribute('aria-labelledby')).toBe('quickstart-title');
    });

    it('should have a title with correct id', () => {
      const title = section.querySelector('#quickstart-title');
      expect(title).not.toBeNull();
      expect(title?.tagName).toBe('H2');
      expect(title?.textContent).toBe('Quick Start');
    });

    it('should have an intro paragraph', () => {
      const intro = section.querySelector('.quickstart-intro');
      expect(intro).not.toBeNull();
      expect(intro?.textContent).toContain('MirDB');
    });
  });

  describe('installation command (test case 1)', () => {
    it('should contain installation command', () => {
      const codeBlocks = section.querySelectorAll('pre code');
      const codeTexts = Array.from(codeBlocks).map(block => block.textContent);

      const hasInstallCommand = codeTexts.some(text => text?.includes('cargo install mirdb'));
      expect(hasInstallCommand).toBe(true);
    });

    it('should have installation step with correct title', () => {
      const stepTitles = section.querySelectorAll('.quickstart-step h3');
      const titles = Array.from(stepTitles).map(t => t.textContent);

      expect(titles.some(t => t?.includes('Install'))).toBe(true);
    });
  });

  describe('server startup command (test case 2)', () => {
    it('should contain server startup command', () => {
      const codeBlocks = section.querySelectorAll('pre code');
      const codeTexts = Array.from(codeBlocks).map(block => block.textContent);

      const hasStartCommand = codeTexts.some(text => text?.includes('mirdb'));
      expect(hasStartCommand).toBe(true);
    });

    it('should have start server step with correct title', () => {
      const stepTitles = section.querySelectorAll('.quickstart-step h3');
      const titles = Array.from(stepTitles).map(t => t.textContent);

      expect(titles.some(t => t?.includes('Start') && t?.includes('Server'))).toBe(true);
    });
  });

  describe('example Memcached commands (test case 3)', () => {
    it('should contain example Memcached commands', () => {
      const codeBlocks = section.querySelectorAll('pre code');
      const codeTexts = Array.from(codeBlocks).map(block => block.textContent).join('\n');

      // Check for SET command
      expect(codeTexts).toContain('set');
      // Check for GET command
      expect(codeTexts).toContain('get');
      // Check for DELETE command
      expect(codeTexts).toContain('delete');
    });

    it('should show expected responses', () => {
      const codeBlocks = section.querySelectorAll('pre code');
      const codeTexts = Array.from(codeBlocks).map(block => block.textContent).join('\n');

      expect(codeTexts).toContain('STORED');
      expect(codeTexts).toContain('DELETED');
    });

    it('should have connect and use step', () => {
      const stepTitles = section.querySelectorAll('.quickstart-step h3');
      const titles = Array.from(stepTitles).map(t => t.textContent);

      expect(titles.some(t => t?.includes('Connect'))).toBe(true);
    });
  });

  describe('copy buttons (test case 4)', () => {
    it('should have copy buttons for all code blocks', () => {
      const codeBlockWrappers = section.querySelectorAll('.code-block-wrapper');
      const copyButtons = section.querySelectorAll('.copy-button');

      expect(codeBlockWrappers.length).toBeGreaterThan(0);
      expect(copyButtons.length).toBe(codeBlockWrappers.length);
    });

    it('should have copy button for installation command', () => {
      const installBlock = Array.from(section.querySelectorAll('.quickstart-code-block')).find(
        block => block.textContent?.includes('cargo install')
      );
      expect(installBlock).not.toBeNull();

      const copyButton = installBlock?.querySelector('.copy-button');
      expect(copyButton).not.toBeNull();
    });

    it('should have copy button for server startup command', () => {
      const startBlock = Array.from(section.querySelectorAll('.quickstart-code-block')).find(
        block => {
          const codeContent = block.querySelector('code')?.textContent;
          return codeContent === 'mirdb';
        }
      );
      expect(startBlock).not.toBeNull();

      const copyButton = startBlock?.querySelector('.copy-button');
      expect(copyButton).not.toBeNull();
    });

    it('should have copy button for example commands', () => {
      const exampleBlock = Array.from(section.querySelectorAll('.quickstart-code-block')).find(
        block => block.textContent?.includes('nc localhost')
      );
      expect(exampleBlock).not.toBeNull();

      const copyButton = exampleBlock?.querySelector('.copy-button');
      expect(copyButton).not.toBeNull();
    });

    it('copy buttons should have accessible aria-label', () => {
      const copyButtons = section.querySelectorAll('.copy-button');

      copyButtons.forEach(button => {
        expect(button.getAttribute('aria-label')).toBe('Copy code to clipboard');
      });
    });
  });

  describe('syntax highlighting (test case 8)', () => {
    it('should apply syntax highlighting classes to code', () => {
      const codeBlocks = section.querySelectorAll('pre code');

      codeBlocks.forEach(code => {
        expect(code.classList.contains('language-bash')).toBe(true);
      });
    });

    it('should have pre elements with language data attribute', () => {
      const preElements = section.querySelectorAll('pre');

      preElements.forEach(pre => {
        expect(pre.getAttribute('data-language')).toBe('bash');
      });
    });

    it('should highlight comments', () => {
      const commentSpans = section.querySelectorAll('.token.comment');
      expect(commentSpans.length).toBeGreaterThan(0);
    });

    it('should highlight keywords', () => {
      const keywordSpans = section.querySelectorAll('.token.keyword');
      expect(keywordSpans.length).toBeGreaterThan(0);
    });

    it('should highlight function/command names', () => {
      const functionSpans = section.querySelectorAll('.token.function');
      expect(functionSpans.length).toBeGreaterThan(0);
    });
  });

  describe('code labels', () => {
    it('should have labels for code blocks', () => {
      const labels = section.querySelectorAll('.code-label');
      expect(labels.length).toBeGreaterThan(0);
    });

    it('should have Installation label', () => {
      const labels = section.querySelectorAll('.code-label');
      const labelTexts = Array.from(labels).map(l => l.textContent);

      expect(labelTexts).toContain('Installation');
    });

    it('should have Start Server label', () => {
      const labels = section.querySelectorAll('.code-label');
      const labelTexts = Array.from(labels).map(l => l.textContent);

      expect(labelTexts).toContain('Start Server');
    });

    it('should have Example Commands label', () => {
      const labels = section.querySelectorAll('.code-label');
      const labelTexts = Array.from(labels).map(l => l.textContent);

      expect(labelTexts).toContain('Example Commands');
    });
  });

  describe('steps structure', () => {
    it('should have exactly 3 steps', () => {
      const steps = section.querySelectorAll('.quickstart-step');
      expect(steps.length).toBe(3);
    });

    it('should have numbered step titles', () => {
      const stepTitles = section.querySelectorAll('.quickstart-step h3');
      const titles = Array.from(stepTitles).map(t => t.textContent);

      expect(titles[0]).toContain('1.');
      expect(titles[1]).toContain('2.');
      expect(titles[2]).toContain('3.');
    });

    it('each step should have a description paragraph', () => {
      const steps = section.querySelectorAll('.quickstart-step');

      steps.forEach(step => {
        const description = step.querySelector('p');
        expect(description).not.toBeNull();
        expect(description?.textContent?.length).toBeGreaterThan(0);
      });
    });
  });

  describe('accessibility', () => {
    it('should use semantic heading structure', () => {
      const h2 = section.querySelector('h2');
      const h3s = section.querySelectorAll('h3');

      expect(h2).not.toBeNull();
      expect(h3s.length).toBe(3);
    });

    it('should have container div for proper layout', () => {
      const container = section.querySelector('.container');
      expect(container).not.toBeNull();
    });
  });
});
