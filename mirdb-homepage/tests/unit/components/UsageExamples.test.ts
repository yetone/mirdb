/**
 * Usage Examples Component Unit Tests.
 * Owner: Scenario 3 - Usage Examples with Syntax Highlighting
 *
 * Tests:
 * - Usage section renders with code blocks
 * - set/get/delete commands are displayed
 * - Syntax highlighting is applied
 * - Code blocks use monospace font
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderUsageExamples, CODE_EXAMPLES } from '@/components/UsageExamples';

describe('UsageExamples Component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderUsageExamples();
  });

  describe('Section rendering', () => {
    it('renders usage examples section with correct id', () => {
      expect(section.id).toBe('usage');
    });

    it('renders with usage-section class', () => {
      expect(section.classList.contains('usage-section')).toBe(true);
    });

    it('renders section with code block container', () => {
      const container = section.querySelector('.usage-examples-container');
      expect(container).not.toBeNull();
    });

    it('renders title with correct text', () => {
      const title = section.querySelector('.usage-title');
      expect(title).not.toBeNull();
      expect(title?.textContent).toBe('Usage Examples');
    });

    it('renders intro paragraph', () => {
      const intro = section.querySelector('.usage-intro');
      expect(intro).not.toBeNull();
      expect(intro?.textContent).toContain('memcached');
    });
  });

  describe('Set command example', () => {
    it('displays set command example', () => {
      const codeBlocks = section.querySelectorAll('.code-block');
      const allCode = Array.from(codeBlocks).map(b => b.textContent).join(' ');
      expect(allCode).toContain('set');
      expect(allCode).toContain('key');
    });

    it('shows set command with correct format', () => {
      // Find the set command example card
      const cards = section.querySelectorAll('.usage-example-card');
      const setCard = Array.from(cards).find(card =>
        card.querySelector('.code-content')?.textContent?.includes('set key')
      );
      expect(setCard).not.toBeNull();

      const codeContent = setCard?.querySelector('.code-content')?.textContent;
      expect(codeContent).toContain('set key 0 0 5');
      expect(codeContent).toContain('value');
    });

    it('shows set command with syntax highlighting', () => {
      const commandSpans = section.querySelectorAll('.syntax-command');
      const setCommand = Array.from(commandSpans).find(span =>
        span.textContent === 'set'
      );
      expect(setCommand).not.toBeNull();
    });
  });

  describe('Get command example', () => {
    it('displays get command example', () => {
      const codeBlocks = section.querySelectorAll('.code-block');
      const allCode = Array.from(codeBlocks).map(b => b.textContent).join(' ');
      expect(allCode).toContain('get');
    });

    it('shows get command and expected response', () => {
      const cards = section.querySelectorAll('.usage-example-card');
      const getCard = Array.from(cards).find(card => {
        const description = card.querySelector('.usage-example-description')?.textContent;
        return description?.toLowerCase().includes('retrieve');
      });
      expect(getCard).not.toBeNull();

      // Check command
      const commandBlock = getCard?.querySelector('.usage-command-block .code-content');
      expect(commandBlock?.textContent).toContain('get key');

      // Check response
      const responseBlock = getCard?.querySelector('.usage-response-block .code-content');
      expect(responseBlock?.textContent).toContain('VALUE');
      expect(responseBlock?.textContent).toContain('END');
    });
  });

  describe('Delete command example', () => {
    it('displays delete command example', () => {
      const codeBlocks = section.querySelectorAll('.code-block');
      const allCode = Array.from(codeBlocks).map(b => b.textContent).join(' ');
      expect(allCode).toContain('delete');
    });

    it('shows delete key command', () => {
      const cards = section.querySelectorAll('.usage-example-card');
      const deleteCard = Array.from(cards).find(card => {
        const description = card.querySelector('.usage-example-description')?.textContent;
        return description?.toLowerCase().includes('remove');
      });
      expect(deleteCard).not.toBeNull();

      const commandBlock = deleteCard?.querySelector('.usage-command-block .code-content');
      expect(commandBlock?.textContent).toContain('delete key');
    });
  });

  describe('Code examples data', () => {
    it('exports CODE_EXAMPLES array', () => {
      expect(Array.isArray(CODE_EXAMPLES)).toBe(true);
    });

    it('contains three examples', () => {
      expect(CODE_EXAMPLES.length).toBe(3);
    });

    it('each example has command and description', () => {
      CODE_EXAMPLES.forEach(example => {
        expect(example.command).toBeDefined();
        expect(example.description).toBeDefined();
        expect(typeof example.command).toBe('string');
        expect(typeof example.description).toBe('string');
      });
    });
  });

  describe('Code block structure', () => {
    it('renders pre elements with code-block class', () => {
      const codeBlocks = section.querySelectorAll('pre.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    it('renders code elements with code-content class', () => {
      const codeContents = section.querySelectorAll('code.code-content');
      expect(codeContents.length).toBeGreaterThan(0);
    });

    it('code blocks have memcached language attribute', () => {
      const codeBlocks = section.querySelectorAll('.code-block');
      codeBlocks.forEach(block => {
        expect(block.getAttribute('data-language')).toBe('memcached');
      });
    });
  });

  describe('Accessibility', () => {
    it('section has aria-labelledby attribute', () => {
      expect(section.getAttribute('aria-labelledby')).toBe('usage-title');
    });

    it('title has matching id', () => {
      const title = section.querySelector('#usage-title');
      expect(title).not.toBeNull();
    });
  });
});
