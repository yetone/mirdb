/**
 * Getting Started Component Unit Tests.
 * Owner: Scenario 5 - Getting Started Section
 *
 * Tests:
 * - Section renders with installation commands
 * - Code blocks are present and copyable
 * - Default configuration is displayed
 * - Documentation link is present
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  renderGettingStarted,
  INSTALL_COMMANDS,
  INSTALL_OPTIONS,
  DEFAULT_CONFIG,
  QUICK_START_CONFIG,
} from '../../../src/components/GettingStarted';

describe('GettingStarted Component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderGettingStarted();
  });

  describe('Section Rendering', () => {
    it('should render the getting started section', () => {
      expect(section).toBeTruthy();
      expect(section.tagName).toBe('SECTION');
      expect(section.id).toBe('getting-started');
      expect(section.getAttribute('data-testid')).toBe('getting-started-section');
    });

    it('should have a section title', () => {
      const title = section.querySelector('.getting-started-title');
      expect(title).toBeTruthy();
      expect(title?.textContent).toBe('Getting Started');
    });

    it('should have a section description', () => {
      const description = section.querySelector('.getting-started-description');
      expect(description).toBeTruthy();
      expect(description?.textContent).toContain('MirDB');
    });
  });

  describe('Installation Commands', () => {
    it('should display installation commands', () => {
      const installGrid = section.querySelector('[data-testid="install-grid"]');
      expect(installGrid).toBeTruthy();
    });

    it('should have cargo install option', () => {
      const cargoOption = section.querySelector('[data-testid="install-option-cargo"]');
      expect(cargoOption).toBeTruthy();

      const title = cargoOption?.querySelector('[data-testid="install-option-title"]');
      expect(title?.textContent).toBe('Using Cargo');

      const codeContent = cargoOption?.querySelector('[data-testid="code-content"]');
      expect(codeContent?.textContent).toContain('cargo install mirdb');
    });

    it('should have build from source option', () => {
      const sourceOption = section.querySelector('[data-testid="install-option-source"]');
      expect(sourceOption).toBeTruthy();

      const title = sourceOption?.querySelector('[data-testid="install-option-title"]');
      expect(title?.textContent).toBe('Build from Source');

      const codeContent = sourceOption?.querySelector('[data-testid="code-content"]');
      expect(codeContent?.textContent).toContain('git clone');
      expect(codeContent?.textContent).toContain('cargo build --release');
    });

    it('should have correct INSTALL_COMMANDS array', () => {
      expect(INSTALL_COMMANDS).toContain('cargo install mirdb');
      expect(INSTALL_COMMANDS).toContain('git clone https://github.com/pjzhong/mirdb.git');
      expect(INSTALL_COMMANDS).toContain('cargo build --release');
    });

    it('should have correct INSTALL_OPTIONS structure', () => {
      expect(INSTALL_OPTIONS).toHaveLength(2);
      expect(INSTALL_OPTIONS[0].id).toBe('cargo');
      expect(INSTALL_OPTIONS[1].id).toBe('source');
    });
  });

  describe('Code Blocks', () => {
    it('should have code blocks with proper structure', () => {
      const codeBlocks = section.querySelectorAll('[data-testid="code-block"]');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    it('should have code content elements', () => {
      const codeContents = section.querySelectorAll('[data-testid="code-content"]');
      expect(codeContents.length).toBeGreaterThan(0);
    });

    it('should have copy buttons for each code block', () => {
      const copyButtons = section.querySelectorAll('[data-testid="copy-button"]');
      expect(copyButtons.length).toBeGreaterThan(0);

      copyButtons.forEach((button) => {
        expect(button.textContent).toBe('Copy');
        expect(button.getAttribute('aria-label')).toBe('Copy to clipboard');
      });
    });

    it('should use monospace font for code blocks', () => {
      const codeContents = section.querySelectorAll('[data-testid="code-content"]');
      codeContents.forEach((content) => {
        expect(content.classList.contains('font-mono')).toBe(true);
      });
    });
  });

  describe('Default Configuration', () => {
    it('should display default configuration section', () => {
      const configSection = section.querySelector('[data-testid="default-config"]');
      expect(configSection).toBeTruthy();
    });

    it('should display default port 12333', () => {
      const portConfig = section.querySelector('[data-testid="config-port"]');
      expect(portConfig).toBeTruthy();
      expect(portConfig?.textContent).toContain('12333');
      expect(DEFAULT_CONFIG.port).toBe(12333);
    });

    it('should display default work directory /tmp/mirdb', () => {
      const workDirConfig = section.querySelector('[data-testid="config-workdir"]');
      expect(workDirConfig).toBeTruthy();
      expect(workDirConfig?.textContent).toContain('/tmp/mirdb');
      expect(DEFAULT_CONFIG.workDir).toBe('/tmp/mirdb');
    });

    it('should have config grid layout', () => {
      const configGrid = section.querySelector('[data-testid="config-grid"]');
      expect(configGrid).toBeTruthy();
      expect(configGrid?.classList.contains('grid')).toBe(true);
    });
  });

  describe('Quick Start', () => {
    it('should display quick start section', () => {
      const quickStart = section.querySelector('[data-testid="quick-start"]');
      expect(quickStart).toBeTruthy();
    });

    it('should have quick start code block', () => {
      const quickStartCodeBlock = section.querySelector('[data-testid="code-block-quick-start"]');
      expect(quickStartCodeBlock).toBeTruthy();
    });

    it('should contain mirdb command in quick start', () => {
      const quickStartCode = section.querySelector('[data-testid="code-block-quick-start"] [data-testid="code-content"]');
      expect(quickStartCode?.textContent).toContain('mirdb');
    });

    it('should show port and work directory in quick start config', () => {
      expect(QUICK_START_CONFIG).toContain('12333');
      expect(QUICK_START_CONFIG).toContain('/tmp/mirdb');
    });
  });

  describe('Documentation Link', () => {
    it('should display documentation link section', () => {
      const docLinkSection = section.querySelector('[data-testid="documentation-link"]');
      expect(docLinkSection).toBeTruthy();
    });

    it('should have documentation link with correct attributes', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink).toBeTruthy();
      expect(docLink.href).toBe('https://github.com/pjzhong/mirdb#readme');
      expect(docLink.target).toBe('_blank');
      expect(docLink.rel).toBe('noopener noreferrer');
    });

    it('should have correct link text', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]');
      expect(docLink?.textContent).toContain('View Full Documentation');
    });
  });

  describe('Accessibility', () => {
    it('should have proper section structure', () => {
      expect(section.tagName).toBe('SECTION');
      expect(section.id).toBe('getting-started');
    });

    it('should have heading hierarchy', () => {
      const h2 = section.querySelector('h2');
      const h3s = section.querySelectorAll('h3');
      expect(h2).toBeTruthy();
      expect(h3s.length).toBeGreaterThan(0);
    });

    it('should have aria-labels on copy buttons', () => {
      const copyButtons = section.querySelectorAll('[data-testid="copy-button"]');
      copyButtons.forEach((button) => {
        expect(button.getAttribute('aria-label')).toBe('Copy to clipboard');
      });
    });
  });
});
