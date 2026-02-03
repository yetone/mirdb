/**
 * Unit tests for Installation component.
 * Owner: Scenario 5 - Installation Section
 *
 * Tests:
 * - Installation content data validation
 * - Component structure verification
 * - Tabbed interface for platforms (Cargo, Docker, Source)
 * - Quick start guide presence
 * - Accessibility attributes
 */

import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';

// Read the component and content files
const installationContent = JSON.parse(
  readFileSync(join(__dirname, '../../../src/content/installation.json'), 'utf-8')
);

const installationComponent = readFileSync(
  join(__dirname, '../../../src/components/Installation.astro'),
  'utf-8'
);

describe('Installation Section', () => {
  describe('Installation Content Data', () => {
    it('should have cargo installation with correct command', () => {
      expect(installationContent.cargo).toBeDefined();
      expect(installationContent.cargo.command).toBe('cargo install mirdb');
      expect(installationContent.cargo.id).toBe('cargo');
      expect(installationContent.cargo.label).toBe('Cargo');
      expect(installationContent.cargo.description).toBeTruthy();
    });

    it('should have docker installation with correct command', () => {
      expect(installationContent.docker).toBeDefined();
      expect(installationContent.docker.command).toBe('docker run -p 9000:9000 yetone/mirdb');
      expect(installationContent.docker.id).toBe('docker');
      expect(installationContent.docker.label).toBe('Docker');
      expect(installationContent.docker.description).toBeTruthy();
    });

    it('should have source installation with git clone and cargo build steps', () => {
      expect(installationContent.source).toBeDefined();
      expect(installationContent.source.steps).toBeInstanceOf(Array);
      expect(installationContent.source.steps.length).toBeGreaterThanOrEqual(3);

      // Verify git clone step
      const gitCloneStep = installationContent.source.steps.find(
        (step: { command: string }) => step.command.includes('git clone')
      );
      expect(gitCloneStep).toBeDefined();
      expect(gitCloneStep.command).toContain('https://github.com/yetone/mirdb');

      // Verify cargo build step
      const cargoBuildStep = installationContent.source.steps.find(
        (step: { command: string }) => step.command.includes('cargo build --release')
      );
      expect(cargoBuildStep).toBeDefined();
    });

    it('should have quick start guide with telnet command', () => {
      expect(installationContent.quickStart).toBeDefined();
      expect(installationContent.quickStart.title).toBe('Quick Start');
      expect(installationContent.quickStart.command).toContain('telnet');
      expect(installationContent.quickStart.description).toBeTruthy();
    });

    it('each platform should have required properties', () => {
      const platforms = ['cargo', 'docker'];
      platforms.forEach((platform) => {
        const data = installationContent[platform];
        expect(data).toHaveProperty('id');
        expect(data).toHaveProperty('label');
        expect(data).toHaveProperty('command');
        expect(data).toHaveProperty('description');
        expect(typeof data.command).toBe('string');
        expect(data.command.length).toBeGreaterThan(0);
      });
    });

    it('source build should have steps array with command and description', () => {
      const source = installationContent.source;
      expect(source.steps).toBeInstanceOf(Array);
      source.steps.forEach((step: { command: string; description: string }) => {
        expect(step).toHaveProperty('command');
        expect(step).toHaveProperty('description');
        expect(typeof step.command).toBe('string');
        expect(typeof step.description).toBe('string');
      });
    });
  });

  describe('Installation Component Structure', () => {
    it('should have a section with id="installation"', () => {
      expect(installationComponent).toContain('id="installation"');
    });

    it('should have section heading', () => {
      expect(installationComponent).toContain('<h2');
      expect(installationComponent).toContain('Installation');
      expect(installationComponent).toContain('id="installation-heading"');
    });

    it('should import and use CodeBlock component', () => {
      expect(installationComponent).toContain("import CodeBlock from './CodeBlock.astro'");
      expect(installationComponent).toContain('<CodeBlock');
    });

    it('should import installation.json content', () => {
      expect(installationComponent).toContain("import installation from '../content/installation.json'");
    });

    it('should have tabbed interface with role="tablist"', () => {
      expect(installationComponent).toContain('role="tablist"');
      expect(installationComponent).toContain('aria-label="Installation methods"');
    });

    it('should have tab buttons with proper ARIA attributes', () => {
      expect(installationComponent).toContain('role="tab"');
      expect(installationComponent).toContain('aria-selected');
      expect(installationComponent).toContain('aria-controls');
    });

    it('should have tab panels with role="tabpanel"', () => {
      expect(installationComponent).toContain('role="tabpanel"');
      expect(installationComponent).toContain('aria-labelledby');
    });

    it('should have quick start section', () => {
      expect(installationComponent).toContain('quickStart.title');
      expect(installationComponent).toContain('quickStart.description');
      expect(installationComponent).toContain('quickStart.command');
    });

    it('should have link to full documentation', () => {
      expect(installationComponent).toContain('https://github.com/yetone/mirdb');
      expect(installationComponent).toContain('View Full Documentation');
      expect(installationComponent).toContain('rel="noopener noreferrer"');
    });
  });

  describe('Tab Functionality', () => {
    it('should have tab switching script', () => {
      expect(installationComponent).toContain('function initInstallTabs()');
      expect(installationComponent).toContain('function switchTab(targetTabId');
    });

    it('should support keyboard navigation', () => {
      expect(installationComponent).toContain('ArrowLeft');
      expect(installationComponent).toContain('ArrowRight');
      expect(installationComponent).toContain('Home');
      expect(installationComponent).toContain('End');
    });

    it('should re-initialize on Astro page transitions', () => {
      expect(installationComponent).toContain("document.addEventListener('astro:page-load'");
    });

    it('should use data attributes for tab identification', () => {
      expect(installationComponent).toContain('data-install-tab');
      expect(installationComponent).toContain('data-install-panel');
    });
  });

  describe('Accessibility', () => {
    it('should have aria-labelledby linking section heading', () => {
      expect(installationComponent).toContain('aria-labelledby="installation-heading"');
    });

    it('should have tabindex on tab panels', () => {
      expect(installationComponent).toContain('tabindex="0"');
    });

    it('should have focus styles configured', () => {
      expect(installationComponent).toContain('focus:outline-none');
      expect(installationComponent).toContain('focus:ring-2');
    });
  });

  describe('Copy Functionality Integration', () => {
    it('should have unique IDs for each code block using template literals', () => {
      // Component uses template literals for dynamic IDs
      expect(installationComponent).toContain('id={`install-code-${platform.id}`}');
      expect(installationComponent).toContain('id={`install-code-source-${stepIndex}`}');
      expect(installationComponent).toContain('id="install-code-quickstart"');
    });

    it('should pass id prop to CodeBlock for copy targeting', () => {
      expect(installationComponent).toContain('id={`install-code-${platform.id}`}');
    });
  });
});

describe('Installation Commands Validation', () => {
  it('cargo command should be valid Cargo install format', () => {
    const cargoCommand = installationContent.cargo.command;
    expect(cargoCommand).toMatch(/^cargo install \w+$/);
  });

  it('docker command should have correct port mapping', () => {
    const dockerCommand = installationContent.docker.command;
    expect(dockerCommand).toMatch(/docker run -p \d+:\d+ [\w\/]+/);
    expect(dockerCommand).toContain('9000:9000');
  });

  it('source build should include release flag', () => {
    const releaseStep = installationContent.source.steps.find(
      (step: { command: string }) => step.command.includes('cargo build')
    );
    expect(releaseStep.command).toContain('--release');
  });

  it('quick start command should connect to correct port', () => {
    const quickStartCommand = installationContent.quickStart.command;
    expect(quickStartCommand).toContain('localhost');
    expect(quickStartCommand).toContain('9000');
  });
});
