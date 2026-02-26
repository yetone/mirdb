/**
 * Getting Started Section Unit Tests
 * Owner: Scenario 4 - Getting Started Section
 *
 * Tests for:
 * - Getting Started section visibility and accessibility
 * - Installation instructions presence
 * - Setup steps documentation
 * - Quick start commands in code blocks
 * - Documentation links
 * - Step ordering
 */

const fs = require('fs');
const path = require('path');

describe('Getting Started Section', () => {
    let document;
    let htmlContent;

    beforeAll(() => {
        // Read the HTML file
        const htmlPath = path.join(__dirname, '../../index.html');
        htmlContent = fs.readFileSync(htmlPath, 'utf8');
    });

    beforeEach(() => {
        // Set up the DOM for each test
        document = new DOMParser().parseFromString(htmlContent, 'text/html');
    });

    describe('Test Case 1: Section Visibility and Accessibility', () => {
        test('getting started section exists with correct id', () => {
            const section = document.getElementById('getting-started');
            expect(section).not.toBeNull();
            expect(section.tagName.toLowerCase()).toBe('section');
        });

        test('getting started section has proper class for styling', () => {
            const section = document.getElementById('getting-started');
            expect(section.classList.contains('getting-started-section')).toBe(true);
        });

        test('getting started section has a heading', () => {
            const section = document.getElementById('getting-started');
            const heading = section.querySelector('h2');
            expect(heading).not.toBeNull();
            expect(heading.textContent.toLowerCase()).toContain('getting started');
        });

        test('getting started section is accessible via anchor link from hero', () => {
            const heroLink = document.querySelector('a[href="#getting-started"]');
            expect(heroLink).not.toBeNull();
        });

        test('getting started section has aria-labelledby attribute', () => {
            const section = document.getElementById('getting-started');
            expect(section.getAttribute('aria-labelledby')).toBe('getting-started-title');
        });
    });

    describe('Test Case 2: Installation Instructions', () => {
        test('installation step exists', () => {
            const installStep = document.querySelector('[data-step="2"]');
            expect(installStep).not.toBeNull();
        });

        test('installation step has title mentioning installation', () => {
            const installStep = document.querySelector('[data-step="2"]');
            const title = installStep.querySelector('.step-title');
            expect(title).not.toBeNull();
            expect(title.textContent.toLowerCase()).toContain('install');
        });

        test('installation step contains git clone command', () => {
            const installStep = document.querySelector('[data-step="2"]');
            const codeBlock = installStep.querySelector('.code-block');
            expect(codeBlock).not.toBeNull();
            expect(codeBlock.textContent).toContain('git clone');
        });

        test('installation step contains cargo build command', () => {
            const installStep = document.querySelector('[data-step="2"]');
            const codeBlock = installStep.querySelector('.code-block');
            expect(codeBlock).not.toBeNull();
            expect(codeBlock.textContent).toContain('cargo build');
        });

        test('installation instructions mention release mode', () => {
            const installStep = document.querySelector('[data-step="2"]');
            const content = installStep.textContent;
            expect(content).toContain('--release');
        });
    });

    describe('Test Case 3: Setup Steps', () => {
        test('prerequisites step exists', () => {
            const prerequisitesStep = document.querySelector('[data-step="1"]');
            expect(prerequisitesStep).not.toBeNull();
        });

        test('prerequisites step lists Rust requirement', () => {
            const prerequisitesStep = document.querySelector('[data-step="1"]');
            const content = prerequisitesStep.textContent.toLowerCase();
            expect(content).toContain('rust');
        });

        test('prerequisites step lists Cargo requirement', () => {
            const prerequisitesStep = document.querySelector('[data-step="1"]');
            const content = prerequisitesStep.textContent.toLowerCase();
            expect(content).toContain('cargo');
        });

        test('configuration step exists', () => {
            const configStep = document.querySelector('[data-step="3"]');
            expect(configStep).not.toBeNull();
        });

        test('configuration step has title mentioning configuration', () => {
            const configStep = document.querySelector('[data-step="3"]');
            const title = configStep.querySelector('.step-title');
            expect(title).not.toBeNull();
            expect(title.textContent.toLowerCase()).toContain('config');
        });

        test('configuration step mentions default port', () => {
            const configStep = document.querySelector('[data-step="3"]');
            const content = configStep.textContent;
            expect(content).toContain('12333');
        });

        test('configuration step mentions data directory', () => {
            const configStep = document.querySelector('[data-step="3"]');
            const content = configStep.textContent;
            expect(content).toContain('/tmp/mirdb');
        });
    });

    describe('Test Case 4: Quick Start Commands', () => {
        test('quick start step exists', () => {
            const quickStartStep = document.querySelector('[data-step="4"]');
            expect(quickStartStep).not.toBeNull();
        });

        test('quick start step has title mentioning quick start', () => {
            const quickStartStep = document.querySelector('[data-step="4"]');
            const title = quickStartStep.querySelector('.step-title');
            expect(title).not.toBeNull();
            expect(title.textContent.toLowerCase()).toContain('quick start');
        });

        test('quick start step contains code block', () => {
            const quickStartStep = document.querySelector('[data-step="4"]');
            const codeBlock = quickStartStep.querySelector('.code-block');
            expect(codeBlock).not.toBeNull();
        });

        test('quick start step shows how to run the server', () => {
            const quickStartStep = document.querySelector('[data-step="4"]');
            const codeBlock = quickStartStep.querySelector('.code-block');
            expect(codeBlock.textContent).toContain('mirdb');
        });

        test('quick start step shows example memcached commands', () => {
            const quickStartStep = document.querySelector('[data-step="4"]');
            const codeBlock = quickStartStep.querySelector('.code-block');
            expect(codeBlock.textContent).toContain('set');
            expect(codeBlock.textContent).toContain('get');
        });

        test('code blocks use pre and code elements', () => {
            const codeBlocks = document.querySelectorAll('.getting-started-section .code-block');
            codeBlocks.forEach(block => {
                expect(block.querySelector('pre')).not.toBeNull();
                expect(block.querySelector('code')).not.toBeNull();
            });
        });
    });

    describe('Test Case 5: Documentation Links', () => {
        test('documentation links section exists', () => {
            const linksSection = document.querySelector('.getting-started-links');
            expect(linksSection).not.toBeNull();
        });

        test('documentation links section has a title', () => {
            const linksSection = document.querySelector('.getting-started-links');
            const title = linksSection.querySelector('.links-title');
            expect(title).not.toBeNull();
        });

        test('github documentation link exists', () => {
            const githubLink = document.querySelector('[data-resource="github"]');
            expect(githubLink).not.toBeNull();
            expect(githubLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
        });

        test('documentation links have doc-link class', () => {
            const docLinks = document.querySelectorAll('.getting-started-section .doc-link');
            expect(docLinks.length).toBeGreaterThan(0);
        });

        test('documentation links open in new tab', () => {
            const docLinks = document.querySelectorAll('.getting-started-section .resource-link');
            docLinks.forEach(link => {
                expect(link.getAttribute('target')).toBe('_blank');
                expect(link.getAttribute('rel')).toContain('noopener');
            });
        });

        test('memcached protocol link exists', () => {
            const memcachedLink = document.querySelector('[data-resource="memcached"]');
            expect(memcachedLink).not.toBeNull();
        });

        test('community/issues link exists', () => {
            const issuesLink = document.querySelector('[data-resource="issues"]');
            expect(issuesLink).not.toBeNull();
            expect(issuesLink.getAttribute('href')).toContain('issues');
        });
    });

    describe('Test Case 6: Step Ordering', () => {
        test('steps are numbered 1 through 4', () => {
            const steps = document.querySelectorAll('.getting-started-step');
            expect(steps.length).toBe(4);

            steps.forEach((step, index) => {
                expect(step.getAttribute('data-step')).toBe(String(index + 1));
            });
        });

        test('each step has a visible step number', () => {
            const stepNumbers = document.querySelectorAll('.step-number');
            expect(stepNumbers.length).toBe(4);

            stepNumbers.forEach((num, index) => {
                expect(num.textContent.trim()).toBe(String(index + 1));
            });
        });

        test('steps are in logical order: prerequisites, installation, configuration, quick start', () => {
            const steps = document.querySelectorAll('.getting-started-step');
            const stepTitles = Array.from(steps).map(step =>
                step.querySelector('.step-title').textContent.toLowerCase()
            );

            expect(stepTitles[0]).toContain('prerequisite');
            expect(stepTitles[1]).toContain('install');
            expect(stepTitles[2]).toContain('config');
            expect(stepTitles[3]).toContain('quick start');
        });

        test('each step has a step-content container', () => {
            const steps = document.querySelectorAll('.getting-started-step');
            steps.forEach(step => {
                expect(step.querySelector('.step-content')).not.toBeNull();
            });
        });

        test('each step has a title and description', () => {
            const steps = document.querySelectorAll('.getting-started-step');
            steps.forEach(step => {
                expect(step.querySelector('.step-title')).not.toBeNull();
                expect(step.querySelector('.step-description')).not.toBeNull();
            });
        });
    });

    describe('Integration: Section Structure', () => {
        test('getting started section contains all required elements', () => {
            const section = document.getElementById('getting-started');

            // Has heading
            expect(section.querySelector('h2')).not.toBeNull();

            // Has steps
            expect(section.querySelectorAll('.getting-started-step').length).toBe(4);

            // Has code blocks
            expect(section.querySelectorAll('.code-block').length).toBeGreaterThanOrEqual(2);

            // Has documentation links
            expect(section.querySelector('.getting-started-links')).not.toBeNull();
        });

        test('getting started section is within main element', () => {
            const main = document.querySelector('main');
            const section = document.getElementById('getting-started');
            expect(main.contains(section)).toBe(true);
        });
    });
});
