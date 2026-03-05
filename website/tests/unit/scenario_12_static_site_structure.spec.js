/**
 * Scenario 12: Static Site Structure Tests
 *
 * Tests verify:
 * 1. Site generator config file exists and is valid
 * 2. Content markdown files are properly formatted with frontmatter
 * 3. CSS/Tailwind configuration is properly configured
 * 4. Site can be deployed to static hosting
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

const WEBSITE_DIR = path.join(__dirname, '../..');

test.describe('Scenario 12: Static Site Structure', () => {

    test.describe('Test Case 1: Site Generator Config', () => {
        test('config.toml exists', () => {
            const configPath = path.join(WEBSITE_DIR, 'config.toml');
            expect(fs.existsSync(configPath)).toBe(true);
        });

        test('config.toml contains required fields', () => {
            const configPath = path.join(WEBSITE_DIR, 'config.toml');
            const content = fs.readFileSync(configPath, 'utf-8');

            // Check for base_url
            expect(content).toContain('base_url');
            // Check for title
            expect(content).toContain('title');
            // Check for description
            expect(content).toContain('description');
        });

        test('config.toml has valid base_url', () => {
            const configPath = path.join(WEBSITE_DIR, 'config.toml');
            const content = fs.readFileSync(configPath, 'utf-8');

            // Should have a URL format for base_url
            const baseUrlMatch = content.match(/base_url\s*=\s*["']([^"']+)["']/);
            expect(baseUrlMatch).toBeTruthy();
            expect(baseUrlMatch[1]).toMatch(/^https?:\/\//);
        });

        test('config.toml has MirDB title', () => {
            const configPath = path.join(WEBSITE_DIR, 'config.toml');
            const content = fs.readFileSync(configPath, 'utf-8');

            // Title should mention MirDB
            expect(content.toLowerCase()).toContain('mirdb');
        });
    });

    test.describe('Test Case 2: Content Markdown Files', () => {
        test('_index.md exists in content directory', () => {
            const contentPath = path.join(WEBSITE_DIR, 'content/_index.md');
            expect(fs.existsSync(contentPath)).toBe(true);
        });

        test('_index.md has valid TOML frontmatter', () => {
            const contentPath = path.join(WEBSITE_DIR, 'content/_index.md');
            const content = fs.readFileSync(contentPath, 'utf-8');

            // Check for TOML frontmatter delimiters (+++)
            expect(content).toMatch(/^\+\+\+/);
            expect(content.match(/\+\+\+/g).length).toBeGreaterThanOrEqual(2);
        });

        test('_index.md frontmatter contains title', () => {
            const contentPath = path.join(WEBSITE_DIR, 'content/_index.md');
            const content = fs.readFileSync(contentPath, 'utf-8');

            // Extract frontmatter
            const frontmatterMatch = content.match(/\+\+\+([\s\S]*?)\+\+\+/);
            expect(frontmatterMatch).toBeTruthy();
            expect(frontmatterMatch[1]).toContain('title');
        });

        test('_index.md frontmatter contains description', () => {
            const contentPath = path.join(WEBSITE_DIR, 'content/_index.md');
            const content = fs.readFileSync(contentPath, 'utf-8');

            // Extract frontmatter
            const frontmatterMatch = content.match(/\+\+\+([\s\S]*?)\+\+\+/);
            expect(frontmatterMatch).toBeTruthy();
            expect(frontmatterMatch[1]).toContain('description');
        });
    });

    test.describe('Test Case 3: CSS/Tailwind Configuration', () => {
        test('tailwind.config.js exists', () => {
            const configPath = path.join(WEBSITE_DIR, 'tailwind.config.js');
            expect(fs.existsSync(configPath)).toBe(true);
        });

        test('tailwind.config.js has content paths', () => {
            const configPath = path.join(WEBSITE_DIR, 'tailwind.config.js');
            const content = fs.readFileSync(configPath, 'utf-8');

            // Should have content configuration for template scanning
            expect(content).toContain('content');
            expect(content).toContain('templates');
        });

        test('tailwind.config.js has dark mode configured', () => {
            const configPath = path.join(WEBSITE_DIR, 'tailwind.config.js');
            const content = fs.readFileSync(configPath, 'utf-8');

            // Should have darkMode set to 'class' for toggle support
            expect(content).toContain('darkMode');
            expect(content).toContain("'class'");
        });

        test('tailwind.config.js has monospace font configured', () => {
            const configPath = path.join(WEBSITE_DIR, 'tailwind.config.js');
            const content = fs.readFileSync(configPath, 'utf-8');

            // Should have monospace font family for code blocks
            expect(content).toContain('fontFamily');
            expect(content).toContain('mono');
        });

        test('sass/main.scss exists with Tailwind directives', () => {
            const sassPath = path.join(WEBSITE_DIR, 'sass/main.scss');
            expect(fs.existsSync(sassPath)).toBe(true);

            const content = fs.readFileSync(sassPath, 'utf-8');
            expect(content).toContain('@tailwind base');
            expect(content).toContain('@tailwind components');
            expect(content).toContain('@tailwind utilities');
        });

        test('package.json exists with project metadata', () => {
            const packagePath = path.join(WEBSITE_DIR, 'package.json');
            expect(fs.existsSync(packagePath)).toBe(true);

            const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf-8'));
            expect(packageJson.name).toBe('mirdb-homepage');
        });
    });

    test.describe('Test Case 4: Deployability Structure', () => {
        test('static/index.html exists for deployment', () => {
            const indexPath = path.join(WEBSITE_DIR, 'static/index.html');
            expect(fs.existsSync(indexPath)).toBe(true);
        });

        test('static/index.html is valid HTML5', () => {
            const indexPath = path.join(WEBSITE_DIR, 'static/index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Check for HTML5 doctype
            expect(content).toContain('<!DOCTYPE html>');
            // Check for lang attribute
            expect(content).toMatch(/<html[^>]*lang=/);
            // Check for viewport meta
            expect(content).toContain('viewport');
            // Check for charset
            expect(content).toContain('charset');
        });

        test('static/css directory exists', () => {
            const cssDir = path.join(WEBSITE_DIR, 'static/css');
            expect(fs.existsSync(cssDir)).toBe(true);
        });

        test('static/images directory has required assets', () => {
            const imagesDir = path.join(WEBSITE_DIR, 'static/images');
            expect(fs.existsSync(imagesDir)).toBe(true);

            // Check for logo
            const logoPath = path.join(imagesDir, 'logo.gif');
            expect(fs.existsSync(logoPath)).toBe(true);
        });

        test('templates directory has base.html', () => {
            const basePath = path.join(WEBSITE_DIR, 'templates/base.html');
            expect(fs.existsSync(basePath)).toBe(true);
        });

        test('templates directory has index.html', () => {
            const indexPath = path.join(WEBSITE_DIR, 'templates/index.html');
            expect(fs.existsSync(indexPath)).toBe(true);
        });

        test('templates/partials directory exists with all partials', () => {
            const partialsDir = path.join(WEBSITE_DIR, 'templates/partials');
            expect(fs.existsSync(partialsDir)).toBe(true);

            const requiredPartials = [
                'header.html',
                'hero.html',
                'features.html',
                'quickstart.html',
                'configuration.html',
                'status.html',
                'footer.html'
            ];

            for (const partial of requiredPartials) {
                const partialPath = path.join(partialsDir, partial);
                expect(fs.existsSync(partialPath), `Missing partial: ${partial}`).toBe(true);
            }
        });

        test('index.html includes all partial templates', () => {
            const indexPath = path.join(WEBSITE_DIR, 'templates/index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Check that partials are included
            expect(content).toContain('header.html');
            expect(content).toContain('hero.html');
            expect(content).toContain('features.html');
            expect(content).toContain('quickstart.html');
            expect(content).toContain('configuration.html');
            expect(content).toContain('status.html');
            expect(content).toContain('footer.html');
        });
    });
});
