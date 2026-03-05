/**
 * Simple build script for MirDB Homepage
 * Assembles HTML partials into the final static site
 */

const fs = require('fs');
const path = require('path');

const TEMPLATES_DIR = path.join(__dirname, 'templates');
const PARTIALS_DIR = path.join(TEMPLATES_DIR, 'partials');
const OUTPUT_DIR = path.join(__dirname, 'static');

function readFile(filePath) {
    return fs.readFileSync(filePath, 'utf-8');
}

function readPartial(name) {
    const partialPath = path.join(PARTIALS_DIR, `${name}.html`);
    if (fs.existsSync(partialPath)) {
        return readFile(partialPath);
    }
    return `<!-- ${name} partial not found -->`;
}

function build() {
    console.log('Building MirDB Homepage...');

    // Read base template
    const baseTemplate = readFile(path.join(TEMPLATES_DIR, 'base.html'));

    // Read index template
    const indexTemplate = readFile(path.join(TEMPLATES_DIR, 'index.html'));

    // Replace partial placeholders in index
    let content = indexTemplate
        .replace('{{HEADER}}', readPartial('header'))
        .replace('{{HERO}}', readPartial('hero'))
        .replace('{{FEATURES}}', readPartial('features'))
        .replace('{{QUICKSTART}}', readPartial('quickstart'))
        .replace('{{CONFIGURATION}}', readPartial('configuration'))
        .replace('{{STATUS}}', readPartial('status'))
        .replace('{{FOOTER}}', readPartial('footer'));

    // Insert content into base template
    const finalHtml = baseTemplate.replace('{{CONTENT}}', content);

    // Write output
    const outputPath = path.join(OUTPUT_DIR, 'index.html');
    fs.writeFileSync(outputPath, finalHtml);

    console.log(`Built: ${outputPath}`);
}

build();
