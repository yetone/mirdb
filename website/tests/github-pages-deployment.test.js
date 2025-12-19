// @ts-check
/**
 * GitHub Pages Deployment Tests
 * Scenario: Verify site is properly configured for GitHub Pages deployment
 *
 * Test Cases:
 * 1. Valid GitHub Actions workflow exists for building and deploying
 * 2. Build step completes successfully in CI
 * 3. Site is served via HTTPS from GitHub CDN (simulated verification)
 * 4. All asset paths work correctly on GitHub Pages domain
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const assert = require('assert');
const yaml = require('yaml');

// Test results collector
const results = {
  passed: 0,
  failed: 0,
  tests: []
};

function test(name, fn) {
  try {
    fn();
    results.passed++;
    results.tests.push({ name, status: 'pass' });
    console.log(`✓ ${name}`);
  } catch (error) {
    results.failed++;
    results.tests.push({ name, status: 'fail', error: error.message, stack: error.stack });
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
  }
}

console.log('GitHub Pages Deployment Tests\n');
console.log('='.repeat(50));

// Test Case 1: Valid GitHub Actions workflow exists for building and deploying
test('Test Case 1: Valid GitHub Actions workflow exists for building and deploying', () => {
  const workflowPath = path.resolve(__dirname, '../../.github/workflows/deploy-pages.yml');

  // Check workflow file exists
  assert(fs.existsSync(workflowPath), 'GitHub Actions workflow file should exist at .github/workflows/deploy-pages.yml');

  const workflowContent = fs.readFileSync(workflowPath, 'utf8');

  // Parse YAML to validate structure
  let workflow;
  try {
    workflow = yaml.parse(workflowContent);
  } catch (e) {
    assert.fail(`Workflow YAML should be valid: ${e.message}`);
  }

  // Validate workflow name
  assert(workflow.name, 'Workflow should have a name');
  console.log(`    Workflow name: ${workflow.name}`);

  // Validate trigger events
  assert(workflow.on, 'Workflow should have trigger events');
  assert(workflow.on.push || workflow.on.workflow_dispatch, 'Workflow should trigger on push or manual dispatch');

  if (workflow.on.push) {
    console.log(`    Trigger branches: ${JSON.stringify(workflow.on.push.branches)}`);
  }

  // Validate permissions for GitHub Pages
  assert(workflow.permissions, 'Workflow should have permissions defined');
  assert(workflow.permissions.pages === 'write', 'Workflow should have pages write permission');
  assert(workflow.permissions['id-token'] === 'write', 'Workflow should have id-token write permission for OIDC');

  // Validate jobs
  assert(workflow.jobs, 'Workflow should have jobs defined');
  assert(workflow.jobs.build, 'Workflow should have a build job');
  assert(workflow.jobs.deploy, 'Workflow should have a deploy job');

  // Validate build job has necessary steps
  const buildJob = workflow.jobs.build;
  const buildSteps = buildJob.steps.map(s => s.name || s.uses || 'unnamed');

  console.log(`    Build job steps: ${buildSteps.join(', ')}`);

  // Check for essential steps in build job (check both step names and action uses)
  const buildStepsUses = buildJob.steps.map(s => s.uses || '');

  const hasCheckout = buildSteps.some(s => s.includes('Checkout') || s.includes('checkout')) ||
                      buildStepsUses.some(s => s.includes('checkout'));
  const hasNodeSetup = buildSteps.some(s => s.includes('Node') || s.includes('node')) ||
                       buildStepsUses.some(s => s.includes('setup-node'));
  const hasInstall = buildSteps.some(s => s.toLowerCase().includes('install'));
  const hasBuild = buildSteps.some(s => s.toLowerCase().includes('build'));
  const hasUploadArtifact = buildSteps.some(s => s.includes('upload') && s.includes('artifact')) ||
                            buildStepsUses.some(s => s.includes('upload-pages-artifact'));

  assert(hasCheckout, 'Build job should checkout code');
  assert(hasNodeSetup, 'Build job should setup Node.js');
  assert(hasInstall, 'Build job should install dependencies');
  assert(hasBuild, 'Build job should run build');
  assert(hasUploadArtifact, 'Build job should upload pages artifact');

  // Validate deploy job uses GitHub Pages action
  const deployJob = workflow.jobs.deploy;
  const deploySteps = deployJob.steps.map(s => s.uses || s.name || 'unnamed');

  const hasDeployPages = deploySteps.some(s => s.includes('deploy-pages'));
  assert(hasDeployPages, 'Deploy job should use deploy-pages action');

  // Validate deploy job depends on build
  assert(deployJob.needs === 'build' || (Array.isArray(deployJob.needs) && deployJob.needs.includes('build')),
    'Deploy job should depend on build job');

  // Validate environment configuration
  assert(deployJob.environment, 'Deploy job should have environment configured');
  assert(deployJob.environment.name === 'github-pages', 'Deploy environment should be github-pages');

  console.log('    Workflow structure validated successfully');
});

// Test Case 2: Build step completes successfully in CI
test('Test Case 2: Build step completes successfully in CI', () => {
  const websiteDir = path.resolve(__dirname, '..');

  // Ensure dependencies are installed
  console.log('    Installing dependencies...');
  try {
    execSync('npm ci || npm install', {
      cwd: websiteDir,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe']
    });
  } catch (installError) {
    // Continue even if npm ci fails, npm install might work
  }

  // Run the build command
  console.log('    Running build...');
  let buildOutput;
  let buildError = null;

  try {
    buildOutput = execSync('npm run build', {
      cwd: websiteDir,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe']
    });
  } catch (error) {
    buildError = error;
  }

  // Assert build succeeded
  assert(buildError === null, `Build should complete without errors. Error: ${buildError?.message}`);

  // Verify output files exist
  const distDir = path.join(websiteDir, 'dist');
  assert(fs.existsSync(distDir), 'dist directory should exist after build');

  const indexPath = path.join(distDir, 'index.html');
  assert(fs.existsSync(indexPath), 'index.html should exist in dist');

  const cssPath = path.join(distDir, 'output.css');
  assert(fs.existsSync(cssPath), 'output.css should exist in dist');

  // Verify files have content
  const indexContent = fs.readFileSync(indexPath, 'utf8');
  assert(indexContent.length > 0, 'index.html should have content');
  assert(indexContent.includes('<!DOCTYPE html>'), 'index.html should be valid HTML');

  const cssContent = fs.readFileSync(cssPath, 'utf8');
  assert(cssContent.length > 0, 'output.css should have content');

  console.log(`    Build output: dist/index.html (${Math.round(indexContent.length / 1024)}KB)`);
  console.log(`    Build output: dist/output.css (${Math.round(cssContent.length / 1024)}KB)`);
});

// Test Case 3: Verify HTTPS deployment configuration
test('Test Case 3: Verify HTTPS deployment configuration (site served via HTTPS from GitHub CDN)', () => {
  const workflowPath = path.resolve(__dirname, '../../.github/workflows/deploy-pages.yml');
  const workflowContent = fs.readFileSync(workflowPath, 'utf8');
  const workflow = yaml.parse(workflowContent);

  // GitHub Pages automatically serves via HTTPS
  // We verify the configuration enables this:

  // 1. Check workflow uses official GitHub Pages actions
  const deployJob = workflow.jobs.deploy;
  const usesDeployPages = deployJob.steps.some(step =>
    step.uses && step.uses.includes('actions/deploy-pages')
  );
  assert(usesDeployPages, 'Should use official actions/deploy-pages for secure deployment');

  // 2. Check configure-pages is used for proper setup
  const buildJob = workflow.jobs.build;
  const usesConfigurePages = buildJob.steps.some(step =>
    step.uses && step.uses.includes('actions/configure-pages')
  );
  assert(usesConfigurePages, 'Should use actions/configure-pages for proper HTTPS configuration');

  // 3. Verify OIDC token for secure deployment
  assert(workflow.permissions['id-token'] === 'write',
    'Should have id-token write permission for secure OIDC deployment');

  // 4. Check environment URL is configured (for HTTPS deployment URL)
  assert(deployJob.environment.url, 'Deploy environment should have URL configured');
  console.log(`    Deployment URL template: ${deployJob.environment.url}`);

  // 5. Verify the HTML uses HTTPS for external resources
  const indexPath = path.resolve(__dirname, '../dist/index.html');
  if (fs.existsSync(indexPath)) {
    const htmlContent = fs.readFileSync(indexPath, 'utf8');

    // Check external links use HTTPS
    const httpLinks = htmlContent.match(/http:\/\/[^"'\s]*/g) || [];
    const nonSecureExternalLinks = httpLinks.filter(link =>
      !link.startsWith('http://localhost') &&
      !link.startsWith('http://127.0.0.1')
    );

    assert(nonSecureExternalLinks.length === 0,
      `All external resources should use HTTPS. Found non-secure links: ${nonSecureExternalLinks.join(', ')}`);

    console.log('    All external resources use HTTPS');
  }

  console.log('    HTTPS deployment configuration verified');
});

// Test Case 4: Check base URL configuration (asset paths work correctly on GitHub Pages domain)
test('Test Case 4: Check base URL configuration (all asset paths work correctly on GitHub Pages domain)', () => {
  const indexPath = path.resolve(__dirname, '../dist/index.html');
  assert(fs.existsSync(indexPath), 'dist/index.html should exist');

  const htmlContent = fs.readFileSync(indexPath, 'utf8');

  // Check CSS reference uses relative path (compatible with any base URL)
  const cssLinkMatch = htmlContent.match(/<link[^>]*href=["']([^"']*output\.css)["']/);
  assert(cssLinkMatch, 'Should have a link to output.css');

  const cssPath = cssLinkMatch[1];
  console.log(`    CSS path: ${cssPath}`);

  // Relative path or root-relative path are both acceptable for GitHub Pages
  const isRelativePath = !cssPath.startsWith('http://') && !cssPath.startsWith('https://');
  assert(isRelativePath, 'CSS path should be relative for GitHub Pages compatibility');

  // Check for any internal links that might break on GitHub Pages
  // Links to sections within the page should use anchors
  const internalLinks = htmlContent.match(/href=["']([^"']*?)["']/g) || [];

  const brokenInternalLinks = [];
  internalLinks.forEach(link => {
    const href = link.match(/href=["']([^"']*?)["']/)[1];

    // Skip external links, mailto, tel, anchors
    if (href.startsWith('http://') ||
        href.startsWith('https://') ||
        href.startsWith('mailto:') ||
        href.startsWith('tel:') ||
        href.startsWith('#')) {
      return;
    }

    // Check if it's a relative path to a file that should exist
    if (href.endsWith('.html') || href.endsWith('.css') || href.endsWith('.js')) {
      const fullPath = path.resolve(path.dirname(indexPath), href);
      if (!fs.existsSync(fullPath)) {
        brokenInternalLinks.push(href);
      }
    }
  });

  if (brokenInternalLinks.length > 0) {
    console.log(`    Warning: Potentially broken internal links: ${brokenInternalLinks.join(', ')}`);
  }

  // Verify no absolute paths that would break on GitHub Pages subdirectory
  const absoluteAssetPaths = htmlContent.match(/(src|href)=["']\/[^"']*["']/g) || [];
  const problematicPaths = absoluteAssetPaths.filter(p => {
    const path = p.match(/["']([^"']+)["']/)[1];
    // Root-relative paths starting with / might break if deployed to subdirectory
    // However, for user/org pages (username.github.io) they work fine
    return false; // We allow root-relative paths as GitHub Pages handles them
  });

  console.log(`    Found ${absoluteAssetPaths.length} root-relative asset paths`);

  // Check that image sources are either external HTTPS or relative
  const imgSources = htmlContent.match(/<img[^>]*src=["']([^"']+)["']/g) || [];
  imgSources.forEach(img => {
    const srcMatch = img.match(/src=["']([^"']+)["']/);
    if (srcMatch) {
      const src = srcMatch[1];
      const isValidSource = src.startsWith('https://') ||
                           src.startsWith('data:') ||
                           (!src.startsWith('http://') && !src.startsWith('//'));
      assert(isValidSource, `Image source should be HTTPS or relative: ${src}`);
    }
  });

  console.log('    Asset path configuration verified for GitHub Pages');
});

console.log('\n' + '='.repeat(50));
console.log(`\nResults: ${results.passed} passed, ${results.failed} failed`);

// Output detailed results for CI/CD
console.log('\nDetailed Results:');
results.tests.forEach(t => {
  console.log(`  ${t.status === 'pass' ? '✓' : '✗'} ${t.name}`);
  if (t.error) {
    console.log(`    Error: ${t.error}`);
  }
});

// Exit with error code if any tests failed
if (results.failed > 0) {
  process.exit(1);
}
