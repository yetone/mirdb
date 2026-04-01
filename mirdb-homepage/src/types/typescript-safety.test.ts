/**
 * TypeScript Type Safety Tests.
 * Owner: Scenario 18 - TypeScript Type Safety
 *
 * Tests:
 * - TypeScript compiler reports no errors (Test Case 1)
 * - Component props have TypeScript interfaces defined (Test Case 2)
 * - tsconfig.json has strict: true enabled (Test Case 3)
 */

import { describe, it, expect } from 'vitest'
import { execSync } from 'child_process'
import { readFileSync, existsSync, readdirSync } from 'fs'
import { resolve, join } from 'path'

const PROJECT_ROOT = resolve(__dirname, '../..')

describe('TypeScript Type Safety', () => {
  describe('Test Case 1: TypeScript Compiler', () => {
    it('should report no errors when running tsc --noEmit', () => {
      let output: string
      let exitCode: number = 0

      try {
        // Run TypeScript compiler
        output = execSync('node node_modules/typescript/bin/tsc --noEmit', {
          cwd: PROJECT_ROOT,
          encoding: 'utf-8',
          stdio: ['pipe', 'pipe', 'pipe'],
        })
      } catch (error: unknown) {
        const execError = error as { stdout?: string; stderr?: string; status?: number }
        output = execError.stdout || execError.stderr || ''
        exitCode = execError.status || 1
      }

      // TypeScript should exit with 0 (no errors)
      expect(exitCode).toBe(0)
      // Output should be empty or contain only warnings (no errors)
      expect(output.trim()).toBe('')
    })

    it('should include all source files in compilation', () => {
      const tsconfigPath = resolve(PROJECT_ROOT, 'tsconfig.json')
      const tsconfig = JSON.parse(readFileSync(tsconfigPath, 'utf-8'))

      expect(tsconfig.include).toBeDefined()
      expect(tsconfig.include).toContain('src')
    })
  })

  describe('Test Case 2: Prop Type Definitions', () => {
    const componentsDir = resolve(PROJECT_ROOT, 'src/components')

    it('should have TypeScript interfaces for Hero component props', () => {
      const heroPath = resolve(componentsDir, 'Hero/Hero.tsx')
      const content = readFileSync(heroPath, 'utf-8')

      // Check for interface definition
      expect(content).toMatch(/interface\s+HeroProps\s*\{/)
      // Check that component uses the interface
      expect(content).toMatch(/function\s+Hero\s*\(\s*\{[\s\S]*\}\s*:\s*HeroProps\s*\)/)
    })

    it('should have TypeScript interfaces for FeatureCard component props', () => {
      const featureCardPath = resolve(componentsDir, 'Features/FeatureCard.tsx')
      const content = readFileSync(featureCardPath, 'utf-8')

      // Check for interface definition
      expect(content).toMatch(/interface\s+FeatureCardProps\s*\{/)
      // Check that component uses the interface
      expect(content).toMatch(/function\s+FeatureCard\s*\(\s*\{[\s\S]*\}\s*:\s*FeatureCardProps\s*\)/)
    })

    it('should have TypeScript interfaces for RoadmapItem component props', () => {
      const roadmapItemPath = resolve(componentsDir, 'Roadmap/RoadmapItem.tsx')
      const content = readFileSync(roadmapItemPath, 'utf-8')

      // Check for interface definition
      expect(content).toMatch(/interface\s+RoadmapItemProps\s*\{/)
      // Check that component uses the interface
      expect(content).toMatch(/function\s+RoadmapItem\s*\(\s*\{[\s\S]*\}\s*:\s*RoadmapItemProps\s*\)/)
    })

    it('should have TypeScript interfaces for ThemeToggle component props', () => {
      const themeTogglePath = resolve(componentsDir, 'ThemeToggle/ThemeToggle.tsx')
      const content = readFileSync(themeTogglePath, 'utf-8')

      // Check for interface definition
      expect(content).toMatch(/interface\s+ThemeToggleProps\s*\{/)
    })

    it('should have all component folders with .tsx files', () => {
      const componentFolders = readdirSync(componentsDir).filter(
        (item) => {
          const itemPath = join(componentsDir, item)
          return existsSync(itemPath) &&
            readdirSync(itemPath).some((file) => file.endsWith('.tsx') && !file.includes('.test.'))
        }
      )

      // Verify at least key components exist
      expect(componentFolders).toContain('Hero')
      expect(componentFolders).toContain('Features')
      expect(componentFolders).toContain('Roadmap')
      expect(componentFolders).toContain('ThemeToggle')
    })
  })

  describe('Test Case 3: Strict Mode', () => {
    it('should have strict: true in tsconfig.json', () => {
      const tsconfigPath = resolve(PROJECT_ROOT, 'tsconfig.json')
      const tsconfig = JSON.parse(readFileSync(tsconfigPath, 'utf-8'))

      expect(tsconfig.compilerOptions).toBeDefined()
      expect(tsconfig.compilerOptions.strict).toBe(true)
    })

    it('should have additional strict checks enabled', () => {
      const tsconfigPath = resolve(PROJECT_ROOT, 'tsconfig.json')
      const tsconfig = JSON.parse(readFileSync(tsconfigPath, 'utf-8'))
      const options = tsconfig.compilerOptions

      // Additional strict checks that enhance type safety
      expect(options.noUnusedLocals).toBe(true)
      expect(options.noUnusedParameters).toBe(true)
      expect(options.noFallthroughCasesInSwitch).toBe(true)
    })

    it('should have noEmit: true for type-checking only', () => {
      const tsconfigPath = resolve(PROJECT_ROOT, 'tsconfig.json')
      const tsconfig = JSON.parse(readFileSync(tsconfigPath, 'utf-8'))

      expect(tsconfig.compilerOptions.noEmit).toBe(true)
    })

    it('should use React JSX transform', () => {
      const tsconfigPath = resolve(PROJECT_ROOT, 'tsconfig.json')
      const tsconfig = JSON.parse(readFileSync(tsconfigPath, 'utf-8'))

      expect(tsconfig.compilerOptions.jsx).toBe('react-jsx')
    })
  })

  describe('Type Definition Files', () => {
    const typesDir = resolve(PROJECT_ROOT, 'src/types')

    it('should have shared type definitions in index.ts', () => {
      const indexPath = resolve(typesDir, 'index.ts')
      expect(existsSync(indexPath)).toBe(true)

      const content = readFileSync(indexPath, 'utf-8')
      // Check for Theme type
      expect(content).toMatch(/type\s+Theme\s*=/)
      // Check for ButtonVariant type
      expect(content).toMatch(/type\s+ButtonVariant\s*=/)
      // Check for ExternalLink interface
      expect(content).toMatch(/interface\s+ExternalLink/)
    })

    it('should have feature type definitions', () => {
      const featuresPath = resolve(typesDir, 'features.ts')
      expect(existsSync(featuresPath)).toBe(true)

      const content = readFileSync(featuresPath, 'utf-8')
      // Check for FeatureStatus type
      expect(content).toMatch(/type\s+FeatureStatus\s*=/)
      // Check for Feature interface
      expect(content).toMatch(/interface\s+Feature/)
    })

    it('should have roadmap type definitions', () => {
      const roadmapPath = resolve(typesDir, 'roadmap.ts')
      expect(existsSync(roadmapPath)).toBe(true)

      const content = readFileSync(roadmapPath, 'utf-8')
      // Check for RoadmapStatus type
      expect(content).toMatch(/type\s+RoadmapStatus\s*=/)
      // Check for RoadmapItem interface
      expect(content).toMatch(/interface\s+RoadmapItem/)
    })

    it('should have Vitest type augmentation for jest-dom matchers', () => {
      const vitestTypesPath = resolve(typesDir, 'vitest.d.ts')
      expect(existsSync(vitestTypesPath)).toBe(true)

      const content = readFileSync(vitestTypesPath, 'utf-8')
      // Check for jest-dom reference
      expect(content).toMatch(/@testing-library\/jest-dom/)
      // Check for module augmentation
      expect(content).toMatch(/declare\s+module\s+['"]vitest['"]/)
    })
  })
})
