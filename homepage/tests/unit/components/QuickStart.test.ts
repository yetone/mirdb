import { describe, it, expect, beforeAll } from 'vitest';

import quickstartData from '../../../src/data/quickstart.json';
import type { QuickStartData } from '../../../src/types/index';

const data = quickstartData as QuickStartData;

describe('QuickStart data (quickstart.json)', () => {
  describe('Section metadata', () => {
    it('has a section title', () => {
      expect(data.sectionTitle).toBeTruthy();
      expect(typeof data.sectionTitle).toBe('string');
    });

    it('has a section description', () => {
      expect(data.sectionDescription).toBeTruthy();
      expect(typeof data.sectionDescription).toBe('string');
    });
  });

  describe('Installation methods', () => {
    it('has at least 2 installation methods', () => {
      expect(data.installMethods.length).toBeGreaterThanOrEqual(2);
    });

    it('each install method has a descriptive name', () => {
      for (const method of data.installMethods) {
        expect(method.id).toBeTruthy();
        expect(method.name).toBeTruthy();
        expect(typeof method.name).toBe('string');
        expect(method.name.length).toBeGreaterThan(0);
      }
    });

    it('has cargo as an installation method', () => {
      const cargoMethod = data.installMethods.find((m) => m.id === 'cargo');
      expect(cargoMethod).toBeDefined();
      expect(cargoMethod!.name).toBeTruthy();
    });

    it('has docker as an installation method', () => {
      const dockerMethod = data.installMethods.find((m) => m.id === 'docker');
      expect(dockerMethod).toBeDefined();
      expect(dockerMethod!.name).toBeTruthy();
    });

    it('has from-source as an installation method', () => {
      const sourceMethod = data.installMethods.find((m) => m.id === 'source');
      expect(sourceMethod).toBeDefined();
      expect(sourceMethod!.name).toBeTruthy();
    });

    it('cargo method shows cargo install command', () => {
      const cargoMethod = data.installMethods.find((m) => m.id === 'cargo')!;
      const commandsText = cargoMethod.commands.join(' ');
      expect(commandsText).toContain('cargo install');
    });

    it('docker method shows docker run command', () => {
      const dockerMethod = data.installMethods.find((m) => m.id === 'docker')!;
      const commandsText = dockerMethod.commands.join(' ');
      expect(commandsText).toMatch(/docker (run|pull)/);
    });

    it('each method has at least one command', () => {
      for (const method of data.installMethods) {
        expect(method.commands.length).toBeGreaterThanOrEqual(1);
      }
    });
  });

  describe('Usage examples', () => {
    it('has at least 3 usage examples', () => {
      expect(data.usageExamples.length).toBeGreaterThanOrEqual(3);
    });

    it('has a SET usage example', () => {
      const setExample = data.usageExamples.find((e) => e.id === 'set');
      expect(setExample).toBeDefined();
      expect(setExample!.operation).toBe('SET');
      expect(setExample!.code).toContain('set ');
    });

    it('has a GET usage example', () => {
      const getExample = data.usageExamples.find((e) => e.id === 'get');
      expect(getExample).toBeDefined();
      expect(getExample!.operation).toBe('GET');
      expect(getExample!.code).toContain('get ');
    });

    it('has a DELETE usage example', () => {
      const deleteExample = data.usageExamples.find((e) => e.id === 'delete');
      expect(deleteExample).toBeDefined();
      expect(deleteExample!.operation).toBe('DELETE');
      expect(deleteExample!.code).toContain('delete ');
    });

    it('SET example uses valid Memcached protocol syntax', () => {
      const setExample = data.usageExamples.find((e) => e.id === 'set')!;
      expect(setExample.code).toMatch(/^set \S+/);
    });

    it('GET example uses valid Memcached protocol syntax', () => {
      const getExample = data.usageExamples.find((e) => e.id === 'get')!;
      expect(getExample.code).toMatch(/^get \S+/);
    });

    it('DELETE example uses valid Memcached protocol syntax', () => {
      const deleteExample = data.usageExamples.find((e) => e.id === 'delete')!;
      expect(deleteExample.code).toMatch(/^delete \S+/);
    });

    it('each example has a description', () => {
      for (const example of data.usageExamples) {
        expect(example.description).toBeTruthy();
        expect(typeof example.description).toBe('string');
      }
    });
  });
});
