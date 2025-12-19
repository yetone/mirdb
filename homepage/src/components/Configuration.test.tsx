import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Configuration, configParametersData, exampleTomlConfig, type ConfigParameter } from './Configuration';

describe('Configuration Section - E2E Tests', () => {
  describe('Test Case 1: Check configuration section for example TOML', () => {
    it('displays a code block with valid TOML configuration example', () => {
      render(<Configuration />);

      // Check for the configuration section
      const configSection = screen.getByTestId('configuration-section');
      expect(configSection).toBeInTheDocument();

      // Check for TOML code block
      const tomlCodeBlock = screen.getByTestId('toml-config-block');
      expect(tomlCodeBlock).toBeInTheDocument();

      // Verify it's a pre element with code inside
      expect(tomlCodeBlock.tagName.toLowerCase()).toBe('pre');
      const codeElement = within(tomlCodeBlock).getByTestId('toml-code');
      expect(codeElement).toBeInTheDocument();
    });

    it('TOML example contains valid configuration syntax', () => {
      render(<Configuration />);

      // Verify the exported TOML config string is valid
      expect(exampleTomlConfig).toContain('=');
      expect(exampleTomlConfig).toContain('"');
    });
  });

  describe('Test Case 2: Verify TOML example includes addr setting', () => {
    it('example shows addr = "0.0.0.0:12333" or similar', () => {
      render(<Configuration />);

      const tomlCode = screen.getByTestId('toml-code');
      expect(tomlCode.textContent).toContain('addr');
      expect(tomlCode.textContent).toContain('0.0.0.0:12333');
    });

    it('addr configuration is present in the exported TOML example', () => {
      expect(exampleTomlConfig).toMatch(/addr\s*=\s*"0\.0\.0\.0:12333"/);
    });
  });

  describe('Test Case 3: Verify TOML example includes work_dir setting', () => {
    it('example shows work_dir configuration option', () => {
      render(<Configuration />);

      const tomlCode = screen.getByTestId('toml-code');
      expect(tomlCode.textContent).toContain('work_dir');
    });

    it('work_dir configuration is present in the exported TOML example', () => {
      expect(exampleTomlConfig).toMatch(/work_dir\s*=/);
    });
  });

  describe('Test Case 4: Check for parameter explanation table or list', () => {
    it('configuration parameters have descriptions explaining their purpose', () => {
      render(<Configuration />);

      // Check for the configuration table
      const configTable = screen.getByTestId('config-table');
      expect(configTable).toBeInTheDocument();

      // Check table has proper headers
      expect(screen.getByText('Parameter')).toBeInTheDocument();
      expect(screen.getByText('Description')).toBeInTheDocument();
      expect(screen.getByText('Default')).toBeInTheDocument();
    });

    it('all key parameters are documented in the table', () => {
      render(<Configuration />);

      const configTable = screen.getByTestId('config-table');

      // Check for key parameters
      expect(within(configTable).getByTestId('config-param-addr')).toBeInTheDocument();
      expect(within(configTable).getByTestId('config-param-work_dir')).toBeInTheDocument();
      expect(within(configTable).getByTestId('config-param-max_level')).toBeInTheDocument();
      expect(within(configTable).getByTestId('config-param-mem_table_max_size')).toBeInTheDocument();
      expect(within(configTable).getByTestId('config-param-sst_max_size')).toBeInTheDocument();
    });

    it('each parameter has a description that explains its purpose', () => {
      configParametersData.forEach((param: ConfigParameter) => {
        expect(param.name).toBeDefined();
        expect(param.description).toBeDefined();
        expect(param.description.length).toBeGreaterThan(10); // Meaningful description
        expect(param.default).toBeDefined();
      });
    });
  });

  describe('Test Case 5: Verify TOML code block has syntax highlighting', () => {
    it('TOML configuration renders with proper syntax highlighting class', () => {
      render(<Configuration />);

      const tomlCode = screen.getByTestId('toml-code');
      // Check that the code element has language-toml class for syntax highlighting
      expect(tomlCode.className).toContain('language-toml');
    });

    it('TOML block is wrapped in pre/code elements for proper formatting', () => {
      render(<Configuration />);

      const preBlock = screen.getByTestId('toml-config-block');
      expect(preBlock.tagName.toLowerCase()).toBe('pre');

      const codeElement = preBlock.querySelector('code');
      expect(codeElement).toBeInTheDocument();
      expect(codeElement?.className).toContain('language-toml');
    });
  });
});

describe('Configuration Component - Unit Tests', () => {
  it('renders without crashing', () => {
    render(<Configuration />);
    expect(screen.getByTestId('configuration-section')).toBeInTheDocument();
  });

  it('has a section title', () => {
    render(<Configuration />);
    expect(screen.getByText('Configuration')).toBeInTheDocument();
  });

  it('has proper id for navigation', () => {
    render(<Configuration />);
    const configSection = screen.getByTestId('configuration-section');
    expect(configSection).toHaveAttribute('id', 'configuration');
  });

  it('exports configParametersData array', () => {
    expect(Array.isArray(configParametersData)).toBe(true);
    expect(configParametersData.length).toBeGreaterThan(0);
  });

  it('exports exampleTomlConfig string', () => {
    expect(typeof exampleTomlConfig).toBe('string');
    expect(exampleTomlConfig.length).toBeGreaterThan(0);
  });

  it('exports ConfigParameter type interface', () => {
    const testParam: ConfigParameter = {
      name: 'test',
      description: 'Test parameter',
      default: 'value',
      category: 'Test',
    };
    expect(testParam.name).toBe('test');
  });

  describe('TOML Example Content Validation', () => {
    it('includes all required configuration options in TOML example', () => {
      // From PRD: Should include addr, max_level, work_dir, sst_max_size, mem_table_max_size
      expect(exampleTomlConfig).toContain('addr');
      expect(exampleTomlConfig).toContain('max_level');
      expect(exampleTomlConfig).toContain('work_dir');
      expect(exampleTomlConfig).toContain('sst_max_size');
      expect(exampleTomlConfig).toContain('mem_table_max_size');
    });
  });

  describe('Parameter Data Validation', () => {
    it('has correct number of parameters documented', () => {
      expect(configParametersData.length).toBeGreaterThanOrEqual(5);
    });

    it('includes addr parameter with correct default', () => {
      const addrParam = configParametersData.find((p) => p.name === 'addr');
      expect(addrParam).toBeDefined();
      expect(addrParam?.default).toBe('0.0.0.0:12333');
    });

    it('includes work_dir parameter', () => {
      const workDirParam = configParametersData.find((p) => p.name === 'work_dir');
      expect(workDirParam).toBeDefined();
      expect(workDirParam?.description).toContain('directory');
    });
  });
});
