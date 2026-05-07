import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import QuickStartSection from '@/components/QuickStartSection';

// Mock CodeBlock to simplify tests
jest.mock('@/components/ui/CodeBlock', () => {
  return function MockCodeBlock({ code, language }: { code: string; language: string }) {
    return (
      <div data-testid="code-block" data-language={language}>
        <pre data-testid="code-content">{code}</pre>
        <button data-testid="copy-button">Copy</button>
      </div>
    );
  };
});

describe('QuickStartSection', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders the section with correct heading and description', () => {
    render(<QuickStartSection />);

    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('Getting Started');
    expect(
      screen.getByText(/Get MirDB running locally in minutes/)
    ).toBeInTheDocument();
  });

  it('renders all four quick-start steps', () => {
    render(<QuickStartSection />);

    const steps = [
      'Clone the repository',
      'Build the project',
      'Run the server',
      'Connect and test',
    ];

    steps.forEach((stepTitle) => {
      expect(screen.getByText(stepTitle)).toBeInTheDocument();
    });
  });

  it('renders step numbers in order', () => {
    render(<QuickStartSection />);

    for (let i = 0; i < 4; i++) {
      expect(screen.getByTestId(`quick-start-step-${i}`)).toBeInTheDocument();
    }
  });

  it('renders code blocks with bash language identifier', () => {
    render(<QuickStartSection />);

    const codeBlocks = screen.getAllByTestId('code-block');
    expect(codeBlocks).toHaveLength(4);

    codeBlocks.forEach((block) => {
      expect(block).toHaveAttribute('data-language', 'bash');
    });
  });

  it('renders clone command in first step', () => {
    render(<QuickStartSection />);

    const firstCodeBlock = screen.getByTestId('quick-start-step-0');
    expect(firstCodeBlock).toHaveTextContent('git clone');
    expect(firstCodeBlock).toHaveTextContent('cd mirdb');
  });

  it('renders build command in second step', () => {
    render(<QuickStartSection />);

    const secondStep = screen.getByTestId('quick-start-step-1');
    expect(secondStep).toHaveTextContent('cargo build --release');
  });

  it('renders run command in third step', () => {
    render(<QuickStartSection />);

    const thirdStep = screen.getByTestId('quick-start-step-2');
    expect(thirdStep).toHaveTextContent('cargo run --release --bin mirdb');
  });

  it('renders memcached SET/GET commands in fourth step', () => {
    render(<QuickStartSection />);

    const fourthStep = screen.getByTestId('quick-start-step-3');
    expect(fourthStep).toHaveTextContent('telnet');
    expect(fourthStep).toHaveTextContent('set mykey');
    expect(fourthStep).toHaveTextContent('get mykey');
    expect(fourthStep).toHaveTextContent('STORED');
    expect(fourthStep).toHaveTextContent('END');
  });

  it('displays default configuration note with listen address and work directory', () => {
    render(<QuickStartSection />);

    const configNote = screen.getByText(/Default settings:/);
    expect(configNote).toBeInTheDocument();

    // Check for listen address
    expect(screen.getByText(/0\.0\.0\.0:12333/)).toBeInTheDocument();

    // Check for work directory
    expect(screen.getByText(/\/tmp\/mirdb/)).toBeInTheDocument();

    // Check for config file reference
    expect(screen.getByText(/etc\/mirdb\.toml/)).toBeInTheDocument();
  });

  it('has the correct section id for anchor navigation', () => {
    render(<QuickStartSection />);

    const section = screen.getByTestId('quick-start-section');
    expect(section).toHaveAttribute('id', 'quick-start');
  });

  it('renders copy buttons for each code block', () => {
    render(<QuickStartSection />);

    const copyButtons = screen.getAllByTestId('copy-button');
    expect(copyButtons).toHaveLength(4);
  });
});
