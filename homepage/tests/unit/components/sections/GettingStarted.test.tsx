/**
 * Unit tests for GettingStarted component
 * Owner: Scenario 4 - Getting Started Section
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { GettingStarted } from '../../../../src/components/sections/GettingStarted';

describe('GettingStarted', () => {
  const mockWriteText = vi.fn();

  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('should render the section with correct id', () => {
    render(<GettingStarted />);

    const section = screen.getByTestId('getting-started-section');
    expect(section).toBeInTheDocument();
    expect(section).toHaveAttribute('id', 'getting-started');
  });

  it('should render the section title', () => {
    render(<GettingStarted />);

    const title = screen.getByRole('heading', { name: /getting started/i });
    expect(title).toBeInTheDocument();
  });

  it('should render code block with git clone command', () => {
    render(<GettingStarted />);

    const codeContents = screen.getAllByTestId('code-content');
    const gitCloneCode = codeContents.find(el =>
      el.textContent?.includes('git clone')
    );
    expect(gitCloneCode).toBeInTheDocument();
    expect(gitCloneCode).toHaveTextContent('git clone https://github.com/yetone/mirdb.git');
  });

  it('should render code block with cargo build command', () => {
    render(<GettingStarted />);

    const codeContents = screen.getAllByTestId('code-content');
    const cargoBuildCode = codeContents.find(el =>
      el.textContent?.includes('cargo build')
    );
    expect(cargoBuildCode).toBeInTheDocument();
    expect(cargoBuildCode).toHaveTextContent('cargo build --release');
  });

  it('should render code block with cargo run command', () => {
    render(<GettingStarted />);

    const codeContents = screen.getAllByTestId('code-content');
    const cargoRunCode = codeContents.find(el =>
      el.textContent?.includes('cargo run')
    );
    expect(cargoRunCode).toBeInTheDocument();
    expect(cargoRunCode).toHaveTextContent('cargo run --release');
  });

  it('should render copy button for each code block', () => {
    render(<GettingStarted />);

    const copyButtons = screen.getAllByTestId('copy-button');
    expect(copyButtons).toHaveLength(3);
  });

  it('should render three code blocks total', () => {
    render(<GettingStarted />);

    const codeBlocks = screen.getAllByTestId('code-block');
    expect(codeBlocks).toHaveLength(3);
  });

  it('should render labels for each code block', () => {
    render(<GettingStarted />);

    expect(screen.getByText('Clone Repository')).toBeInTheDocument();
    expect(screen.getByText('Build Project')).toBeInTheDocument();
    expect(screen.getByText('Run Server')).toBeInTheDocument();
  });

  it('should show toast notification when copy button is clicked', async () => {
    mockWriteText.mockResolvedValue(undefined);

    render(<GettingStarted />);

    const copyButtons = screen.getAllByTestId('copy-button');

    await act(async () => {
      fireEvent.click(copyButtons[0]);
    });

    // Allow microtasks to process
    await act(async () => {
      await Promise.resolve();
    });

    expect(screen.getByTestId('toast')).toBeInTheDocument();
    expect(screen.getByText('Copied to clipboard!')).toBeInTheDocument();
  });

  it('should call clipboard API when copy button is clicked', async () => {
    mockWriteText.mockResolvedValue(undefined);

    render(<GettingStarted />);

    const copyButtons = screen.getAllByTestId('copy-button');

    await act(async () => {
      fireEvent.click(copyButtons[0]);
    });

    expect(mockWriteText).toHaveBeenCalledWith('git clone https://github.com/yetone/mirdb.git');
  });

  it('should show error toast when copy fails', async () => {
    mockWriteText.mockRejectedValue(new Error('Copy failed'));

    render(<GettingStarted />);

    const copyButtons = screen.getAllByTestId('copy-button');

    await act(async () => {
      fireEvent.click(copyButtons[0]);
    });

    // Allow microtasks to process
    await act(async () => {
      await Promise.resolve();
    });

    expect(screen.getByTestId('toast')).toBeInTheDocument();
    expect(screen.getByText('Failed to copy')).toBeInTheDocument();
  });

  it('should have accessible aria-labelledby attribute', () => {
    render(<GettingStarted />);

    const section = screen.getByTestId('getting-started-section');
    expect(section).toHaveAttribute('aria-labelledby', 'getting-started-title');
  });

  it('should render description text', () => {
    render(<GettingStarted />);

    expect(screen.getByText(/get up and running with mirdb/i)).toBeInTheDocument();
  });
});
