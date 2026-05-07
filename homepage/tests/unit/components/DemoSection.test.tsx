import { render, screen, waitFor } from '@testing-library/react';
import DemoSection from '@/components/DemoSection';

describe('DemoSection', () => {
  it('renders the demo section with heading and description', () => {
    render(<DemoSection />);

    expect(
      screen.getByRole('heading', { name: /see mirdb in action/i })
    ).toBeInTheDocument();
    expect(
      screen.getByText(
        /watch how mirdb handles basic crud operations/i
      )
    ).toBeInTheDocument();
  });

  it('displays usage.gif with descriptive alt text', () => {
    render(<DemoSection />);

    const gif = screen.getByTestId('usage-gif');
    expect(gif).toBeInTheDocument();
    expect(gif).toHaveAttribute(
      'alt',
      'Animated demonstration of MirDB running SET, GET, and DELETE commands in a terminal session'
    );
    expect(gif).toHaveAttribute('src', '/usage.gif');
  });

  it('usage.gif uses lazy loading', () => {
    render(<DemoSection />);

    const gif = screen.getByTestId('usage-gif');
    expect(gif).toHaveAttribute('loading', 'lazy');
  });

  it('renders the terminal animation component', () => {
    render(<DemoSection />);

    const terminal = screen.getByTestId('terminal-animation');
    expect(terminal).toBeInTheDocument();

    expect(screen.getByText('mirdb-demo')).toBeInTheDocument();

    const windowControls = terminal.querySelectorAll('.rounded-full');
    expect(windowControls.length).toBeGreaterThanOrEqual(3);
  });

  it('terminal animation shows SET, GET, and DELETE commands', async () => {
    render(<DemoSection />);

    await waitFor(
      () => {
        const commandLines = screen.queryAllByTestId('terminal-line-command');
        const commandTexts = commandLines.map((line) => line.textContent);
        expect(commandTexts.some((t) => t?.includes('set mykey'))).toBe(true);
        expect(commandTexts.some((t) => t?.includes('get mykey'))).toBe(true);
        expect(commandTexts.some((t) => t?.includes('delete mykey'))).toBe(true);
      },
      { timeout: 15000 }
    );
  }, 20000);

  it('terminal animation shows corresponding responses', async () => {
    render(<DemoSection />);

    await waitFor(
      () => {
        const responseLines = screen.queryAllByTestId('terminal-line-response');
        const responseTexts = responseLines.map((line) => line.textContent);
        expect(responseTexts.some((t) => t?.includes('STORED'))).toBe(true);
        expect(responseTexts.some((t) => t?.includes('DELETED'))).toBe(true);
        expect(responseTexts.some((t) => t?.includes('END'))).toBe(true);
      },
      { timeout: 15000 }
    );
  }, 20000);

  it('terminal has window controls (red, yellow, green dots)', () => {
    render(<DemoSection />);

    const terminal = screen.getByTestId('terminal-animation');
    const dots = terminal.querySelectorAll('.rounded-full');
    expect(dots.length).toBe(3);

    expect(dots[0]).toHaveClass('bg-red-500');
    expect(dots[1]).toHaveClass('bg-yellow-500');
    expect(dots[2]).toHaveClass('bg-green-500');
  });

  it('has a section with id "demo" for anchor navigation', () => {
    render(<DemoSection />);

    const section = screen.getByTestId('demo-section');
    expect(section).toHaveAttribute('id', 'demo');
  });
});
