'use client';

import { useState, useEffect, useCallback } from 'react';

interface TerminalStep {
  type: 'command' | 'response';
  text: string;
}

const TERMINAL_STEPS: TerminalStep[] = [
  { type: 'command', text: '$ telnet localhost 12333' },
  { type: 'response', text: 'Trying 127.0.0.1...' },
  { type: 'response', text: 'Connected to localhost.' },
  { type: 'command', text: 'set mykey 0 0 5' },
  { type: 'command', text: 'hello' },
  { type: 'response', text: 'STORED' },
  { type: 'command', text: 'get mykey' },
  { type: 'response', text: 'VALUE mykey 0 5' },
  { type: 'response', text: 'hello' },
  { type: 'response', text: 'END' },
  { type: 'command', text: 'delete mykey' },
  { type: 'response', text: 'DELETED' },
  { type: 'command', text: 'get mykey' },
  { type: 'response', text: 'END' },
];

const TYPING_SPEED_MS = 40;
const RESPONSE_DELAY_MS = 600;
const LOOP_DELAY_MS = 3000;

function TerminalAnimation() {
  const [displayedLines, setDisplayedLines] = useState<
    { type: 'command' | 'response'; text: string }[]
  >([]);
  const [currentTyping, setCurrentTyping] = useState('');
  const [stepIndex, setStepIndex] = useState(0);
  const [charIndex, setCharIndex] = useState(0);
  const [isWaiting, setIsWaiting] = useState(false);

  const resetAnimation = useCallback(() => {
    setDisplayedLines([]);
    setCurrentTyping('');
    setStepIndex(0);
    setCharIndex(0);
    setIsWaiting(false);
  }, []);

  useEffect(() => {
    if (stepIndex >= TERMINAL_STEPS.length) {
      const timeout = setTimeout(() => {
        resetAnimation();
      }, LOOP_DELAY_MS);
      return () => clearTimeout(timeout);
    }

    if (isWaiting) {
      const timeout = setTimeout(() => {
        setIsWaiting(false);
        setStepIndex((prev) => prev + 1);
        setCharIndex(0);
        setCurrentTyping('');
      }, RESPONSE_DELAY_MS);
      return () => clearTimeout(timeout);
    }

    const step = TERMINAL_STEPS[stepIndex];

    if (step.type === 'response') {
      setDisplayedLines((prev) => [...prev, step]);
      setIsWaiting(true);
      return;
    }

    if (charIndex < step.text.length) {
      const timeout = setTimeout(() => {
        setCurrentTyping(step.text.slice(0, charIndex + 1));
        setCharIndex((prev) => prev + 1);
      }, TYPING_SPEED_MS);
      return () => clearTimeout(timeout);
    }

    setDisplayedLines((prev) => [...prev, step]);
    setIsWaiting(true);
  }, [stepIndex, charIndex, isWaiting, resetAnimation]);

  const currentStep = TERMINAL_STEPS[stepIndex];
  const isTypingCommand =
    currentStep && currentStep.type === 'command' && !isWaiting;

  return (
    <div
      className="rounded-lg overflow-hidden border border-[var(--border)] shadow-2xl"
      data-testid="terminal-animation"
    >
      <div className="bg-[var(--code-bg)] px-4 py-2 flex items-center gap-2">
        <div className="w-3 h-3 rounded-full bg-red-500" />
        <div className="w-3 h-3 rounded-full bg-yellow-500" />
        <div className="w-3 h-3 rounded-full bg-green-500" />
        <span className="text-[var(--muted)] text-sm ml-2 font-mono">
          mirdb-demo
        </span>
      </div>
      <div className="bg-[#0f172a] p-4 font-mono text-sm min-h-[320px]">
        {displayedLines.map((line, i) => (
          <div
            key={i}
            className={
              line.type === 'command'
                ? 'text-green-400'
                : 'text-[var(--code-fg)]'
            }
            data-testid={`terminal-line-${line.type}`}
          >
            {line.type === 'command' ? (
              <>
                <span className="text-cyan-400">$</span>{' '}
                {line.text.replace('$ ', '')}
              </>
            ) : (
              line.text
            )}
          </div>
        ))}
        {isTypingCommand && (
          <div className="text-green-400">
            <span className="text-cyan-400">$</span> {currentTyping}
            <span className="animate-pulse">|</span>
          </div>
        )}
      </div>
    </div>
  );
}

export default function DemoSection() {
  return (
    <section
      id="demo"
      className="py-20 px-4 sm:px-6 lg:px-8 bg-[var(--background)]"
      data-testid="demo-section"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl font-bold mb-4 text-[var(--foreground)]">
            See MirDB in Action
          </h2>
          <p className="text-lg text-[var(--muted-foreground)] max-w-2xl mx-auto">
            Watch how MirDB handles basic CRUD operations through the memcached
            text protocol.
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 items-start">
          <div className="flex flex-col items-center">
            <img
              src="/usage.gif"
              alt="Animated demonstration of MirDB running SET, GET, and DELETE commands in a terminal session"
              loading="lazy"
              className="rounded-lg shadow-lg max-w-full h-auto"
              data-testid="usage-gif"
            />
            <p className="mt-3 text-sm text-[var(--muted-foreground)]">
              Terminal session showing MirDB CRUD operations
            </p>
          </div>

          <div>
            <TerminalAnimation />
          </div>
        </div>
      </div>
    </section>
  );
}
