/**
 * Tab Panel Component for Terminal.
 * Owner: Scenario 3 - Interactive Terminal Component
 *
 * Expected features:
 * - Tab button styling (active/inactive states)
 * - Panel content area
 * - Keyboard navigation support
 */
import type { ReactNode, KeyboardEvent } from 'react';

interface Tab {
  id: string;
  label: string;
}

interface TabPanelProps {
  tabs: Tab[];
  activeTab: string;
  onTabChange: (tabId: string) => void;
  children: ReactNode;
}

export function TabPanel({ tabs, activeTab, onTabChange, children }: TabPanelProps) {
  const handleKeyDown = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    let newIndex = index;

    switch (event.key) {
      case 'ArrowLeft':
        event.preventDefault();
        newIndex = index === 0 ? tabs.length - 1 : index - 1;
        break;
      case 'ArrowRight':
        event.preventDefault();
        newIndex = index === tabs.length - 1 ? 0 : index + 1;
        break;
      case 'Home':
        event.preventDefault();
        newIndex = 0;
        break;
      case 'End':
        event.preventDefault();
        newIndex = tabs.length - 1;
        break;
      default:
        return;
    }

    onTabChange(tabs[newIndex].id);
    // Focus the new tab button
    const tabButton = document.getElementById(`tab-${tabs[newIndex].id}`);
    tabButton?.focus();
  };

  return (
    <div className="tab-panel" data-testid="tab-panel">
      <div
        className="tab-list flex border-b border-border"
        role="tablist"
        aria-label="Terminal examples"
      >
        {tabs.map((tab, index) => (
          <button
            key={tab.id}
            id={`tab-${tab.id}`}
            role="tab"
            aria-selected={activeTab === tab.id}
            aria-controls={`panel-${tab.id}`}
            tabIndex={activeTab === tab.id ? 0 : -1}
            onClick={() => onTabChange(tab.id)}
            onKeyDown={(e) => handleKeyDown(e, index)}
            className={`tab-button px-4 py-2 font-mono text-sm transition-colors duration-200 ${
              activeTab === tab.id
                ? 'text-accent border-b-2 border-accent bg-surface'
                : 'text-text-secondary hover:text-text-primary hover:bg-surface/50'
            }`}
            data-testid={`tab-${tab.id}`}
          >
            {tab.label}
          </button>
        ))}
      </div>
      <div
        id={`panel-${activeTab}`}
        role="tabpanel"
        aria-labelledby={`tab-${activeTab}`}
        tabIndex={0}
        className="tab-content p-4 bg-surface rounded-b-lg"
        data-testid="tab-content"
      >
        {children}
      </div>
    </div>
  );
}
