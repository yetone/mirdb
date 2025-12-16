/**
 * Commands Reference Section Tests
 * Scenario: Verify that supported commands (SET, GET, DELETE, INFO, etc.) are documented (REQ-4)
 */

const fs = require('fs');
const path = require('path');

describe('Commands Reference Section', () => {
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Test Case 1: Commands section element exists', () => {
    test('should have a commands section element with appropriate heading', () => {
      // Query for commands section element
      const commandsSection = document.querySelector('#commands, .commands, section[id*="command"]');

      expect(commandsSection).not.toBeNull();
      expect(commandsSection.tagName.toLowerCase()).toBe('section');
    });

    test('commands section should have a heading', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      const heading = commandsSection.querySelector('h2, h1');
      expect(heading).not.toBeNull();

      const headingText = heading.textContent.toLowerCase();
      expect(headingText).toContain('command');
    });
  });

  describe('Test Case 2: SET command documentation', () => {
    test('should document SET command with syntax including key, flags, exptime, bytes', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      // Look for SET command card
      const commandCards = commandsSection.querySelectorAll('.command-card, [class*="command"]');
      let setCommand = null;

      for (const card of commandCards) {
        const heading = card.querySelector('h3, h4, .command-name');
        if (heading && heading.textContent.toUpperCase() === 'SET') {
          setCommand = card;
          break;
        }
      }

      expect(setCommand).not.toBeNull();

      // Check for syntax with key, flags, exptime, bytes
      const syntaxElement = setCommand.querySelector('.command-syntax, code, pre');
      expect(syntaxElement).not.toBeNull();

      const syntaxText = setCommand.textContent.toLowerCase();
      expect(syntaxText).toContain('key');
      expect(syntaxText).toContain('flags');
      expect(syntaxText).toContain('exptime');
      expect(syntaxText).toContain('bytes');
    });
  });

  describe('Test Case 3: GET command documentation', () => {
    test('should document GET command with syntax including key', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      // Look for GET command card
      const commandCards = commandsSection.querySelectorAll('.command-card, [class*="command"]');
      let getCommand = null;

      for (const card of commandCards) {
        const heading = card.querySelector('h3, h4, .command-name');
        if (heading && heading.textContent.toUpperCase() === 'GET') {
          getCommand = card;
          break;
        }
      }

      expect(getCommand).not.toBeNull();

      // Check for syntax with key
      const syntaxElement = getCommand.querySelector('.command-syntax, code, pre');
      expect(syntaxElement).not.toBeNull();

      const syntaxText = syntaxElement.textContent.toLowerCase();
      expect(syntaxText).toContain('get');
      expect(syntaxText).toContain('key');
    });
  });

  describe('Test Case 4: DELETE command documentation', () => {
    test('should document DELETE command', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      // Look for DELETE command card
      const commandCards = commandsSection.querySelectorAll('.command-card, [class*="command"]');
      let deleteCommand = null;

      for (const card of commandCards) {
        const heading = card.querySelector('h3, h4, .command-name');
        if (heading && heading.textContent.toUpperCase() === 'DELETE') {
          deleteCommand = card;
          break;
        }
      }

      expect(deleteCommand).not.toBeNull();

      // Check for description
      const description = deleteCommand.querySelector('.command-description, p');
      expect(description).not.toBeNull();
      expect(description.textContent.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 5: INFO command documentation', () => {
    test('should document INFO command for database status', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      // Look for INFO command card
      const commandCards = commandsSection.querySelectorAll('.command-card, [class*="command"]');
      let infoCommand = null;

      for (const card of commandCards) {
        const heading = card.querySelector('h3, h4, .command-name');
        if (heading && heading.textContent.toUpperCase() === 'INFO') {
          infoCommand = card;
          break;
        }
      }

      expect(infoCommand).not.toBeNull();

      // Check for description mentioning status/database
      const cardText = infoCommand.textContent.toLowerCase();
      expect(cardText).toMatch(/status|database|level|info/i);
    });
  });

  describe('Test Case 6: Count documented commands', () => {
    test('should have at least 4 commands documented (SET, GET, DELETE, INFO minimum)', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      // Count command cards
      const commandCards = commandsSection.querySelectorAll('.command-card');
      expect(commandCards.length).toBeGreaterThanOrEqual(4);

      // Verify required commands exist
      const requiredCommands = ['SET', 'GET', 'DELETE', 'INFO'];
      const foundCommands = [];

      for (const card of commandCards) {
        const heading = card.querySelector('h3, h4');
        if (heading) {
          const commandName = heading.textContent.toUpperCase().trim();
          if (requiredCommands.includes(commandName)) {
            foundCommands.push(commandName);
          }
        }
      }

      expect(foundCommands).toContain('SET');
      expect(foundCommands).toContain('GET');
      expect(foundCommands).toContain('DELETE');
      expect(foundCommands).toContain('INFO');
    });

    test('should have command syntax for each documented command', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      const commandCards = commandsSection.querySelectorAll('.command-card');

      for (const card of commandCards) {
        const syntaxElement = card.querySelector('.command-syntax, code');
        expect(syntaxElement).not.toBeNull();
        expect(syntaxElement.textContent.trim().length).toBeGreaterThan(0);
      }
    });

    test('should have examples for each documented command', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      const commandCards = commandsSection.querySelectorAll('.command-card');

      for (const card of commandCards) {
        const exampleElement = card.querySelector('.command-example, pre');
        expect(exampleElement).not.toBeNull();
      }
    });
  });
});
