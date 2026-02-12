/**
 * Jest Setup File
 * Provides globals for JSDOM environment
 */

const { TextEncoder, TextDecoder } = require('util');

global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;
