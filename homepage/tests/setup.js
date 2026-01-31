/**
 * Jest Setup File
 * Provides polyfills for jsdom environment
 */

const { TextEncoder, TextDecoder } = require('util');

global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;
