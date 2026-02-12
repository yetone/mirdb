/**
 * Jest Setup File
 * Sets up global environment for tests
 */

const { TextEncoder, TextDecoder } = require('util');

global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;
