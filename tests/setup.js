// Jest setup file for TextEncoder/TextDecoder polyfill
const { TextEncoder, TextDecoder } = require('util');

global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;
