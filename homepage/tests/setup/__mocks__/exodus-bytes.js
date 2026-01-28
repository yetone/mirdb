// Mock for @exodus/bytes to avoid ESM compatibility issues in Jest
module.exports = {
    encode: (str) => Buffer.from(str),
    decode: (buffer) => buffer.toString()
};
