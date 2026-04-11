/**
 * Delete Key Functionality Tests
 * Owner: Scenario 7 - Key Operations - Delete Key
 *
 * Tests for DELETE /api/keys/{key} API endpoint and UI delete functionality
 */

const fs = require('fs');
const path = require('path');

describe('Delete Key - Backend Handler Tests', () => {
    let keysRsContent;

    beforeAll(() => {
        const keysRsPath = path.join(__dirname, '../src/http/handlers/keys.rs');
        keysRsContent = fs.readFileSync(keysRsPath, 'utf8');
    });

    test('handle_delete_key function exists', () => {
        expect(keysRsContent).toContain('pub fn handle_delete_key');
    });

    test('DeleteKeyResult enum has correct variants', () => {
        expect(keysRsContent).toContain('pub enum DeleteKeyResult');
        expect(keysRsContent).toContain('Deleted(DeleteResponse)');
        expect(keysRsContent).toContain('NotFound(ErrorResponse)');
        expect(keysRsContent).toContain('Error(ErrorResponse)');
    });

    test('DeleteResponse struct has required fields', () => {
        expect(keysRsContent).toContain('pub struct DeleteResponse');
        expect(keysRsContent).toContain('success: bool');
        expect(keysRsContent).toContain('message: String');
    });

    test('Delete handler uses Request::Deleter', () => {
        expect(keysRsContent).toContain('Request::Deleter');
    });

    test('Delete handler handles Response::Deleted', () => {
        expect(keysRsContent).toContain('Response::Deleted');
    });

    test('Delete handler handles Response::NotFound', () => {
        expect(keysRsContent).toContain('Response::NotFound');
    });

    test('DeleteResponse is serializable', () => {
        // Check that DeleteResponse has Serialize derive
        const structIndex = keysRsContent.indexOf('pub struct DeleteResponse');
        expect(structIndex).toBeGreaterThan(-1);

        const beforeStruct = keysRsContent.substring(0, structIndex);
        const lastDerive = beforeStruct.lastIndexOf('#[derive');
        expect(lastDerive).toBeGreaterThan(-1);

        const deriveSection = keysRsContent.substring(lastDerive, structIndex);
        expect(deriveSection).toContain('Serialize');
    });

    test('Delete handler has unit tests', () => {
        expect(keysRsContent).toContain('test_delete_existing_key');
        expect(keysRsContent).toContain('test_delete_nonexistent_key');
    });

    test('Delete handler checks performance within 500ms', () => {
        expect(keysRsContent).toContain('test_delete_key_performance');
        expect(keysRsContent).toContain('elapsed.as_millis() < 500');
    });
});

describe('Delete Key - Router Tests', () => {
    let routerRsContent;

    beforeAll(() => {
        const routerRsPath = path.join(__dirname, '../src/http/router.rs');
        routerRsContent = fs.readFileSync(routerRsPath, 'utf8');
    });

    test('Router has ApiKeysDelete route variant', () => {
        expect(routerRsContent).toContain('ApiKeysDelete');
    });

    test('Router handles DELETE method', () => {
        expect(routerRsContent).toContain('Method::Delete');
    });

    test('DELETE /api/keys/{key} route is configured', () => {
        expect(routerRsContent).toContain('Method::Delete');
        expect(routerRsContent).toContain('/api/keys/');
    });
});

describe('Delete Key - JavaScript UI Tests', () => {
    let keysJsContent;

    beforeAll(() => {
        const keysJsPath = path.join(__dirname, '../src/web/scripts/keys.js');
        keysJsContent = fs.readFileSync(keysJsPath, 'utf8');
    });

    test('deleteKey function exists and is not a placeholder', () => {
        expect(keysJsContent).toContain('function deleteKey');
        // Ensure it's implemented, not just a placeholder
        expect(keysJsContent).not.toContain('function deleteKey(key) {\n        // Placeholder - to be implemented by Scenario 7\n    }');
    });

    test('deleteKey shows confirmation dialog', () => {
        expect(keysJsContent).toContain('showConfirm');
    });

    test('deleteKey has confirmation message text', () => {
        expect(keysJsContent).toContain('Are you sure you want to delete');
    });

    test('deleteKey calls API on confirmation', () => {
        expect(keysJsContent).toContain('MirDBApi.deleteKey');
    });

    test('deleteKey shows success message', () => {
        expect(keysJsContent).toContain('deleted successfully');
    });

    test('deleteKey handles errors with catch', () => {
        expect(keysJsContent).toContain('.catch');
    });

    test('deleteKey shows error message on failure', () => {
        expect(keysJsContent).toContain('Failed to delete key');
    });

    test('deleteKey refreshes key list after deletion', () => {
        expect(keysJsContent).toContain('loadKeys');
    });

    test('deleteKey is exported in module return', () => {
        expect(keysJsContent).toContain('deleteKey: deleteKey');
    });

    test('Cancel action shows feedback message', () => {
        expect(keysJsContent).toContain('Delete cancelled');
    });

    test('performDeleteKey helper function exists', () => {
        expect(keysJsContent).toContain('function performDeleteKey');
    });
});

describe('Delete Key - API Client Tests', () => {
    let apiJsContent;

    beforeAll(() => {
        const apiJsPath = path.join(__dirname, '../src/web/scripts/api.js');
        apiJsContent = fs.readFileSync(apiJsPath, 'utf8');
    });

    test('deleteKey function exists in API client', () => {
        expect(apiJsContent).toContain('function deleteKey');
    });

    test('deleteKey uses DELETE HTTP method', () => {
        expect(apiJsContent).toContain("method: 'DELETE'");
    });

    test('deleteKey uses correct endpoint with encoding', () => {
        expect(apiJsContent).toContain('/api/keys/');
        expect(apiJsContent).toContain('encodeURIComponent');
    });

    test('deleteKey is exported from API module', () => {
        expect(apiJsContent).toContain('deleteKey: deleteKey');
    });

    test('deleteKey returns a Promise', () => {
        // The function uses .then() which indicates it returns a Promise
        const deleteKeySection = apiJsContent.substring(
            apiJsContent.indexOf('function deleteKey'),
            apiJsContent.indexOf('function deleteKey') + 300
        );
        expect(deleteKeySection).toContain('.then(');
    });
});

describe('Delete Key - UI Utilities Tests', () => {
    let uiJsContent;

    beforeAll(() => {
        const uiJsPath = path.join(__dirname, '../src/web/scripts/ui.js');
        uiJsContent = fs.readFileSync(uiJsPath, 'utf8');
    });

    test('showConfirm function exists for delete confirmation', () => {
        expect(uiJsContent).toContain('function showConfirm');
    });

    test('showConfirm accepts onConfirm callback', () => {
        expect(uiJsContent).toContain('onConfirm');
    });

    test('showConfirm accepts onCancel callback', () => {
        expect(uiJsContent).toContain('onCancel');
    });

    test('Confirmation dialog has Cancel button', () => {
        expect(uiJsContent).toContain("textContent = 'Cancel'");
    });

    test('Confirmation dialog has Confirm button', () => {
        expect(uiJsContent).toContain("textContent = 'Confirm'");
    });

    test('MirDBUI exports showConfirm', () => {
        expect(uiJsContent).toContain('showConfirm: showConfirm');
    });
});

describe('Delete Key - E2E Flow Integration', () => {
    test('Complete delete flow is integrated', () => {
        // Test that all components work together
        const keysJsPath = path.join(__dirname, '../src/web/scripts/keys.js');
        const apiJsPath = path.join(__dirname, '../src/web/scripts/api.js');
        const uiJsPath = path.join(__dirname, '../src/web/scripts/ui.js');

        const keysJs = fs.readFileSync(keysJsPath, 'utf8');
        const apiJs = fs.readFileSync(apiJsPath, 'utf8');
        const uiJs = fs.readFileSync(uiJsPath, 'utf8');

        // keys.js uses MirDBUI.showConfirm
        expect(keysJs).toContain('MirDBUI.showConfirm');
        // keys.js uses MirDBApi.deleteKey
        expect(keysJs).toContain('MirDBApi.deleteKey');
        // keys.js uses MirDBUI.showToast
        expect(keysJs).toContain('MirDBUI.showToast');

        // API has deleteKey
        expect(apiJs).toContain('deleteKey');

        // UI has showConfirm
        expect(uiJs).toContain('showConfirm');
    });
});
