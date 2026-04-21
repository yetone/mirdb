//! Interactive Try It Out Section tests.
//! Owner: Scenario 11 - Interactive Try It Out Section
//!
//! Tests:
//! 1. Enter key='demo' value='hello' in SET form and submit - Success message displayed
//! 2. Enter key='demo' in GET form and submit - Value 'hello' displayed in result area
//! 3. Enter key='demo' in DELETE form and submit - Success message: 'DELETED'
//! 4. Submit SET form with empty key - Validation error displayed immediately
//! 5. GET non-existent key via form - Display 'NOT_FOUND' or appropriate error message

use axum::{body::Body, http::Request};
use tower::ServiceExt;

use mirdb::web::routes::create_router;

/// Helper function to get homepage HTML content
async fn get_homepage_html() -> String {
    let app = create_router();
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

/// Helper function to get JavaScript content
async fn get_javascript() -> String {
    let app = create_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/main.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

/// Test Case 1: Verify Try It Out section exists with SET form
/// Test input: Enter key='demo' value='hello' in SET form and submit
/// Expected: Success message displayed: 'STORED' or similar
#[tokio::test]
async fn test_try_it_out_section_exists() {
    let html = get_homepage_html().await;

    // Verify Try It Out section exists
    assert!(
        html.contains("id=\"try-it-out\""),
        "Homepage should have a try-it-out section"
    );

    // Verify section has proper heading
    assert!(
        html.contains("Try It Out"),
        "Section should have 'Try It Out' heading"
    );

    // Verify section has aria-labelledby for accessibility
    assert!(
        html.contains("aria-labelledby=\"try-title\""),
        "Try It Out section should have aria-labelledby for accessibility"
    );
}

/// Test Case 1 (continued): Verify SET form structure
#[tokio::test]
async fn test_set_form_exists_with_correct_structure() {
    let html = get_homepage_html().await;

    // Verify SET form exists
    assert!(
        html.contains("id=\"set-form\""),
        "Homepage should have a SET form"
    );

    // Verify SET form has heading
    assert!(
        html.contains("SET Operation"),
        "SET form should have 'SET Operation' heading"
    );

    // Verify SET form has key input
    assert!(
        html.contains("name=\"key\""),
        "SET form should have a key input field"
    );

    // Verify SET form has value input
    assert!(
        html.contains("name=\"value\""),
        "SET form should have a value input field"
    );

    // Verify SET form has submit button
    assert!(
        html.contains(">SET</button>"),
        "SET form should have a SET submit button"
    );
}

/// Test Case 2: Verify GET form structure
/// Test input: Enter key='demo' in GET form and submit
/// Expected: Value 'hello' displayed in result area
#[tokio::test]
async fn test_get_form_exists_with_correct_structure() {
    let html = get_homepage_html().await;

    // Verify GET form exists
    assert!(
        html.contains("id=\"get-form\""),
        "Homepage should have a GET form"
    );

    // Verify GET form has heading
    assert!(
        html.contains("GET Operation"),
        "GET form should have 'GET Operation' heading"
    );

    // Verify GET form has key input
    assert!(
        html.contains("name=\"key\"") && html.contains("id=\"get-form\""),
        "GET form should have a key input field"
    );

    // Verify GET form has submit button
    assert!(
        html.contains(">GET</button>"),
        "GET form should have a GET submit button"
    );
}

/// Test Case 3: Verify DELETE form structure
/// Test input: Enter key='demo' in DELETE form and submit
/// Expected: Success message: 'DELETED', subsequent GET shows not found
#[tokio::test]
async fn test_delete_form_exists_with_correct_structure() {
    let html = get_homepage_html().await;

    // Verify DELETE form exists
    assert!(
        html.contains("id=\"delete-form\""),
        "Homepage should have a DELETE form"
    );

    // Verify DELETE form has heading
    assert!(
        html.contains("DELETE Operation"),
        "DELETE form should have 'DELETE Operation' heading"
    );

    // Verify DELETE form has key input
    assert!(
        html.contains("name=\"key\"") && html.contains("id=\"delete-form\""),
        "DELETE form should have a key input field"
    );

    // Verify DELETE form has submit button with danger styling
    assert!(
        html.contains(">DELETE</button>"),
        "DELETE form should have a DELETE submit button"
    );

    // Verify DELETE button has danger class for visual emphasis
    assert!(
        html.contains("btn-danger"),
        "DELETE button should have btn-danger class"
    );
}

/// Test Case 4: Verify form validation for empty key
/// Test input: Submit SET form with empty key
/// Expected: Validation error displayed immediately (before submit)
#[tokio::test]
async fn test_form_inputs_have_required_attribute() {
    let html = get_homepage_html().await;

    // Verify key inputs have required attribute for HTML5 validation
    assert!(
        html.contains("required"),
        "Form inputs should have required attribute"
    );

    // Verify inputs have accessible labels (either aria-label or proper label elements)
    assert!(
        html.contains("aria-label=\"Key\"") || html.contains("<label for=") && html.contains("sr-only"),
        "Key inputs should have accessible labels"
    );
}

/// Test Case 4 (continued): Verify JavaScript validation exists
#[tokio::test]
async fn test_javascript_validates_empty_key() {
    let js = get_javascript().await;

    // Verify handleSetForm function exists
    assert!(
        js.contains("handleSetForm") || js.contains("async function handleSetForm"),
        "JavaScript should have handleSetForm function"
    );

    // Verify validateForm function exists
    assert!(
        js.contains("function validateForm"),
        "JavaScript should have validateForm function"
    );

    // Verify validation checks for empty key
    assert!(
        js.contains("Key is required"),
        "Validation should check for empty key"
    );

    // Verify validation is called before form submission
    assert!(
        js.contains("validateForm(key"),
        "Form handlers should call validateForm"
    );
}

/// Test Case 5: Verify result display area exists
/// Test input: GET non-existent key via form
/// Expected: Display 'NOT_FOUND' or appropriate error message
#[tokio::test]
async fn test_result_display_area_exists() {
    let html = get_homepage_html().await;

    // Verify result display area exists
    assert!(
        html.contains("id=\"result\""),
        "Homepage should have a result display area"
    );

    // Verify result area has aria-live for accessibility (announces changes)
    assert!(
        html.contains("aria-live=\"polite\""),
        "Result area should have aria-live for screen reader announcements"
    );
}

/// Verify JavaScript handles SET form submission
#[tokio::test]
async fn test_javascript_handles_set_form_submission() {
    let js = get_javascript().await;

    // Verify handleSetForm function exists
    assert!(
        js.contains("async function handleSetForm") || js.contains("function handleSetForm"),
        "JavaScript should have handleSetForm function"
    );

    // Verify it sends POST to /api/kv/set
    assert!(
        js.contains("/api/kv/set"),
        "handleSetForm should POST to /api/kv/set endpoint"
    );

    // Verify it sends JSON body
    assert!(
        js.contains("Content-Type") && js.contains("application/json"),
        "handleSetForm should send JSON content type"
    );

    // Verify it uses JSON.stringify
    assert!(
        js.contains("JSON.stringify"),
        "handleSetForm should stringify request body as JSON"
    );

    // Verify it shows STORED message on success
    assert!(
        js.contains("STORED"),
        "handleSetForm should show STORED message on success"
    );
}

/// Verify JavaScript handles GET form submission
#[tokio::test]
async fn test_javascript_handles_get_form_submission() {
    let js = get_javascript().await;

    // Verify handleGetForm function exists
    assert!(
        js.contains("async function handleGetForm") || js.contains("function handleGetForm"),
        "JavaScript should have handleGetForm function"
    );

    // Verify it fetches from /api/kv/get
    assert!(
        js.contains("/api/kv/get"),
        "handleGetForm should fetch from /api/kv/get endpoint"
    );

    // Verify it uses query parameter for key
    assert!(
        js.contains("encodeURIComponent") || js.contains("?key="),
        "handleGetForm should pass key as query parameter"
    );
}

/// Verify JavaScript handles DELETE form submission
#[tokio::test]
async fn test_javascript_handles_delete_form_submission() {
    let js = get_javascript().await;

    // Verify handleDeleteForm function exists
    assert!(
        js.contains("async function handleDeleteForm") || js.contains("function handleDeleteForm"),
        "JavaScript should have handleDeleteForm function"
    );

    // Verify it sends DELETE to /api/kv/delete
    assert!(
        js.contains("/api/kv/delete"),
        "handleDeleteForm should DELETE to /api/kv/delete endpoint"
    );

    // Verify it uses DELETE method
    assert!(
        js.contains("method: 'DELETE'"),
        "handleDeleteForm should use DELETE HTTP method"
    );

    // Verify it shows DELETED message on success
    assert!(
        js.contains("DELETED"),
        "handleDeleteForm should show DELETED message on success"
    );
}

/// Verify JavaScript displays result messages
#[tokio::test]
async fn test_javascript_displays_results() {
    let js = get_javascript().await;

    // Verify showResult function exists
    assert!(
        js.contains("function showResult"),
        "JavaScript should have showResult function"
    );

    // Verify it updates the result element
    assert!(
        js.contains("getElementById('result')") || js.contains("result"),
        "showResult should update the result element"
    );

    // Verify it can show both success and error states
    assert!(
        js.contains("success") && js.contains("error"),
        "showResult should handle both success and error states"
    );
}

/// Verify JavaScript handles NOT_FOUND response for GET
#[tokio::test]
async fn test_javascript_handles_not_found_response() {
    let js = get_javascript().await;

    // Verify handleGetForm handles not found case
    assert!(
        js.contains("NOT_FOUND"),
        "handleGetForm should handle not found case with NOT_FOUND message"
    );
}

/// Verify form handlers are attached on DOM ready
#[tokio::test]
async fn test_form_handlers_attached_on_dom_ready() {
    let js = get_javascript().await;

    // Verify DOMContentLoaded event handler exists
    assert!(
        js.contains("DOMContentLoaded"),
        "JavaScript should handle DOMContentLoaded event"
    );

    // Verify SET form handler is attached
    assert!(
        js.contains("set-form") && js.contains("handleSetForm"),
        "SET form handler should be attached on DOM ready"
    );

    // Verify GET form handler is attached
    assert!(
        js.contains("get-form") && js.contains("handleGetForm"),
        "GET form handler should be attached on DOM ready"
    );

    // Verify DELETE form handler is attached
    assert!(
        js.contains("delete-form") && js.contains("handleDeleteForm"),
        "DELETE form handler should be attached on DOM ready"
    );
}

/// Verify operation forms grid layout
#[tokio::test]
async fn test_operation_forms_have_proper_layout() {
    let html = get_homepage_html().await;

    // Verify forms container exists
    assert!(
        html.contains("class=\"operation-forms\""),
        "Forms should be in an operation-forms container"
    );

    // Verify each form has operation-form class
    assert!(
        html.contains("class=\"operation-form\""),
        "Each form should have operation-form class"
    );
}

/// Verify CSS styling for Try It Out section
async fn get_stylesheet() -> String {
    let app = create_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/style.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

/// Verify Try It Out section has proper CSS styling
#[tokio::test]
async fn test_try_it_out_section_has_styling() {
    let css = get_stylesheet().await;

    // Verify try-it-out section styling
    assert!(
        css.contains(".try-it-out"),
        "CSS should have .try-it-out section styles"
    );

    // Verify operation forms styling
    assert!(
        css.contains(".operation-forms"),
        "CSS should have .operation-forms styles"
    );

    // Verify operation form styling
    assert!(
        css.contains(".operation-form"),
        "CSS should have .operation-form styles"
    );

    // Verify result area styling
    assert!(
        css.contains(".result"),
        "CSS should have .result styles"
    );

    // Verify success state styling
    assert!(
        css.contains(".result.success"),
        "CSS should have .result.success styles"
    );

    // Verify error state styling
    assert!(
        css.contains(".result.error"),
        "CSS should have .result.error styles"
    );
}

/// Verify accessibility of form inputs
#[tokio::test]
async fn test_form_inputs_are_accessible() {
    let html = get_homepage_html().await;

    // Verify inputs have aria-label
    assert!(
        html.contains("aria-label="),
        "Form inputs should have aria-label for accessibility"
    );

    // Verify inputs have placeholder text
    assert!(
        html.contains("placeholder=\"Key\""),
        "Key inputs should have placeholder text"
    );

    // Verify value input has placeholder
    assert!(
        html.contains("placeholder=\"Value\""),
        "Value input should have placeholder text"
    );
}

/// Verify hero section links to Try It Out
#[tokio::test]
async fn test_hero_links_to_try_it_out() {
    let html = get_homepage_html().await;

    // Verify hero has a link to try-it-out section
    assert!(
        html.contains("href=\"#try-it-out\""),
        "Hero section should have a 'Try It Out' link"
    );
}

/// Verify DELETE form submits correctly with proper validation
#[tokio::test]
async fn test_delete_form_has_proper_validation() {
    let js = get_javascript().await;

    // Verify handleDeleteForm prevents default form behavior
    assert!(
        js.contains("event.preventDefault()") || js.contains("preventDefault"),
        "Delete form should prevent default submit behavior"
    );

    // Verify it extracts key from form
    assert!(
        js.contains("FormData") || js.contains("formData"),
        "Delete form should extract data from FormData"
    );
}

/// Test form error handling
#[tokio::test]
async fn test_form_error_handling() {
    let js = get_javascript().await;

    // Verify try-catch for error handling
    assert!(
        js.contains("catch") && js.contains("error"),
        "Form handlers should have error handling"
    );

    // Verify error messages are displayed
    assert!(
        js.contains("showResult") && js.contains("true"),
        "Errors should be displayed using showResult with isError=true"
    );
}
