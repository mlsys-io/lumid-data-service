//! Unit tests for `GET /blobs/{key}` content-type resolution.
//!
//! Covers the bug this guards against: a MinIO object with a real stored
//! Content-Type (e.g. `image/jpeg`) must not be served as
//! `application/octet-stream`. All tests run against the pure helpers in
//! `handlers::blobs`; no live Postgres pool or object store required.

use lumid_platform::handlers::blobs::{is_active_content_type, resolve_content_type, valid_content_type};
use lumid_platform::queries::blobs::guess_from_extension;

// ── resolve_content_type: object-store metadata wins ─────────────────────────

#[test]
fn stored_metadata_image_jpeg_wins_over_everything() {
    let ct = resolve_content_type(Some("image/jpeg"), Some("application/octet-stream"), "robotics-demo/frames/000.jpg");
    assert_eq!(ct, "image/jpeg");
}

#[test]
fn stored_metadata_text_plain_is_used_directly() {
    let ct = resolve_content_type(Some("text/plain"), None, "notes/sha256=deadbeef");
    assert_eq!(ct, "text/plain");
}

#[test]
fn stored_metadata_ndjson_survives_even_with_extensionless_key() {
    // application/x-ndjson has no extension-guess entry — it can only survive
    // via stored metadata or the DB, never via `guess_from_extension`.
    let ct = resolve_content_type(
        Some("application/x-ndjson"),
        None,
        "text/sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    assert_eq!(ct, "application/x-ndjson");
}

// ── resolve_content_type: DB fallback for legacy extensionless CAS keys ──────

#[test]
fn db_fallback_used_when_stored_metadata_absent() {
    let ct = resolve_content_type(
        None,
        Some("application/x-ndjson"),
        "text/sha256=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    assert_eq!(
        ct, "application/x-ndjson",
        "legacy CAS blobs ingested before object-store metadata was written must \
         still resolve via raw.blobs, since their sha256= keys have no extension"
    );
}

#[test]
fn db_fallback_used_when_stored_metadata_is_malformed() {
    // A malformed/untrusted stored value must not just get used anyway — it
    // falls through to the next tier exactly like "absent".
    let ct = resolve_content_type(Some("garbage"), Some("text/plain"), "notes/sha256=cccc");
    assert_eq!(ct, "text/plain");
}

// ── resolve_content_type: extension guess, then octet-stream ────────────────

#[test]
fn extension_guess_used_when_no_metadata_and_no_db_row() {
    let ct = resolve_content_type(None, None, "robotics-demo/frames/000.jpg");
    assert_eq!(ct, "image/jpeg");
}

#[test]
fn unknown_extension_and_no_metadata_falls_back_to_octet_stream() {
    let ct = resolve_content_type(None, None, "misc/sha256=deadbeef");
    assert_eq!(ct, "application/octet-stream");
}

#[test]
fn extensionless_jsonl_style_key_with_no_source_anywhere_is_octet_stream() {
    // Demonstrates the compatibility gap explicitly: without stored metadata
    // or a DB row, an extensionless key can never resolve to
    // application/x-ndjson from the key alone.
    let ct = resolve_content_type(
        None,
        None,
        "text/sha256=dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    );
    assert_eq!(ct, "application/octet-stream");
}

// ── malformed / untrusted metadata must not reach header construction ───────

#[test]
fn valid_content_type_rejects_empty_and_missing_slash() {
    assert_eq!(valid_content_type(""), None);
    assert_eq!(valid_content_type("   "), None);
    assert_eq!(valid_content_type("not-a-mime-type"), None);
}

#[test]
fn valid_content_type_rejects_header_injection_attempts() {
    // CR/LF in a header value is how response-splitting / extra-header
    // injection works; `HeaderValue::from_str` must reject these, and
    // `valid_content_type` must propagate that rejection as `None` rather
    // than panicking or silently truncating.
    assert_eq!(
        valid_content_type("image/jpeg\r\nX-Injected: evil"),
        None
    );
    assert_eq!(valid_content_type("image/jpeg\nSet-Cookie: pwned=1"), None);
    assert_eq!(valid_content_type("image/jpeg\0trailing-nul"), None);
}

#[test]
fn valid_content_type_accepts_normal_mime_with_parameters() {
    assert_eq!(
        valid_content_type("text/plain; charset=utf-8"),
        Some("text/plain; charset=utf-8".to_string())
    );
}

#[test]
fn resolve_content_type_never_panics_on_malformed_stored_and_db_values() {
    let ct = resolve_content_type(
        Some("image/jpeg\r\nX-Evil: 1"),
        Some("also\r\nbad"),
        "unknown-key-with-no-extension",
    );
    assert_eq!(ct, "application/octet-stream");
}

// ── active-content mitigation (stored XSS via public GET) ───────────────────

#[test]
fn html_and_svg_are_flagged_as_active_content() {
    assert!(is_active_content_type("text/html"));
    assert!(is_active_content_type("text/html; charset=utf-8"));
    assert!(is_active_content_type("image/svg+xml"));
    assert!(is_active_content_type("application/xhtml+xml"));
}

#[test]
fn images_and_text_plain_are_not_active_content() {
    assert!(!is_active_content_type("image/jpeg"));
    assert!(!is_active_content_type("text/plain"));
    assert!(!is_active_content_type("application/x-ndjson"));
    assert!(!is_active_content_type("application/octet-stream"));
}

// ── guess_from_extension: the reproduction case from the bug report ────────

#[test]
fn jpg_extension_guesses_image_jpeg() {
    assert_eq!(
        guess_from_extension("robotics-demo/frames/000001.jpg"),
        Some("image/jpeg".to_string())
    );
    assert_eq!(
        guess_from_extension("robotics-demo/frames/000001.jpeg"),
        Some("image/jpeg".to_string())
    );
}

#[test]
fn txt_extension_guesses_text_plain() {
    assert_eq!(
        guess_from_extension("notes/readme.txt"),
        Some("text/plain".to_string())
    );
}

#[test]
fn jsonl_and_ndjson_extensions_are_not_guessable() {
    // Mirrors Python's `mimetypes.guess_type`, which also doesn't know these
    // by default — they must come from stored metadata or raw.blobs instead.
    assert_eq!(guess_from_extension("events/log.jsonl"), None);
    assert_eq!(guess_from_extension("events/log.ndjson"), None);
}

#[test]
fn key_with_no_extension_guesses_nothing() {
    assert_eq!(
        guess_from_extension("text/sha256=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        None
    );
}
