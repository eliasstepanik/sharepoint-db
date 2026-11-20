//! Small shared helpers (hex), so modules do not each carry their own copy.

/// Lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Decodes lowercase or uppercase hex; `None` on any non-hex character or an odd
/// number of characters.
pub(crate) fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    let b = hex.as_bytes();
    if !b.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() / 2);
    for i in 0..b.len() / 2 {
        let hi = (b[i * 2] as char).to_digit(16)?;
        let lo = (b[i * 2 + 1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}
