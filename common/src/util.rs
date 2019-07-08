/// Returns a tuple of the first brace and last brace if found in buf.
/// Intended to find slices to deserialize with serde.
/// It is NOT smart about braces embedded in strings.
pub fn find_brace(buf: &[u8]) -> Option<(usize, usize)> {
    let mut opens = 0;
    let mut first_brace = 0;
    let mut got_open = false;
    for (count, b) in buf.iter().enumerate() {
        if *b == b'{' {
            opens += 1;
            if !got_open {
                first_brace = count;
            }
            got_open = true;
        }
        if *b == b'}' {
            opens -= 1;
        }
        if opens == 0 && got_open {
            return Some((first_brace, count));
        };
    }
    None
}
