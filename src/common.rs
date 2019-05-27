pub fn last_brace(buf: &[u8]) -> Option<usize> {
    let mut opens = 0;
    let mut count = 0;
    for b in buf {
        if *b == b'{' {
            opens += 1;
        }
        if *b == b'}' {
            opens -= 1;
        }
        if opens == 0 {
            return Some(count);
        };
        count += 1;
    }
    None
}
