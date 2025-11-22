#[inline(always)]
pub fn likely(b: bool) -> bool {
    if !b {
        cold();
    }
    b
}

#[inline(always)]
pub fn unlikely(b: bool) -> bool {
    if b {
        cold();
    }
    b
}

// Branch prediction hints
#[inline(always)]
#[cold]
fn cold() {}
