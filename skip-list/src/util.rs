pub fn from_raw_mut<'a, T>(p: *mut T) -> Option<&'a mut T> {
    if p.is_null() {
        None
    } else {
        unsafe {
            Some(&mut *p)
        }
    }
}

pub fn from_raw<'a, T>(p: *mut T) -> Option<&'a T> {
    if p.is_null() {
        None
    } else {
        unsafe {
            Some(&*p)
        }
    }
}
