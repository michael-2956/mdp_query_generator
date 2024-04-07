#[macro_export]
macro_rules! unwrap_variant {
    ($target: expr, $pat: path) => { {
        if let $pat(a) = $target {
            a
        } else {
            panic!("Failed to unwrap variant: {} to {}", stringify!($target), stringify!($pat));
        }
    } };
}

#[macro_export]
macro_rules! unwrap_pat {
    ($target: expr, $arg: pat, $ret: expr) => { {
        match $target {
            $arg => $ret,
            _ => panic!("Failed to unwrap pattern: {} to {}", stringify!($target), stringify!($arg)),
        }
    } };
}

#[macro_export]
macro_rules! unwrap_variant_ref {
    ($target: expr, $pat: path) => { {
        if let $pat(ref a) = $target {
            a
        } else {
            panic!("Failed to unwrap variant: {} to {}", stringify!($target), stringify!($pat));
        }
    } };
}

#[macro_export]
macro_rules! unwrap_variant_or_else {
    ($target: expr, $pat: path, $l: expr) => { {
        if let $pat(a) = $target {
            a
        } else {
            $l();
            panic!("Failed to unwrap variant: {} to {}", stringify!($target), stringify!($pat));
        }
    } };
}