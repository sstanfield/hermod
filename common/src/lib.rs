#![feature(async_await)]

//! This module is code that is common to both the client and server.

#[macro_use]
extern crate serde_derive;

pub mod types;
pub use crate::types::*;

pub mod util;
pub use crate::util::*;

pub mod protocolx;
pub use crate::protocolx::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
