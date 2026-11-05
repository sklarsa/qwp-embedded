//! `no_std` QuestDB Wire Protocol (QWP) client for microcontrollers. Allocation-free: the
//! application owns all memory, so a board hands over whatever it has to spare.
//!
//! - [`Encoder`] builds QWP messages into a caller's buffer. No dependencies.
//! - [`Ring`] holds records until QuestDB acks them.
//! - `sender::run` (feature `sender`) streams a ring over WebSocket with embassy-net.
#![cfg_attr(not(test), no_std)]

mod encoder;
mod ring;
#[cfg(feature = "sender")]
pub mod sender;

pub use encoder::*;
pub use ring::Ring;
