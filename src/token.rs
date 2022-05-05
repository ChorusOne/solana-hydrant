use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};
use serde::Serialize;
use std::fmt;

/// Generate a token type that wraps the minimal unit of the token, it’s
/// “Lamport”. The symbol is for 10<sup>9</sup> of its minimal units and is
/// only used for `Debug` and `Display` printing.
#[macro_export]
macro_rules! impl_token {
    ($TokenLamports:ident, $symbol:expr, decimals = $decimals:expr) => {
        #[derive(
            Copy,
            Clone,
            Default,
            Eq,
            Ord,
            PartialEq,
            PartialOrd,
            BorshDeserialize,
            BorshSerialize,
            BorshSchema,
            Serialize,
        )]
        pub struct $TokenLamports(pub u64);

        impl fmt::Display for $TokenLamports {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "{}.{} {}",
                    self.0 / 10u64.pow($decimals),
                    &format!("{:0>9}", self.0 % 10u64.pow($decimals))[9 - $decimals..],
                    $symbol
                )
            }
        }

        impl fmt::Debug for $TokenLamports {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                fmt::Display::fmt(self, f)
            }
        }

        /// Parse a numeric string as an amount of Lamports, i.e., with 9 digit precision.
        ///
        /// Note that this parses the Lamports amount divided by 10<sup>9</sup>,
        /// which can include a decimal point. It does not parse the number of
        /// Lamports! This makes this function the semi-inverse of `Display`
        /// (only `Display` adds the suffixes, and we do not expect that
        /// here).
        impl std::str::FromStr for $TokenLamports {
            type Err = &'static str;
            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                let mut value = 0_u64;
                let mut is_after_decimal = false;
                let mut exponent: i32 = $decimals;
                let mut had_digit = false;

                // Walk the bytes one by one, we only expect ASCII digits or '.', so bytes
                // suffice. We build up the value as we go, and if we get past the decimal
                // point, we also track how far we are past it.
                for ch in s.as_bytes() {
                    match ch {
                        b'0'..=b'9' => {
                            value = value * 10 + ((ch - b'0') as u64);
                            if is_after_decimal {
                                exponent -= 1;
                            }
                            had_digit = true;
                        }
                        b'.' if !is_after_decimal => is_after_decimal = true,
                        b'.' => return Err("Value can contain at most one '.' (decimal point)."),
                        b'_' => { /* As a courtesy, allow numeric underscores for readability. */ }
                        _ => return Err("Invalid value, only digits, '_', and '.' are allowed."),
                    }

                    if exponent < 0 {
                        return Err("Value can contain at most 9 digits after the decimal point.");
                    }
                }

                if !had_digit {
                    return Err("Value must contain at least one digit.");
                }

                // If the value contained fewer than 9 digits behind the decimal point
                // (or no decimal point at all), scale up the value so it is measured
                // in lamports.
                while exponent > 0 {
                    value *= 10;
                    exponent -= 1;
                }

                Ok($TokenLamports(value))
            }
        }
    };
}

impl_token!(Lamports, "SOL", decimals = 9);
