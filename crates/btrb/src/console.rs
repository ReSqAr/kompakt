//! What a CLI command may do to the terminal: print, prompt, read stdin.
//!
//! ```text
//!   command(cmd, &mut dyn Console)     the app's code; no rpassword, no stdout
//!        │
//!   StdConsole                         the real terminal (cli)
//!   ScriptedConsole                    canned answers, captured output (tests)
//! ```
//!
//! Library crates deny `print_stdout`; routing every line through a trait is
//! what lets a command live in its app's crate, and be tested without a TTY.

use crate::Error;
use std::collections::VecDeque;
use std::io::{Read, Write};

pub trait Console: Send {
    fn println(&mut self, line: &str);
    fn eprintln(&mut self, line: &str);
    /// A line read without echo.
    fn prompt_secret(&mut self, label: &str) -> Result<String, Error>;
    /// Everything on stdin (the `-` argument of a list).
    fn read_stdin(&mut self) -> Result<String, Error>;
}

/// The process's terminal.
pub struct StdConsole;

impl Console for StdConsole {
    fn println(&mut self, line: &str) {
        // A closed pipe (`| head`) is the reader's choice, not a failure.
        let _ = writeln!(std::io::stdout(), "{line}");
    }

    fn eprintln(&mut self, line: &str) {
        let _ = writeln!(std::io::stderr(), "{line}");
    }

    fn prompt_secret(&mut self, label: &str) -> Result<String, Error> {
        rpassword::prompt_password(label).map_err(Error::new)
    }

    fn read_stdin(&mut self) -> Result<String, Error> {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(Error::new)?;
        Ok(text)
    }
}

/// Answers given up front, output captured. For tests of commands.
#[derive(Default)]
pub struct ScriptedConsole {
    pub secrets: VecDeque<String>,
    pub stdin: String,
    pub out: Vec<String>,
    pub err: Vec<String>,
}

impl Console for ScriptedConsole {
    fn println(&mut self, line: &str) {
        self.out.push(line.to_owned());
    }

    fn eprintln(&mut self, line: &str) {
        self.err.push(line.to_owned());
    }

    fn prompt_secret(&mut self, label: &str) -> Result<String, Error> {
        self.secrets
            .pop_front()
            .ok_or_else(|| Error::new(format!("no scripted answer for the prompt {label:?}")))
    }

    fn read_stdin(&mut self) -> Result<String, Error> {
        Ok(std::mem::take(&mut self.stdin))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn a_scripted_console_answers_in_order_and_runs_out_loudly() {
        let mut c = ScriptedConsole {
            secrets: VecDeque::from(["one".to_owned()]),
            stdin: "a\nb".to_owned(),
            ..Default::default()
        };
        assert_eq!(c.prompt_secret("Password: ").unwrap(), "one");
        assert!(c.prompt_secret("Password: ").is_err());
        assert_eq!(c.read_stdin().unwrap(), "a\nb");
        c.println("hello");
        assert_eq!(c.out, ["hello"]);
    }
}
