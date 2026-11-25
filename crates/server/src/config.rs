//! Command-line and environment settings, parsed by hand: three options do
//! not justify a parser crate.

use std::net::SocketAddr;
use std::path::PathBuf;

/// What the process was asked to do.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    /// Run the server.
    Run(Config),
    /// Print the help text and exit.
    Help,
    /// Print the version and exit.
    Version,
}

/// Settings of a running server.
#[derive(Debug, PartialEq, Eq)]
pub struct Config {
    /// Where the database lives.
    pub data_dir: PathBuf,
    /// Listen address of the OTLP gRPC door.
    pub otlp_grpc_addr: SocketAddr,
    /// Listen address of the HTTP query API.
    pub api_addr: SocketAddr,
}

/// The text printed by `--help`.
pub const HELP: &str = "\
nightswatchdb: a database for OpenTelemetry traces

Usage: nightswatchdb [options]

Options (each also reads an environment variable; a flag wins):
  --data-dir DIR           NWDB_DATA_DIR         [./data]
  --otlp-grpc-addr IP:PORT NWDB_OTLP_GRPC_ADDR   [127.0.0.1:4317]
  --api-addr IP:PORT       NWDB_API_ADDR         [127.0.0.1:4320]
  --help                   print this text
  --version                print the version

There is no authentication and no TLS. The defaults listen on the loopback
interface only; listen on other addresses only on a network you trust.
";

/// Parses the arguments (without the program name). `env` looks up an
/// environment variable.
pub fn parse(
    args: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Command, String> {
    let mut data_dir = None;
    let mut otlp = None;
    let mut api = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let (name, inline) = match arg.split_once('=') {
            Some((n, v)) if n.starts_with("--") => (n.to_owned(), Some(v.to_owned())),
            _ => (arg, None),
        };
        let slot = match name.as_str() {
            "--help" | "-h" => return Ok(Command::Help),
            "--version" | "-V" => return Ok(Command::Version),
            "--data-dir" => &mut data_dir,
            "--otlp-grpc-addr" => &mut otlp,
            "--api-addr" => &mut api,
            _ => return Err(format!("unknown argument `{name}`")),
        };
        let value = match inline.or_else(|| args.next()) {
            Some(v) => v,
            None => return Err(format!("`{name}` needs a value")),
        };
        *slot = Some(value);
    }
    let pick = |flag: Option<String>, var: &str, default: &str| {
        flag.or_else(|| env(var).filter(|v| !v.is_empty()))
            .unwrap_or_else(|| default.to_owned())
    };
    let addr = |s: String, what: &str| {
        s.parse::<SocketAddr>()
            .map_err(|e| format!("{what} `{s}` is not an IP:PORT address: {e}"))
    };
    Ok(Command::Run(Config {
        data_dir: pick(data_dir, "NWDB_DATA_DIR", "./data").into(),
        otlp_grpc_addr: addr(
            pick(otlp, "NWDB_OTLP_GRPC_ADDR", "127.0.0.1:4317"),
            "OTLP gRPC address",
        )?,
        api_addr: addr(pick(api, "NWDB_API_ADDR", "127.0.0.1:4320"), "API address")?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(args: &[&str], env: &[(&str, &str)]) -> Result<Command, String> {
        parse(args.iter().map(|s| s.to_string()), |k| {
            env.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        })
    }

    #[test]
    fn defaults_flags_and_environment() {
        let Ok(Command::Run(c)) = run(&[], &[]) else {
            panic!()
        };
        assert_eq!(c.data_dir, PathBuf::from("./data"));
        assert_eq!(c.otlp_grpc_addr.to_string(), "127.0.0.1:4317");
        assert_eq!(c.api_addr.to_string(), "127.0.0.1:4320");

        let env = [("NWDB_DATA_DIR", "/env"), ("NWDB_API_ADDR", "0.0.0.0:1")];
        let Ok(Command::Run(c)) = run(&["--data-dir", "/flag", "--otlp-grpc-addr=[::1]:9"], &env)
        else {
            panic!()
        };
        assert_eq!(c.data_dir, PathBuf::from("/flag"));
        assert_eq!(c.otlp_grpc_addr.to_string(), "[::1]:9");
        assert_eq!(c.api_addr.to_string(), "0.0.0.0:1");
    }

    #[test]
    fn help_version_and_errors() {
        assert_eq!(run(&["--help"], &[]), Ok(Command::Help));
        assert_eq!(run(&["--version"], &[]), Ok(Command::Version));
        assert!(run(&["--nope"], &[]).is_err());
        assert!(run(&["--data-dir"], &[]).is_err());
        assert!(run(&["--api-addr", "localhost:1"], &[]).is_err());
        assert!(run(&[], &[("NWDB_API_ADDR", "x")]).is_err());
    }
}
