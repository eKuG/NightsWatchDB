//! Command-line and environment settings, parsed by hand: three options do
//! not justify a parser crate.

use std::ffi::OsString;
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
    /// Extra host names the API answers to (see the README).
    pub api_allowed_hosts: Vec<String>,
}

/// The text printed by `--help`.
pub const HELP: &str = "\
nightswatchdb: a database for OpenTelemetry traces

Usage: nightswatchdb [options]

Options (each also reads an environment variable; a flag wins):
  --data-dir DIR           NWDB_DATA_DIR         [./data]
  --otlp-grpc-addr IP:PORT NWDB_OTLP_GRPC_ADDR   [127.0.0.1:4317]
  --api-addr IP:PORT       NWDB_API_ADDR         [127.0.0.1:4320]
  --api-allowed-hosts LIST NWDB_API_ALLOWED_HOSTS [none]
                           comma-separated host names the API answers to
                           besides localhost and the loopback addresses
  --help                   print this text
  --version                print the version

There is no authentication and no TLS. The defaults listen on the loopback
interface only; listen on other addresses only on a network you trust.
";

/// Parses the arguments (without the program name). `env` looks up an
/// environment variable. Arguments and variables that are not valid UTF-8 are
/// errors, not silently replaced.
pub fn parse(
    args: impl IntoIterator<Item = OsString>,
    env: impl Fn(&str) -> Option<OsString>,
) -> Result<Command, String> {
    let mut data_dir = None;
    let mut otlp = None;
    let mut api = None;
    let mut hosts = None;
    let mut args = args.into_iter().map(|a| {
        a.into_string()
            .map_err(|a| format!("argument {a:?} is not valid UTF-8"))
    });
    while let Some(arg) = args.next() {
        let arg = arg?;
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
            "--api-allowed-hosts" => &mut hosts,
            _ => return Err(format!("unknown argument `{name}`")),
        };
        let value = match inline {
            Some(v) => v,
            None => args
                .next()
                .ok_or_else(|| format!("`{name}` needs a value"))??,
        };
        *slot = Some(value);
    }
    let pick = |flag: Option<String>, var: &str, default: &str| -> Result<String, String> {
        if let Some(v) = flag {
            return Ok(v);
        }
        match env(var) {
            None => Ok(default.to_owned()),
            Some(v) => match v.into_string() {
                Ok(v) if v.is_empty() => Ok(default.to_owned()),
                Ok(v) => Ok(v),
                Err(_) => Err(format!("environment variable {var} is not valid UTF-8")),
            },
        }
    };
    let addr = |s: String, what: &str| {
        s.parse::<SocketAddr>()
            .map_err(|e| format!("{what} `{s}` is not an IP:PORT address: {e}"))
    };
    Ok(Command::Run(Config {
        data_dir: pick(data_dir, "NWDB_DATA_DIR", "./data")?.into(),
        otlp_grpc_addr: addr(
            pick(otlp, "NWDB_OTLP_GRPC_ADDR", "127.0.0.1:4317")?,
            "OTLP gRPC address",
        )?,
        api_addr: addr(pick(api, "NWDB_API_ADDR", "127.0.0.1:4320")?, "API address")?,
        api_allowed_hosts: pick(hosts, "NWDB_API_ALLOWED_HOSTS", "")?
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .map(str::to_owned)
            .collect(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(args: &[&str], env: &[(&str, &str)]) -> Result<Command, String> {
        parse(args.iter().map(OsString::from), |k| {
            env.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).into())
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

    #[test]
    fn allowed_hosts_are_a_comma_separated_list() {
        let Ok(Command::Run(c)) = run(&[], &[("NWDB_API_ALLOWED_HOSTS", "a.example, b.example,,")])
        else {
            panic!()
        };
        assert_eq!(c.api_allowed_hosts, ["a.example", "b.example"]);
        let Ok(Command::Run(c)) = run(&["--api-allowed-hosts=c.example"], &[]) else {
            panic!()
        };
        assert_eq!(c.api_allowed_hosts, ["c.example"]);
        let Ok(Command::Run(c)) = run(&[], &[]) else {
            panic!()
        };
        assert!(c.api_allowed_hosts.is_empty());
    }

    #[test]
    fn input_that_is_not_utf8_is_an_error() {
        use std::os::unix::ffi::OsStringExt;
        let bad = OsString::from_vec(vec![0x66, 0xff]);
        assert!(parse([bad.clone()], |_| None).is_err());
        assert!(parse([OsString::from("--data-dir"), bad.clone()], |_| None).is_err());
        let var = bad.clone();
        let r = parse([], move |k| (k == "NWDB_DATA_DIR").then(|| var.clone()));
        assert!(r.unwrap_err().contains("NWDB_DATA_DIR"));
        let var = bad;
        assert!(parse([], move |k| (k == "NWDB_API_ADDR").then(|| var.clone())).is_err());
    }
}
