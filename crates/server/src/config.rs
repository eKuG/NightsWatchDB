//! Command-line and environment settings, parsed by hand: three options do
//! not justify a parser crate.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

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
    /// Start even if log files were lost (see the README).
    pub accept_lost_log: bool,
    /// How long data is kept; `None` keeps everything (see the README).
    pub retention: Option<Duration>,
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
  --accept-lost-log        NWDB_ACCEPT_LOST_LOG  [off]
                           start although log files were lost, giving up the
                           records acknowledged since the last flush
  --retention DURATION     NWDB_RETENTION        [keep everything]
                           a positive whole number and a unit s, m, h or d,
                           for example 7d; older data is removed
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
    let mut retention = None;
    let mut accept_lost_log = false;
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
        if name == "--accept-lost-log" {
            if inline.is_some() {
                return Err("`--accept-lost-log` takes no value".into());
            }
            accept_lost_log = true;
            continue;
        }
        let slot = match name.as_str() {
            "--help" | "-h" => return Ok(Command::Help),
            "--version" | "-V" => return Ok(Command::Version),
            "--data-dir" => &mut data_dir,
            "--otlp-grpc-addr" => &mut otlp,
            "--api-addr" => &mut api,
            "--api-allowed-hosts" => &mut hosts,
            "--retention" => &mut retention,
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
    // A flag is parsed as given, so `--retention ""` is an error; an empty
    // variable counts as unset, like the others.
    let retention = match retention {
        Some(v) => Some(parse_retention(&v)?),
        None => match pick(None, "NWDB_RETENTION", "")?.as_str() {
            "" => None,
            v => Some(parse_retention(v)?),
        },
    };
    Ok(Command::Run(Config {
        data_dir: pick(data_dir, "NWDB_DATA_DIR", "./data")?.into(),
        otlp_grpc_addr: addr(
            pick(otlp, "NWDB_OTLP_GRPC_ADDR", "127.0.0.1:4317")?,
            "OTLP gRPC address",
        )?,
        api_addr: addr(pick(api, "NWDB_API_ADDR", "127.0.0.1:4320")?, "API address")?,
        accept_lost_log: accept_lost_log
            || match pick(None, "NWDB_ACCEPT_LOST_LOG", "false")?.as_str() {
                "1" | "true" => true,
                "0" | "false" => false,
                other => {
                    return Err(format!(
                        "NWDB_ACCEPT_LOST_LOG `{other}` is not one of 1, true, 0, false"
                    ));
                }
            },
        retention,
        api_allowed_hosts: pick(hosts, "NWDB_API_ALLOWED_HOSTS", "")?
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .map(str::to_owned)
            .collect(),
    }))
}

/// A positive whole number followed by `s`, `m`, `h` or `d`, such as `7d`.
/// Nothing else is accepted: no sign, no space, no fraction, no zero.
fn parse_retention(text: &str) -> Result<Duration, String> {
    let bad = || {
        format!(
            "retention `{text}` is not a positive whole number followed by s, m, h or d \
             (for example 7d)"
        )
    };
    let unit = match text.chars().last() {
        Some('s') => 1,
        Some('m') => 60,
        Some('h') => 3600,
        Some('d') => 86_400,
        _ => return Err(bad()),
    };
    let digits = &text[..text.len() - 1];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let n: u64 = digits.parse().map_err(|_| bad())?;
    if n == 0 {
        return Err(bad());
    }
    n.checked_mul(unit)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("retention `{text}` is too large"))
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
    fn accept_lost_log_is_a_switch() {
        let get = |args: &[&str], env: &[(&str, &str)]| match run(args, env) {
            Ok(Command::Run(c)) => Ok(c.accept_lost_log),
            Ok(_) => panic!(),
            Err(e) => Err(e),
        };
        assert_eq!(get(&[], &[]), Ok(false));
        assert_eq!(get(&["--accept-lost-log"], &[]), Ok(true));
        assert!(get(&["--accept-lost-log=1"], &[]).is_err());
        for (v, want) in [("1", true), ("true", true), ("0", false), ("false", false)] {
            assert_eq!(get(&[], &[("NWDB_ACCEPT_LOST_LOG", v)]), Ok(want), "{v}");
        }
        assert!(get(&[], &[("NWDB_ACCEPT_LOST_LOG", "yes")]).is_err());
        // The flag wins over a variable that says no.
        assert_eq!(
            get(&["--accept-lost-log"], &[("NWDB_ACCEPT_LOST_LOG", "0")]),
            Ok(true)
        );
    }

    #[test]
    fn retention_is_a_number_with_a_unit() {
        let get = |args: &[&str], env: &[(&str, &str)]| match run(args, env) {
            Ok(Command::Run(c)) => Ok(c.retention),
            Ok(_) => panic!(),
            Err(e) => Err(e),
        };
        let secs = |n| Ok(Some(Duration::from_secs(n)));
        assert_eq!(get(&[], &[]), Ok(None));
        assert_eq!(get(&["--retention", "90s"], &[]), secs(90));
        assert_eq!(get(&["--retention=15m"], &[]), secs(900));
        assert_eq!(get(&[], &[("NWDB_RETENTION", "36h")]), secs(36 * 3600));
        assert_eq!(get(&[], &[("NWDB_RETENTION", "7d")]), secs(7 * 86_400));
        // The flag wins; an empty variable is unset.
        assert_eq!(
            get(&["--retention=1d"], &[("NWDB_RETENTION", "x")]),
            secs(86_400)
        );
        assert_eq!(get(&[], &[("NWDB_RETENTION", "")]), Ok(None));
        for bad in [
            "",
            "0",
            "0d",
            "00s",
            "7",
            "d",
            "7w",
            "7D",
            "-7d",
            "+7d",
            " 7d",
            "7d ",
            "7 d",
            "1.5h",
            "1e3s",
            "7dd",
            "0x10s",
            "99999999999999999999d",
            "307445734561825861d",
        ] {
            assert!(get(&["--retention", bad], &[]).is_err(), "flag `{bad}`");
            if !bad.is_empty() {
                assert!(get(&[], &[("NWDB_RETENTION", bad)]).is_err(), "env `{bad}`");
            }
        }
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
