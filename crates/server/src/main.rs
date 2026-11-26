//! The `nightswatchdb` binary. See the README of this crate.

use nwdb_server::config::{Command, Config, HELP};
use nwdb_server::{Server, ServerOptions};
use std::process::ExitCode;
use tokio::signal::unix::{SignalKind, signal};

fn main() -> ExitCode {
    let args = std::env::args_os().skip(1);
    let config = match nwdb_server::config::parse(args, |k| std::env::var_os(k)) {
        Ok(Command::Run(c)) => c,
        Ok(Command::Help) => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("nightswatchdb {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("nightswatchdb: {e}\nTry --help.");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("nightswatchdb: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run(config))
}

async fn run(config: Config) -> ExitCode {
    let (mut term, mut int) = match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(t), Ok(i)) => (t, i),
        _ => {
            eprintln!("nightswatchdb: cannot install signal handlers");
            return ExitCode::FAILURE;
        }
    };
    let mut options = ServerOptions::new(&config.data_dir);
    options.otlp_grpc_addr = config.otlp_grpc_addr;
    options.api_addr = config.api_addr;
    options.api_allowed_hosts = config.api_allowed_hosts;
    options.engine.accept_lost_log = config.accept_lost_log;
    options.engine.retention = config.retention;
    options.engine.compaction.enabled = !config.no_compaction;
    if let Some(quiet) = config.compaction_quiet {
        options.engine.compaction.quiet = quiet;
    }
    eprintln!(
        "nightswatchdb {}: opening {}",
        env!("CARGO_PKG_VERSION"),
        config.data_dir.display()
    );
    // Recovery can take long. A signal that arrives meanwhile ends the process
    // at once: nothing has been accepted yet, and recovery is redone at the
    // next start.
    let started = tokio::select! {
        r = Server::start(options) => r,
        _ = term.recv() => quit_now("SIGTERM received while starting, exiting now"),
        _ = int.recv() => quit_now("SIGINT received while starting, exiting now"),
    };
    let server = match started {
        Ok(s) => s,
        Err(e) => {
            eprintln!("nightswatchdb: cannot start: {e}");
            if e.to_string().contains("accept_lost_log") {
                eprintln!(
                    "nightswatchdb: log files were lost. To start anyway and give up the \
                     records acknowledged since the last flush, pass --accept-lost-log \
                     (or set NWDB_ACCEPT_LOST_LOG=1)."
                );
            }
            return ExitCode::FAILURE;
        }
    };
    let s = server.engine().stats();
    eprintln!(
        "recovered: {} parts, {} log records replayed, {} bytes buffered",
        s.parts,
        s.acked_lsn.saturating_sub(s.flushed_lsn),
        s.buffered_bytes
    );
    eprintln!(
        "listening: OTLP gRPC on {}, HTTP API on {}",
        server.otlp_grpc_addr(),
        server.api_addr()
    );

    let fatal = tokio::select! {
        _ = term.recv() => { eprintln!("SIGTERM received, shutting down"); false }
        _ = int.recv() => { eprintln!("SIGINT received, shutting down"); false }
        () = server.fatal() => { eprintln!("fatal engine error, shutting down"); true }
    };
    let mut failed = fatal;
    // The write-ahead log makes a quick exit safe: what was acknowledged is
    // replayed at the next start. A second signal asks for exactly that.
    let shutdown = server.shutdown();
    tokio::pin!(shutdown);
    let result = tokio::select! {
        r = &mut shutdown => r,
        _ = term.recv() => quit_now("second SIGTERM received, exiting now"),
        _ = int.recv() => quit_now("second SIGINT received, exiting now"),
    };
    if let Err(e) = result {
        eprintln!("error during shutdown: {e}");
        failed = true;
    }
    if failed {
        ExitCode::FAILURE
    } else {
        eprintln!("shut down cleanly");
        ExitCode::SUCCESS
    }
}

/// Exits at once. Returning from `run` would wait for the engine's shutdown
/// thread, which is what the sender of the signal does not want to wait for.
fn quit_now(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(130)
}
