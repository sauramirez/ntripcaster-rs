mod config;
mod server;
mod state;

use std::process::ExitCode;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    match config::Config::load_from_cli() {
        Ok(config) => match server::run(config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("server error: {err}");
                ExitCode::from(1)
            }
        },
        Err(err) => {
            eprintln!("configuration error: {err}");
            ExitCode::from(2)
        }
    }
}
