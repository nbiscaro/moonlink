pub fn init_logging() {
    use std::io;
    use tracing_subscriber::{fmt, EnvFilter};

    let _ = fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}
